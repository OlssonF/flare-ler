# Script 1 in workflow to produce/read in forecasts and write to file
# The forecasts include 3 process-based models within LER
# and 2 "baseline" models (random walk and climatology)

# library(FLAREr)
library(arrow)
library(tidyverse)
library(lubridate)
library(fable)

# Set s3 environment variables
Sys.unsetenv("AWS_DEFAULT_REGION")
Sys.unsetenv("AWS_S3_ENDPOINT")
Sys.setenv("AWS_EC2_METADATA_DISABLED"="TRUE")
Sys.setenv('USE_HTTPS' = TRUE)
options(dplyr.summarise.inform = FALSE)

setwd(here::here())

#### a) LER forecasts ####
# The LER foreacsts are produced within the FCRE-forecast-code ler_ms workflow

# Read in the raw forecasts from s3 bucket (ler_ms3 for reruns)
s3_ler <- arrow::s3_bucket(bucket = "forecasts/ler_ms3/parquet",
                           endpoint_override =  "s3.flare-forecast.org",
                           anonymous = TRUE)

ds_ler <- arrow::open_dataset(s3_ler) 

# local location to write the parquet files
local_path <- './forecasts/reruns'

if (!dir.exists(local_path)) {
  dir.create(local_path)
}

# When to produce forecasts for
first_date <- ds_ler %>%
  distinct(reference_datetime) %>%
  summarise(min(reference_datetime)) %>%
  pull()

last_date <- ds_ler %>%
  distinct(reference_datetime) %>%
  summarise(max(reference_datetime)) %>%
  pull()

forecast_dates <- paste0(seq.Date(as.Date(first_date),as.Date(last_date), 7), ' 00:00:00')

ds_ler |>
  dplyr::filter(model_id == 'GOTM',
                reference_datetime %in% forecast_dates) |>
  dplyr::collect() |>
  dplyr::group_by(site_id, model_id, reference_datetime) |>
  arrow::write_dataset(path = local_path)

ds_ler |>
  dplyr::filter(model_id == 'GLM',
                reference_datetime %in% forecast_dates) |>
  dplyr::collect() |>
  dplyr::group_by(site_id, model_id, reference_datetime) |>
  arrow::write_dataset(path = local_path)

ds_ler |>
  dplyr::filter(model_id == 'Simstrat',
                reference_datetime %in% forecast_dates) |>
  dplyr::collect() |>
  dplyr::group_by(site_id, model_id, reference_datetime) |>
  arrow::write_dataset(path = local_path)


#### b) Baseline forecasts ####
# Targets data needed to produce baseline forecasts
targets <- read_csv('https://s3.flare-forecast.org/targets/ler_ms3/fcre/fcre-targets-insitu.csv') %>%
  filter(variable == 'temperature')



# data frame with all depth and start_date combinations to be forecast
forecast_vars <- expand.grid(start = as_date(gsub(' 00:00:00', '',forecast_dates)), 
                             depth_use = unique(targets$depth)) %>%
  mutate(h = 15)

##### Random walk #####
forecast.RW  <- function(start, h= 15, depth_use) {
  
  # Work out when the forecast should start
  forecast_starts <- targets %>%
    dplyr::filter(!is.na(observation) & depth == depth_use & datetime <= start)
  
  if (nrow(forecast_starts) !=0) {
    forecast_starts <- forecast_starts %>%
      # Start the day after the most recent non-NA value
      dplyr::summarise(start_date = ymd(max(datetime) + lubridate::days(1))) %>% # Date
      dplyr::mutate(h = (start - start_date) + h) %>% # Horizon value
      dplyr::ungroup()
    
    # Generate the RW model
    RW_model <- targets %>%
      mutate(datetime = as_date(datetime)) %>%
      # filter the targets data for the depth and start time
      dplyr::filter(depth == depth_use & datetime <= start) %>%
      tsibble::as_tsibble(key = 'depth', index = 'datetime') %>%
      # add NA values
      tsibble::fill_gaps() %>%
      # Remove the NA's put at the end, so that the forecast starts from the last day with an observation,
      # rather than today
      dplyr::filter(datetime < forecast_starts$start_date) %>%
      fabletools::model(RW = fable::RW(observation))
    
    # Generate the forecast
    RW_forecast <- RW_model %>%
      fabletools::generate(h = as.numeric(forecast_starts$h),
                           bootstrap = T, 
                           times = 256) %>%
      rename(model_id = .model,
             prediction = .sim,
             parameter = .rep) %>%
      as_tibble() %>% 
      mutate(reference_datetime = as.character(format(start,
                                                      '%Y-%m-%d %H:%M:%S')),
             # Add in the additional columns needed to score the forecast (like the FLARE output)
             site_id = 'fcre',
             variable = 'temperature',
             family = 'ensemble',
             forecast = 0,
             variable_type = 'state',
             pub_time = Sys.time()) 
    
    message('RW forecast for ', start, ' at ', depth_use, ' m')
    return(RW_forecast)
  }  else {
    message('RW forecast not run for ', start, ' at ', depth_use, ' m')
  }
}

# Produces forecast for all depths and date combinations
RW_forecast <- purrr::pmap_dfr(forecast_vars, forecast.RW)

#========================#

##### Climatology ####

# Function to create climatology forecast
# some of this function will change when we are forecasting the 2020-2021 period
forecast.clim <- function(targets = targets, start, h=14) {
  # only the targets available before should be used to produce the forecast
  doy_targets <- targets %>%
    filter(datetime < start) %>%
    
    # this gives the day of year as if it were a leap year
    mutate(doy = ifelse((yday(datetime) > 59 & lubridate::leap_year(datetime) != T),
                        yday(datetime) + 1,
                        yday(datetime))) %>%
    
    # find the day of year average
    group_by(doy, depth) %>%
    summarise(prediction = mean(observation))
  
  # Day of year of the forecast dates
  forecast_doy <- data.frame(datetime = seq(start, as_date(start + lubridate::days(h)), "1 day")) %>%
    mutate(doy = yday(datetime)) 
  
  # All combinations of doy and depth
  site_doy <- expand.grid(doy = forecast_doy$doy, depth = unique(targets$depth))
  
  # produce the forecast
  clim_forecast <- 
    doy_targets %>%
    ungroup() %>%
    mutate(doy = as.integer(doy)) %>% 
    filter(doy %in% forecast_doy$doy) %>% 
    full_join(., site_doy, by = c('doy', 'depth')) %>%
    full_join(., forecast_doy, by = 'doy') %>%
    select(-doy) 
  
  # Uncertainty - sd of the residuals between the last two years of data
  clim_uncertainty <- data.frame(depth = unique(clim_forecast$depth),
                                 sd = NA)
  
  for (i in 1:length(unique(clim_forecast$depth))) {
    # for a specified depth
    depth_use <- unique(clim_forecast$depth)[i]
    
    for_lm <-
      targets %>%
      mutate(datetime = ymd(datetime)) %>%
      # only for the last two years, ensure each DOY has two data points
      filter(between(datetime, (start - years(2) + days(2)), start),
             depth == depth_use) %>%
      # day of year (as if leap year)
      mutate(doy = ifelse(yday(datetime) > 59 & lubridate::leap_year(datetime) == F,
                          yday(datetime) + 1,
                          yday(datetime))) %>%
      group_by(doy, depth) %>% 
      # find which year it is (the first or second)
      mutate(yr = row_number()) %>% 
      select(-datetime, -site_id, -variable) %>%
      pivot_wider(names_from = yr, values_from = observation, names_prefix = 'yr')
    
    clim_uncertainty$sd[i] <- round(sd(residuals(lm(yr1~yr2, for_lm))), 2)
    
  }
  
  # Combine with the point forecast (DOY mean) with uncertainty
  clim_forecast <- clim_uncertainty %>%
    full_join(clim_forecast, ., by='depth') %>%
    mutate(reference_datetime = start) %>%
    
    # makes sure there are all combinations of site and datetime for each reference_datetime
    group_by(reference_datetime) %>%
    tidyr::complete(., depth, datetime) %>%
    
    # If there is a gap in the forecast, linearly interpolate, should only be for leap year missingness
    # mutate(prediction = imputeTS::na_interpolation(prediction),
    #        sd = imputeTS::na_interpolation(sd)) %>%
    mutate(model_id = 'climatology') 
  
  message('climatology forecast for ', start)
  return(clim_forecast[, c('model_id', 'datetime', 'reference_datetime', 'depth', 'prediction', 'sd')])
  
}

# get the climatology forecast for each date
climatology <- as_date(gsub(' 00:00:00', '',forecast_dates)) |> 
  map_dfr( ~ forecast.clim(targets = targets, start = .x)) 

# Function to create an ensemble from the mean and standard deviation
create.ensemble <- function(climatology, times = 256) {
  data.frame(parameter = 1:times, 
             prediction = rnorm(n=times, 
                                mean = climatology$prediction, 
                                sd= climatology$sd)) %>%
    mutate(datetime = climatology$datetime, 
           reference_datetime = as.character(format(climatology$reference_datetime,
                                                    '%Y-%m-%d %H:%M:%S')),
           depth = climatology$depth)
}

# Run the function over each row (datetime, reference_datetime and depth combination)
climatology_forecast <- climatology %>%
  split(1:nrow(.)) %>%
  purrr::map_dfr(create.ensemble) %>%
  # add in the extra columns needed
  mutate(model_id = 'climatology',
         site_id = 'fcre',
         variable = 'temperature',
         family = 'ensemble',
         forecast = 0,
         variable_type = 'state',
         pub_time = Sys.time()) 
#=============================#

#### c) Write forecasts to file ####
forecasts <- ls(pattern = '_forecast')

# write the forecasts to a local parquet database
for (i in 1:length(forecasts)) {
  
  get(forecasts[i])|>
      dplyr::group_by(site_id, model_id, reference_datetime) |>
      arrow::write_dataset(path = local_path) 
  
  message(forecasts[i],' written')
}

#==========================#