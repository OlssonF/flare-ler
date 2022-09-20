# Script 1 in workflow to produce/read in forecasts and write to file
# The forecasts include 3 process-based models within LER
  # and 2 "baseline" models (random walk and climatology)

library(GLM3r)
library(FLAREr)
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

#### a) LER forecasts ####
# The LER foreacsts are produced within the FLARE-LER workflow

# Read in the raw forecasts from s3 bucket
s3_ler <- arrow::s3_bucket(bucket = "test-csv/ler/fcre",
                                endpoint_override =  "s3.flare-forecast.org",
                                anonymous = TRUE)

ds_ler <- arrow::open_dataset(s3_ler, format = "csv")

# Fetch forecast from S3 bucket
ler_forecast <- ds_ler |> 
  # At the moment just looking at 1 depth
  filter(depth == 1) %>%
  dplyr::collect()

#### b) Baseline forecasts ####
# Targets data needed to produce baseline forecasts
targets <- read_csv('https://s3.flare-forecast.org/targets/ler/fcre/fcre-targets-insitu.csv') %>%
  filter(variable == 'temperature')

# When to produce forecasts for
forecast_dates <- seq(ymd('2018-08-03'), ymd('2019-01-11'), 7)

# data frame with all depth and start_date combinations to be forecast
forecast_vars <- expand.grid(start = forecast_dates, 
                             depth_use = unique(targets$depth)) %>%
  mutate(h = 14)

##### Random walk #####
forecast.RW  <- function(start, h= 14, depth_use) {
  
  # Work out when the forecast should start
  forecast_starts <- targets %>%
    dplyr::filter(!is.na(observed) & depth == depth_use, time < start)
  
  if (nrow(forecast_starts) !=0) {
    forecast_starts <- forecast_starts %>% 
      # Start the day after the most recent non-NA value
      dplyr::summarise(start_date = ymd(max(time) + lubridate::days(1))) %>% # Date
      dplyr::mutate(h = (start - start_date) + h) %>% # Horizon value
      dplyr::ungroup()
    
    # Generate the RW model
    RW_model <- targets %>%
      mutate(time = as_date(time)) %>%
      # filter the targets data for the depth and start time
      dplyr::filter(depth == depth_use & time < start) %>%
      tsibble::as_tsibble(key = 'depth', index = 'time') %>%
      # add NA values
      tsibble::fill_gaps() %>%
      # Remove the NA's put at the end, so that the forecast starts from the last day with an observation,
      # rather than today
      dplyr::filter(time < forecast_starts$start_date)  %>%
      fabletools::model(RW = fable::RW(observed))
    
    # Generate the forecast
    RW_forecast <- RW_model %>%
      fabletools::generate(h = as.numeric(forecast_starts$h),
                           bootstrap = T,
                           times = 200) %>%
      rename(model_id = .model,
             predicted = .sim,
             ensemble = .rep) %>%
      as_tibble() %>% 
      mutate(#h = as.numeric(time - min(time) + 1),
        start_time = start) 
    
    message('RW forecast for ', start, ' at ', depth_use, ' m')
    return(RW_forecast)
  }  else {
    message('RW forecast not run for ', start, ' at ', depth_use, ' m')
  }
}

# Produces forecast for all depths and date combinations
RW_forecast <- purrr::pmap_dfr(forecast_vars, forecast.RW) %>%
  # Add in the additional columns needed to score the forecast (like the FLARE output)
  mutate(site_id = 'fcre',
         variable = 'temperature',
         forecast = 0,
         variable_type = 'state')

#========================#

##### Climatology ####

# Function to create climatology forecast
# some of this function will change when we are forecasting the 2020-2021 period
forecast.clim <- function(targets = targets, start, h=14) {
  # only the targets available before should be used to produce the forecast
  # currently uses all data after instead
  doy_targets <- targets %>%
    filter(time > start) %>%
    
    # this gives the day of year as if it were a leap year
    mutate(doy = ifelse(yday(time) > 59 & lubridate::leap_year(time) == F,
                        yday(time) + lubridate::days(1),
                        yday(time))) %>%
    
    # find the day of year average
    group_by(doy, depth) %>%
    summarise(predicted = mean(observed))
  
  # Day of year of the forecast dates
  forecast_doy <- data.frame(time = seq(start, as_date(start + lubridate::days(h-1)), "1 day")) %>%
    mutate(doy = yday(time)) 
  
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
  
  # Uncertainty - Calculate the sd between observations of each month per depth and then find the 
  # average (mean) of these, gives a different sd for each depth
  clim_forecast <- targets %>%
    filter(time > start) %>%
    mutate(month = zoo::as.yearmon(time)) %>%
    
    # Find the standard deviation of observations for each month
    group_by(month, depth) %>% 
    summarise(sd = sd(observed)) %>%
    group_by(depth) %>%
    summarise(sd = mean(sd, na.rm = T)) %>%
    
    # Combine with the point forecast (DOY mean)
    full_join(clim_forecast, ., by='depth') %>%
    mutate(start_time = start) %>%
    
    # makes sure there are all combinations of site and time for each start_Time
    group_by(start_time) %>%
    tidyr::complete(., depth, time) %>%
    
    # If there is a gap in the forecast, linearly interpolate, should only be for leap year missingness
    mutate(predicted = imputeTS::na_interpolation(predicted),
           sd = imputeTS::na_interpolation(sd)) %>%
    mutate(model_id = 'climatology') 
  
  message('climatology forecast for ', start)
  return(clim_forecast[, c('model_id', 'time', 'start_time', 'depth', 'predicted', 'sd')])
  
}

# get the climatology forecast for each date
climatology <- forecast_dates %>%
  map_dfr( ~ forecast.clim(targets = targets, start = .x)) #%>%

# Function to create an ensemble from the mean and standard deviation
create.ensemble <- function(climatology, times = 200) {
  data.frame(ensemble = 1:times, predicted = rnorm(n=times, mean = climatology$predicted, sd= climatology$sd)) %>%
    mutate(time = climatology$time, 
           start_time = climatology$start_time,
           depth = climatology$depth)
}

# Run the function over each row (time, start_time and depth combination)
climatology_ensemble <- climatology %>%
  split(1:nrow(.)) %>%
  purrr::map_dfr(create.ensemble) %>%
  # add in the extra columns needed
  mutate(model_id = 'climatology',
         site_id = 'fcre',
         variable = 'temperature',
         forecast = 0,
         variable_type = 'state')
#=============================#

#### c) Write forecasts to file ####

# Runs differently in console and jobs
if (getwd() == dirname(rstudioapi::getSourceEditorContext()$path)) {
  write_csv(RW_forecast, '../forecasts/RW_forecast.csv.gz')
  write_csv(climatology_ensemble, '../forecasts/climatology_forecast.csv.gz')
  write_csv(ler, '../forecasts/ler_forecast.csv.gz')

} else {
  write_csv(RW_forecast, './forecasts/RW_forecast.csv.gz')
  write_csv(climatology_ensemble, './forecasts/climatology_forecast.csv.gz')
  write_csv(ler_forecast, './forecasts/ler_forecast.csv.gz')
}

#==========================#