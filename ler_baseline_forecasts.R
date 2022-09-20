
library(tidyverse)
library(lubridate)
library(fable)

options(dplyr.summarise.inform = FALSE)
# start with the targets data
targets <- read_csv('https://s3.flare-forecast.org/targets/ler/fcre/fcre-targets-insitu.csv') %>%
  filter(variable == 'temperature') #%>%
  # mutate(site_id = paste(site_id, depth, sep = '_'))

# When to produce forecasts for
forecast_dates <- seq(ymd('2018-08-03'), ymd('2019-01-11'), 7)

forecast_vars <- expand.grid(start = forecast_dates, depth_use = unique(targets$depth)) %>%
  mutate(#targets = 'targets',
         h = 14)

##### 1. Random walk ####
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


RW <- purrr::pmap_dfr(forecast_vars, forecast.RW) %>%
  mutate(site_id = 'fcre',
         variable = 'temperature',
         forecast = 0,
         variable_type = 'state')

write_csv(RW, 'RW.csv.gz')
#=======================================#

##### 2. Climatology ####

forecast.clim <- function(targets = targets, start, h=14) {
  # only the targets available before should be used to produce the forecast
  # currently uses all data
  doy_targets <- targets %>%
    # filter(time < start) %>%
    # this gives the day of year as if it were a leap year
    mutate(doy = ifelse(yday(time) > 59 & leap_year(time) == F,
                        yday(time) + 1,
                        yday(time))) %>%
    # find the day of year average
    group_by(doy, depth) %>%
    summarise(predicted = mean(observed))
  
  # Day of year of the forecast dates
  forecast_doy <- data.frame(time = seq(start, as_date(start + days(h-1)), "1 day")) %>%
    mutate(doy = yday(time)) 
  
  # All combinations of doy and site_id
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
    # average (mean and median) of these, gives a different sd for each depth
  clim_forecast <- targets %>%
    # filter(time < start) %>%
    mutate(month = zoo::as.yearmon(time)) %>%
    group_by(month, depth) %>% 
    # Find the standard deviation of observations for each month
    summarise(sd = sd(observed)) %>%
    group_by(depth) %>%
    summarise(sd = mean(sd, na.rm = T)) %>%
    # Combine with the point forecast
    full_join(clim_forecast, ., by='depth') %>%
    mutate(start_time = start) %>%
    group_by(start_time) %>%
    # makes sure there are all combinations of site and time
    tidyr::complete(., depth, time) %>%
    # If there is a gap in the forecast, linearly interpolate, should only be for leap year missingness
    mutate(predicted = imputeTS::na_interpolation(predicted),
           sd = imputeTS::na_interpolation(sd)) %>%
    mutate(model_id = 'climatology') 
  
  message('climatology forecast for ', start)
  return(clim_forecast[, c('model_id', 'time', 'start_time', 'depth', 'predicted', 'sd')])
  
}

# pretend data that we can use to develop method
forecast_dates_clim <- forecast_dates + years(1)

# get the climatology forecast for each date
climatology <- forecast_dates_clim %>%
  map_dfr( ~ forecast.clim(targets = subset(targets, time > max(forecast_dates_clim)), start = .x)) #%>%

# Function to create an ensemble from the mean and standard deviation
create.ensemble <- function(climatology, times) {
  data.frame(ensemble = 1:200, predicted = rnorm(n=200, mean = climatology$predicted, sd= climatology$sd)) %>%
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
         variable_type = 'state',
         # pretend data
         time = time - years(1),
         start_time = start_time - years(1))

write_csv(climatology_ensemble, 'climatology.csv.gz') 
#================================#

# Combine and score the two empirical forecasts

# Scoring forecasts individually
score_file_RW <- FLAREr::generate_forecast_score(
  # don't need to read in targets before
  targets_file = 'https://s3.flare-forecast.org/targets/ler/fcre/fcre-targets-insitu.csv',
  # needs to be from the csv
  forecast_file = 'RW.csv.gz',
  output_directory = './scores')

scored_RW <- read_parquet(score_file_RW)



score_file_climatology <- FLAREr::generate_forecast_score(
  # don't need to read in targets before
  targets_file = 'https://s3.flare-forecast.org/targets/ler/fcre/fcre-targets-insitu.csv',
  # needs to be from the csv
  forecast_file = 'climatology.csv.gz',
  output_directory = './scores')


scored_climatology <- arrow::read_parquet(score_file_climatology)
