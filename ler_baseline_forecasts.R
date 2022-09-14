
library(tidyverse)
library(lubridate)
library(fable)

options(dplyr.summarise.inform = FALSE)
# start with the targets data
targets <- read_csv('https://s3.flare-forecast.org/targets/ler/fcre/fcre-targets-insitu.csv') %>%
  filter(variable == 'temperature') #%>%
  # mutate(site_id = paste(site_id, depth, sep = '_'))

# When to produce forecasts for
forecast_dates <- seq(ymd('2020-11-08'), ymd('2021-03-01'), 7)

forecast_vars <- expand.grid(start = forecast_dates, depth_use = unique(targets$depth)) %>%
  mutate(#targets = 'targets',
         h = 34)

##### 1. Random walk ####
forecast.RW  <- function(start, h= 34, depth_use) {
  
  # Work out when the forecast should start
  forecast_starts <- targets %>%
    dplyr::filter(!is.na(observed) & depth == depth_use, time < start) %>%
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
  
  
}


RW <- purrr::pmap_dfr(forecast_vars, forecast.RW) %>%
  mutate(site_id = 'fcre',
         variable = 'temperature',
         forecast = 0,
         variable_type = 'state')

write_csv(RW, 'RW.csv.gz')
#=======================================#

##### 2. Climatology ####

forecast.clim <- function(targets = targets, start, h=34) {
  # only the targets available before should be used to produce the forecast
  doy_targets <- targets %>%
    filter(time < start) %>%
    # this gives the day of year as if it were a leap year
    mutate(doy = ifelse(yday(time) > 59 & leap_year(time) == F,
                        yday(time) + 1,
                        yday(time))) %>%
    # find the day of year average
    group_by(depth, doy) %>%
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
    filter(time < start) %>%
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

climatology <- forecast_dates %>%
  map_dfr( ~ forecast.clim(targets = targets, start = .x)) %>%
  mutate(site_id = 'fcre',
         variable = 'temperature',
         forecast = 0,
         variable_type = 'state')

write_csv(climatology, 'climatology.csv.gz') 
#================================#
