
library(tidyverse)
library(lubridate)
library(fable)

options(dplyr.summarise.inform = FALSE)
# start with the targets data
targets <- read_csv('https://s3.flare-forecast.org/targets/ler/fcre/fcre-targets-insitu.csv') %>%
  filter(variable == 'temperature') %>%
  mutate(site_id = paste(site_id, depth, sep = '_'))

# When to produce forecasts for
forecast_dates <- seq(ymd('2020-01-01'), ymd('2020-12-31'), 7)

##### 1. Random walk ####
forecast.RW  <- function(targets = targets, start, h= 14, sd = 'mean') {
  
  # Fit the model to the data (only data already 'collected')
  RW_model <- targets %>%
    mutate(time = as.Date(time)) %>%
    as_tsibble(key = c('site_id'), index = 'time') %>%
    filter(time < start) %>%
    tsibble::fill_gaps() %>%
    fabletools::model(RW = fable::RW(observed))
  
  
  RW_forecast <- RW_model %>% fabletools::generate(h = h,
                                                bootstrap = T,
                                                times = 200)  %>%
    rename(model_id = .model,
           predicted = .sim) %>%
    as_tibble() %>% 
    group_by(site_id, time) %>%
    # uncertainty
    summarise(sd = sd(predicted),
              predicted = mean(predicted))  %>%
    # Add more columns
    mutate(h = as.numeric(time - min(time) + 1),
           start_time = start) 

  message('RW forecast for ', start)
  return(RW_forecast)
  
  
}

RW <- forecast_dates %>%
  map_dfr( ~ forecast.RW(targets = targets, start = .x))
#=======================================#

##### 2. Climatology ####

forecast.clim <- function(targets = targets, start, h=14, sd = 'mean') {
  # only the targets available before should be used to produce the forecast
  doy_targets <- targets %>%
    filter(time < start) %>%
    # this gives the day of year as if it were a leap year
    mutate(doy = ifelse(yday(time) > 59 & leap_year(time) == F,
                        yday(time) + 1,
                        yday(time))) %>%
    # find the day of year average
    group_by(site_id, doy) %>%
    summarise(predicted = mean(observed))
  
  # Day of year of the forecast dates
  forecast_doy <- data.frame(time = seq(start, as_date(start + days(h-1)), "1 day")) %>%
    mutate(doy = yday(time)) 
  
  # All combinations of doy and site_id
  site_doy <- expand.grid(doy = forecast_doy$doy, site_id = unique(targets$site_id))
  
  # produce the forecast
  clim_forecast <- 
    doy_targets %>%
    ungroup() %>%
    mutate(doy = as.integer(doy)) %>% 
    filter(doy %in% forecast_doy$doy) %>% 
    full_join(., site_doy, by = c('doy', 'site_id')) %>%
    full_join(., forecast_doy, by = 'doy') %>%
    select(-doy) 
  
    # Uncertainty - Calculate the sd between observations of each month per depth and then find the 
    # average (mean and median) of these, gives a different sd for each depth
  clim_forecast <- targets %>%
    filter(time < start) %>%
    mutate(month = zoo::as.yearmon(time)) %>%
    group_by(month, site_id) %>% 
    # Find the standard deviation of observations for each month
    summarise(sd = sd(observed)) %>%
    group_by(site_id) %>%
    summarise(sd_mean = mean(sd, na.rm = T),
              sd_median = median(sd, na.rm = T)) %>%
    # Combine with the point forecast
    full_join(clim_forecast, ., by='site_id') %>%
    mutate(start_time = start) %>%
    group_by(start_time) %>%
    # makes sure there are all combinations of site and time
    tidyr::complete(., site_id, time) %>%
    # If there is a gap in the forecast, linearly interpolate, should only be for leap year missingness
    mutate(predicted = imputeTS::na_interpolation(predicted),
           sd_mean = imputeTS::na_interpolation(sd_mean),
           sd_median = imputeTS::na_interpolation(sd_median)) %>%
    mutate(model_id = 'climatology',
           h = as.numeric(time - start + 1)) 
  
  message('climatology forecast for ', start)
  return(clim_forecast[, c('model_id', 'time', 'start_time', 'site_id', 'predicted', paste0('sd_', sd), 'h')])
  
}

climatology <- forecast_dates %>%
  map_dfr( ~ forecast.clim(targets = targets, start = .x))
#================================#