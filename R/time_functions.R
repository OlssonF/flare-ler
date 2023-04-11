# function to extract the season
as_season <- function(datetime) {
  factor(lubridate::quarter(datetime, fiscal_start = 3),
         levels = c(1,2,3,4), 
         labels = c('spring', 'summer', 'autumn', 'winter'))
}

# extract the year of the forecasts (1st year runs Oct-Oct etc.)
which_year <- function(datetime) {
  start <- ymd(min(datetime))
  start2 <- start + years(1)
  
  ifelse(between(ymd(datetime), start, start2),
         'yr_1', 'yr_2')
}


is_strat  <- function(datetime, strat_dates) {
  
  df <- as.data.frame(datetime) %>%
    mutate(year = year(datetime)) %>%
    left_join(., strat_dates, by = 'year') %>%
    mutate(start = as_datetime(start),
           end = as_datetime(end)) %>% 
    mutate(strat = ifelse(year == 2023, 'mixed_period', 
                          ifelse(datetime >= start & datetime <= end , 'stratified_period', 'mixed_period')))
  
  return(df$strat)
}
