# function to extract the season
as_season <- function(datetime) {
  factor(lubridate::quarter(datetime, fiscal_start = 3),
         levels = c(1,2,3,4), 
         labels = c('spring', 'summer', 'autumn', 'winter'))
}

# extract the year of the forecasts (1st year runs Oct-Oct etc.)
which_year <- function(datetime) {
  start <- ymd_hms(min(datetime))
  start2 <- start + years(1)
  
  ifelse(between(ymd_hms(datetime), start, start2),
         'yr_1', 'yr_2')
}
