# Function to calculate the stratified period based on observations
library(tidyverse)
library(rLakeAnalyzer)

calc_strat_dates <- function(density_diff, targets) {
  targets_density<- read_csv(targets, show_col_types = F, progress = F) %>%
    filter(variable == 'temperature') %>% 
    # use a density difference between top and bottom to define stratification
    mutate(density = rLakeAnalyzer::water.density(observation)) %>%
    pivot_wider(names_from = depth, 
                values_from = c(density, observation), 
                id_cols = c(datetime, site_id)) %>%
    arrange(datetime) |> 
    mutate(strat = ifelse(density_8 - density_0 > density_diff &
                            observation_0 > observation_8,
                          1, 0),
           strat = imputeTS::na_interpolation(strat, option = 'linear'))
  
  # extract the dates of the stratified periods
  #using a loop function to go through each year and do the rle function
  
  strat <- data.frame(year = unique(year(targets_density$datetime)), 
                      length = NA,
                      start = NA,
                      end = NA)
  
  for (i in 1:nrow(strat)) {
    year_use <- strat$year[i]
    
    temp.dens <- targets_density %>%
      mutate(year = year(datetime)) %>% 
      filter(year == year_use)
    
    if (nrow(temp.dens) >= 300) {
      #run length encoding according to the strat var
      temp.rle <- rle(temp.dens$strat)
      
      #what is the max length for which the value is "norm"
      strat$length[i] <- max(temp.rle$lengths[temp.rle$values==1], 
                             na.rm = T)
      
      #stratification dates
      rle.strat <- data.frame(strat = temp.rle$values, 
                              lengths = temp.rle$lengths)
      
      # Get the end of ech run
      rle.strat$end <- cumsum(rle.strat$lengths)
      # Get the start of each run
      rle.strat$start <- rle.strat$end - rle.strat$lengths + 1
      
      # Sort rows by whehter it is stratified or not
      rle.strat <- rle.strat[order(rle.strat$strat), ]
      
      start.row <- rle.strat$start[which(rle.strat$length == max(rle.strat$lengths)
                                         & rle.strat$strat == 1)] 
      #gets the row with the start date
      #of the run which has the max length and is 1
      
      end.row <- rle.strat$end[which(rle.strat$length == max(rle.strat$lengths)
                                     & rle.strat$strat == 1)] 
      #gets the row with the end date
      #of the run which has the max length and is TRuE
      
      strat$start[which(strat$year == year_use)] <- as.character(temp.dens$datetime[start.row])
      strat$end[which(strat$year == year_use)] <- as.character(temp.dens$datetime[end.row])
      
    }
    
  }
  return(strat)
}

calc_strat_freq <- function(targets, density_diff, inverse = F) {
  
  if (inverse == T) {
    targets_density <- read_csv(targets, show_col_types = F, progress = F) %>%
      filter(variable == 'temperature') %>% 
      # use a density difference between top and bottom to define stratification
      mutate(density = rLakeAnalyzer::water.density(observation)) %>%
      pivot_wider(names_from = depth, 
                  values_from = c(density, observation), 
                  id_cols = c(datetime, site_id)) %>%
      mutate(dens_diff = density_8 - density_1,
             strat = ifelse((dens_diff > density_diff) &
                                  (observation_1 < observation_8),
                                1, 0),
             strat = imputeTS::na_interpolation(strat, option = 'linear'))
  }
  if (inverse == F) {
    targets_density <- read_csv(targets, show_col_types = F, progress = F) %>%
      filter(variable == 'temperature') %>% 
      # use a density difference between top and bottom to define stratification
      mutate(density = rLakeAnalyzer::water.density(observation)) %>%
      pivot_wider(names_from = depth, 
                  values_from = c(density, observation), 
                  id_cols = c(datetime, site_id)) %>%
      mutate(strat = ifelse(density_8 - density_0 > density_diff &
                                  observation_0 > observation_9,
                                1, 0),
             strat = imputeTS::na_interpolation(strat, option = 'linear'))  
  }
  
  strat <- data.frame(year = unique(year(targets_density$datetime)), 
                      freq = NA)
  
  # Loop through each year
  for (i in 1:nrow(strat)) {
    year_use <- strat$year[i]
    
    temp.dens <- targets_density %>%
      mutate(year = year(datetime)) %>% 
      filter(year == year_use)
    
    # Give a warming if not a whole year
    if (nrow(temp.dens) >= 365) {
      strat$freq[i] <- length(which(temp.dens$strat == 1))
    } else {
      strat$freq[i] <- length(which(temp.dens$strat == 1))
      message('Warning! Only ', nrow(temp.dens), ' days in ', year_use, ' obs data')
    }
    
  }
  return(strat)
}


calc_thermo <- function(start, end, targets) {
  
  wtr <- read_csv(targets, show_col_types = F, progress = F) %>%
    filter(variable == 'temperature') %>% 
    pivot_wider(names_from = depth, names_prefix = 'wtr_', values_from = observation) |> 
    select(-site_id, -variable)
    
  thermo_depth <- rLakeAnalyzer::ts.thermo.depth(wtr) |> 
    filter(datetime >= as_date(start) &
             datetime <= as_date(end)) |> 
    summarise(mean_thermo = mean(thermo.depth, na.rm = T))
  
  return(data.frame(start = start, end = end, mean_thermo = thermo_depth))
}
