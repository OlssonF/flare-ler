# Function to calculate the stratified period based on observations
library(tidyverse)
library(rLakeAnalyzer)

stratification_density <- function(density_diff, targets) {
  targets_density<- read_csv(targets, show_col_types = F, progress = F) %>%
    filter(variable == 'temperature') %>% 
    # use a density difference between top and bottom to define stratification
    mutate(density = rLakeAnalyzer::water.density(observation)) %>%
    pivot_wider(names_from = depth, 
                values_from = c(density, observation), 
                id_cols = c(datetime, site_id)) %>%
    mutate(strat = ifelse(density_8.75 - density_0 > density_diff &
                            observation_0 > observation_8.75,
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



