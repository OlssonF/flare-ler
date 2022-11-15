# Script 3 in workflow that takes all 8 forecasts and scores them using 
# the FLAREr function 

library(GLM3r)
library(FLAREr)
library(arrow)
library(tidyverse)
library(lubridate)

rm(list = ls())

if (getwd() == dirname(rstudioapi::getSourceEditorContext()$path)) {
  setwd('../')
} 

source('./R/scoring_function.R')

if (!exists('./scores/')) {
  dir.create('./scores/')
}


#### c) Score forecasts ####
# Get a list of the forecasts to be scored
forecast_files <- list.files('./forecasts', pattern = '.gz')
# forecasts_to_score <- forecast_files[c(1,6,7,8,9,12,13,14)]

for (i in 1:length(forecast_files)) {
  forecast_file <- paste0('./forecasts/', forecast_files[i])
  generate_forecast_score_2(targets_file = 'https://s3.flare-forecast.org/targets/ler_ms/fcre/fcre-targets-insitu.csv',
                                  forecast_file = forecast_file,
                                  output_directory = './scores')
  message(forecast_files[i], ' scored')
}

#=============================#