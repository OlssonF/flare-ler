# Script 2 in workflow to score forecast
# The scoring starts by combining different forecasts, then scores different combinations

# There will be 14 different scored forecasts
  # 5 individual models (3 process based, 2 baseline)
  # and then 3 multi-model ensembles (LER, baselines, LER+baselines) - 3 of each based on resampling

library(GLM3r)
library(FLAREr)
library(arrow)
library(tidyverse)
library(lubridate)

if (getwd() == dirname(rstudioapi::getSourceEditorContext()$path)) {
  setwd('../')
} 
#### a) Create multi-model ensembles ####
# LER forecast
# ler_forecast <- data.table::fread('./forecasts/ler_forecast.csv.gz') %>%
#   filter(variable == 'temperature') %>%
#   # Recode the parameter
#   mutate(parameter = case_when(model_id == 'GLM' ~ as.numeric(parameter),
#                               model_id == 'GOTM' ~ as.numeric(parameter) + 1000,
#                               model_id == 'Simstrat' ~ as.numeric(parameter) + 2000))

# Individual process model forecasts
GOTM_forecast <- data.table::fread('./forecasts/GOTM_forecast.csv.gz') %>%
  filter(variable == 'temperature') %>%
  mutate(parameter = parameter + 0)
GLM_forecast <- data.table::fread('./forecasts/GLM_v2_forecast.csv.gz') %>%
  filter(variable == 'temperature') %>%
  mutate(parameter = parameter + 1000)
Simstrat_forecast <- data.table::fread('./forecasts/Simstrat_forecast.csv.gz') %>%
  filter(variable == 'temperature')  %>%
  mutate(parameter = parameter + 2000)

# Baseline forecasts
RW_forecast <- data.table::fread('./forecasts/RW_forecast.csv.gz') %>%
  filter(variable == 'temperature') %>%
  mutate(start_time = as.POSIXct(start_time),
         datetime = as.POSIXct(datetime),
         parameter = parameter + 3000)
climatology_forecast <- data.table::fread('./forecasts/climatology_forecast.csv.gz')  %>%
  filter(variable == 'temperature')  %>%
  mutate(start_time = as.POSIXct(start_time),
         datetime = as.POSIXct(datetime),
         parameter = parameter + 4000)

# function to create the multi-model ensemble, by resampling each individual model
  # ensemble before combining

create.mme <- function(forecasts, n = 256) {
  n_models <- length(forecasts)
  sample <- round(n / n_models, digits = 0)
  
  mme_forecast <- NULL
  for (i in 1:length(forecasts)) {
    forecast_sample <- get(forecasts[i]) %>%
      distinct(parameter) %>%
      slice_sample(n = sample) %>%
      left_join(., get(forecasts[i]), by = "parameter")
    
    mme_forecast <- bind_rows(mme_forecast, forecast_sample)
    message(forecasts[i])
    
  }
  return(mme_forecast)
}

##### Generate the multi model ensembles ####
empirical_256_forecast <- create.mme(forecasts = c('RW_forecast',
                                                   'climatology_forecast'))


empirical_ler_256_forecast <- create.mme(forecasts = c('RW_forecast',
                                                       'climatology_forecast',
                                                       'GOTM_forecast',
                                                       'GLM_forecast',
                                                       'Simstrat_forecast'))

ler_256_forecast <- create.mme(forecasts = c('GOTM_forecast',
                                             'GLM_forecast',
                                             'Simstrat_forecast'))
#===============================================#

# Combine the empirical forecasts
# empirical_forecast <- bind_rows(RW_forecast, climatology_forecast) #%>%
  # Recode the ensemble 
  # mutate(parameter = case_when(model_id == 'RW' ~ as.numeric(parameter),
  #                             model_id == 'climatology' ~ as.numeric(parameter) + 1000))

# Combine the empirical forecasts with LER (5 model ensemble) 
  # - each model is equally weighted across the 256 members, so that ensemble size isn't impacting the result

# 
# ##### Resample empirical #####
# # Equal weighting of models
# empirical_256_forecast <- empirical_forecast %>%
#   group_by(model_id) %>%
#   distinct(parameter) %>%
#   slice_sample(n = 256/n_groups(.)) %>%
#   left_join(., empirical_forecast, by = c("model_id", "parameter"))
# 
# # get a random 256 ensemble - any model
# empirical_random256_forecast <- empirical_forecast %>%
#   distinct(parameter) %>%
#   slice_sample(n=256) %>%
#   left_join(., empirical_forecast, by = "parameter")
# 

##### Resample empirical-LER ######
# Equal weighting of models
# empirical_ler_256_forecast <- empirical_ler_forecast %>%
#   group_by(model_id) %>%
#   distinct(parameter) %>%
#   slice_sample(n = 256/n_groups(.)) %>%
#   left_join(., empirical_ler_forecast, by = c("model_id", "parameter"))
# 
# # get a random 256 ensemble - any model
# empirical_ler_random256_forecast <- empirical_ler_forecast %>%
#   distinct(parameter) %>%
#   slice_sample(n=256) %>%
#   left_join(., empirical_ler_forecast, by = "parameter")
# 

##### Resample LER #####
# # Equal weighting of models
# ler_256_forecast <- ler_forecast %>%
#   group_by(model_id) %>%
#   distinct(parameter) %>%
#   slice_sample(n = floor(256/n_groups(.))) %>%
#   left_join(., ler_forecast, by = c("model_id", "parameter"))
# 
# # get a random 256 ensemble - any model
# ler_random256_forecast <- ler_forecast %>%
#   distinct(parameter) %>%
#   slice_sample(n=255) %>%
#   left_join(., ler_forecast, by = "parameter") 
# 

# Write the multi-model ensemble forecasts so that they can be scored
forecasts_env <- ls(pattern = 'forecast')
# the forecasts derived in this script are the combined empirical and the resampled MMEs
forecasts_write <- unique(c(forecasts_env[str_detect(forecasts_env, '256')], 
                            forecasts_env[str_detect(forecasts_env, 'empirical')]))

for (i in 1:length(forecasts_write)) {
  get(forecasts_write[i]) %>%
        mutate(model_id = gsub('_forecast', '', forecasts_write[i])) %>%
    data.table::fwrite(file = paste0('./forecasts/', forecasts_write[i], '.csv.gz'))
  message(forecasts_write[i], ' written')
}
#=================#


rm(list = ls())
#### c) Score forecasts ####
# Get a list of the forecasts to be scored
forecast_files <- list.files('./forecasts', pattern = '.gz')
# forecasts_to_score <- forecast_files[c(1,6,7,8,9,12,13,14)]

for (i in 1:length(forecast_files)) {
  forecast_file <- paste0('./forecasts/', forecast_files[i])
  FLAREr::generate_forecast_score(targets_file = 'https://s3.flare-forecast.org/targets/ler_ms/fcre/fcre-targets-insitu.csv',
                                  forecast_file = forecast_file,
                                  output_directory = './scores')
  message(forecast_files[i], ' scored')
}

#=============================#