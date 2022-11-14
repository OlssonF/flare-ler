# Script 2 in workflow to generate mme forecasts
# This script combines different model forecasts and saves them in a format that can be scored

# There will be 8 different scored forecasts
# 5 individual models (3 process based, 2 empirical baseline)
# and then 3 multi-model ensembles (LER, empirical baselines, LER+baselines) 

# library(GLM3r)
# library(FLAREr)
library(arrow)
library(tidyverse)
library(lubridate)

if (getwd() == dirname(rstudioapi::getSourceEditorContext()$path)) {
  setwd('../')
} 

rm(list = ls())
gc()
#### a) Create multi-model ensembles ####
# LER forecast
# ler_forecast <- data.table::fread('./forecasts/ler_forecast.csv.gz') %>%
#   filter(variable == 'temperature') %>%
#   # Recode the parameter
#   mutate(parameter = case_when(model_id == 'GLM' ~ as.numeric(parameter),
#                               model_id == 'GOTM' ~ as.numeric(parameter) + 1000,
#                               model_id == 'Simstrat' ~ as.numeric(parameter) + 2000))

columns_need <- c("datetime", "depth",
                  "family", "model_id",
                  "parameter", "prediction", 
                  "reference_datetime",
                  "site_id", "variable")

# Individual process model forecasts
message('read in individual models from file')
GOTM_forecast <- data.table::fread('./forecasts/GOTM_forecast.csv.gz') %>%
  filter(variable == 'temperature') %>%
  mutate(parameter = parameter + 0) %>%
  select(columns_need)
message('GOTM read')
GLM_forecast <- data.table::fread('./forecasts/GLM_v2_forecast.csv.gz') %>%
  filter(variable == 'temperature') %>%
  mutate(parameter = parameter + 1000)%>%
  select(columns_need)
message('GLM read')
Simstrat_forecast <- data.table::fread('./forecasts/Simstrat_forecast.csv.gz') %>%
  filter(variable == 'temperature')  %>%
  mutate(parameter = parameter + 2000) %>%
  select(columns_need)
message('Simstrat read')
# Baseline forecasts
RW_forecast <- data.table::fread('./forecasts/RW_forecast.csv.gz') %>%
  filter(variable == 'temperature') %>%
  mutate(reference_datetime = as.POSIXct(reference_datetime),
         datetime = as.POSIXct(datetime),
         parameter = parameter + 3000) %>%
  select(columns_need)
message('Persistence read')
climatology_forecast <- data.table::fread('./forecasts/climatology_forecast.csv.gz')  %>%
  filter(variable == 'temperature')  %>%
  mutate(reference_datetime = as.POSIXct(reference_datetime),
         datetime = as.POSIXct(datetime),
         parameter = parameter + 4000) %>%
  select(columns_need)
message('Climatology read')
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
message('generate the multi-model ensembles')
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
# OLD superceded method
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
message('write multi-model ensembles')
for (i in 1:length(forecasts_write)) {
  get(forecasts_write[i]) %>%
    mutate(model_id = gsub('_forecast', '', forecasts_write[i])) %>%
    data.table::fwrite(file = paste0('./forecasts/', forecasts_write[i], '.csv.gz'))
  message(forecasts_write[i], ' written')
}
#=================#
