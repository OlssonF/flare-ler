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
ler_forecast <- read_csv('./forecasts/ler_forecast.csv.gz') %>%
  filter(variable == 'temperature') %>%
  # Recode the ensemble
  mutate(ensemble = case_when(model_id == 'GLM' ~ ensemble,
                              model_id == 'GOTM' ~ ensemble + 1000,
                              model_id == 'Simstrat' ~ ensemble + 2000))

# Individual process model forecasts
GOTM_forecast <- read_csv('./forecasts/GOTM_forecast.csv.gz') %>%
  filter(variable == 'temperature')
GLM_forecast <- read_csv('./forecasts/GLM_forecast.csv.gz') %>%
  filter(variable == 'temperature')
Simstrat_forecast <- read_csv('./forecasts/Simstrat_forecast.csv.gz') %>%
  filter(variable == 'temperature')

# Baseline forecasts
RW_forecast <- read_csv('./forecasts/RW_forecast.csv.gz') %>%
  filter(variable == 'temperature')
climatology_forecast <- read_csv('./forecasts/climatology_forecast.csv.gz')  %>%
  filter(variable == 'temperature')

# Combine the empirical forecasts
empirical_forecast <- bind_rows(RW_forecast, climatology_forecast) %>%
  # Recode the ensemble 
  mutate(ensemble = case_when(model_id == 'RW' ~ ensemble,
                              model_id == 'climatology' ~ ensemble + 1000))

# Combine the empirical forecasts with LER (5 model ensemble)
empirical_ler_forecast <- bind_rows(RW_forecast, climatology_forecast) %>%
  bind_rows(., ler_forecast) %>%
  # Recode the ensemble 
  mutate(ensemble = case_when(model_id == 'RW' ~ ensemble,
                              model_id == 'climatology' ~ ensemble + 1000,
                              model_id == 'GLM' ~ ensemble + 2000,
                              model_id == 'GOTM' ~ ensemble + 3000,
                              model_id == 'Simstrat' ~ ensemble + 4000))

#### b) Resample multi-model ensembles ####
# For the multi-model ensembles resample the forecasts to get a ensemble of n = 256,
# so that ensemble size isn't impacting the result

##### Resample empirical #####
# Equal weighting of models
empirical_256_forecast <- empirical_forecast %>%
  group_by(model_id) %>%
  distinct(ensemble) %>%
  slice_sample(n = 256/n_groups(.)) %>%
  left_join(., empirical_forecast, by = c("model_id", "ensemble"))

# get a random 256 ensemble - any model
empirical_random256_forecast <- empirical_forecast %>%
  distinct(ensemble) %>%
  slice_sample(n=256) %>%
  left_join(., empirical_forecast, by = "ensemble")


##### Resample empirical-LER ######
# Equal weighting of models
empirical_ler_256_forecast <- empirical_ler_forecast %>%
  group_by(model_id) %>%
  distinct(ensemble) %>%
  slice_sample(n = 256/n_groups(.)) %>%
  left_join(., empirical_ler_forecast, by = c("model_id", "ensemble"))

# get a random 256 ensemble - any model
empirical_ler_random256_forecast <- empirical_ler_forecast %>%
  distinct(ensemble) %>%
  slice_sample(n=256) %>%
  left_join(., empirical_ler_forecast, by = "ensemble")


##### Resample LER #####
# Equal weighting of models
ler_256_forecast <- ler_forecast %>%
  group_by(model_id) %>%
  distinct(ensemble) %>%
  slice_sample(n = floor(256/n_groups(.))) %>%
  left_join(., ler_forecast, by = c("model_id", "ensemble"))

# get a random 256 ensemble - any model
ler_random256_forecast <- ler_forecast %>%
  distinct(ensemble) %>%
  slice_sample(n=255) %>%
  left_join(., ler_forecast, by = "ensemble") 


# Write the multi-model ensemble forecasts so that they can be scored
forecasts_env <- ls(pattern = 'forecast')
# the forecasts derived in this script are the combined empirical and the resampled MMEs
forecasts_write <- unique(c(forecasts_env[str_detect(forecasts_env, '256')], 
                            forecasts_env[str_detect(forecasts_env, 'empirical')]))

for (i in 1:length(forecasts_write)) {
  get(forecasts_write[i]) %>%
        mutate(model_id = gsub('_forecast', '', forecasts_write[i])) %>%
    write_csv(file = paste0('./forecasts/', forecasts_write[i], '.csv.gz'))
  message(forecasts_write[i], ' written')
}
#=================#

#### c) Score forecasts ####
# Get a list of the forecasts to be scored
forecast_files <- list.files('./forecasts')

for (i in 1:length(forecast_files)) {
  forecast_file <- paste0('./forecasts/', forecast_files[i])
  FLAREr::generate_forecast_score(targets_file = 'https://s3.flare-forecast.org/targets/ler_ms/fcre/fcre-targets-insitu.csv',
                                  forecast_file = forecast_file,
                                  output_directory = './scores')
  message(forecast_files[i], ' scored')
}

#=============================#