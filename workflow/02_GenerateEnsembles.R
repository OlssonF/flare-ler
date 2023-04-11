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

setwd(here::here())

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

# Individual process model forecasts
message('read in individual models from file')

# Reads in from a local directory
local_path <- './forecasts/reruns'

forecast_parquets <- arrow::open_dataset(local_path)

GOTM_forecast <- forecast_parquets |>
  filter(model_id == 'GOTM',
         variable == 'temperature') |>
  collect()
message('GOTM read')

GLM_forecast <- forecast_parquets |>
  filter(model_id == 'GLM',
         variable == 'temperature')  |>
  mutate(parameter = parameter + 1000)|>
  collect()
message('GLM read')

Simstrat_forecast <- forecast_parquets |>
  filter(model_id == 'Simstrat',
         variable == 'temperature') |>
  mutate(parameter = parameter + 2000)|>
  collect() 
message('Simstrat read')

# Baseline forecasts
RW_forecast <- forecast_parquets |>
  filter(model_id == 'RW',
         variable == 'temperature')  |>
  mutate(parameter = parameter + 3000)|>
  collect()
message('Persistence read')

climatology_forecast <- forecast_parquets |>
  filter(model_id == 'climatology',
         variable == 'temperature') |>
  mutate(parameter = parameter + 4000) |>
  collect() 
message('Climatology read')

# function to create the multi-model ensemble, by resampling each individual model
# ensemble before combining

create.mme <- function(forecasts, n = 256, ensemble_name, path = local_path) {
  n_models <- length(forecasts)
  sample <- round(n / n_models, digits = 0)

  mme_forecast <- NULL
  for (i in 1:length(forecasts)) {
    forecast_sample <- get(forecasts[i]) %>%
      distinct(parameter) %>%
      slice_sample(n = sample) %>%
      left_join(., get(forecasts[i]), by = "parameter", multiple = "all") %>%
      mutate(model_id = ensemble_name,
             site_id = 'fcre') %>%
      group_by(site_id, model_id, reference_datetime)
    
    mme_forecast <- bind_rows(mme_forecast, forecast_sample) 
    
  }
  
  mme_forecast |>
    arrow::write_dataset(local_path)
  message(ensemble_name, ' generated')
}

##### Generate the multi model ensembles ####
gc()

message('generate the multi-model ensembles')
create.mme(forecasts = c('RW_forecast',
                         'climatology_forecast'),
           ensemble_name = 'empirical')

create.mme(forecasts = c('RW_forecast',
                         'climatology_forecast',
                         'GOTM_forecast',
                         'GLM_forecast',
                         'Simstrat_forecast'),
           ensemble_name = 'empirical_ler')

create.mme(forecasts = c('GOTM_forecast',
                         'GLM_forecast',
                         'Simstrat_forecast'),
           ensemble_name = 'ler')
#===============================================#

