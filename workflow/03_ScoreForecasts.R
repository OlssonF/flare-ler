# Script 3 in workflow that takes all 8 forecasts and scores them using 
# the FLAREr function 

# library(FLAREr)
library(arrow)
library(tidyverse)
library(lubridate)

rm(list = ls())

if (getwd() == dirname(rstudioapi::getSourceEditorContext()$path)) {
  setwd('../')
} 

source('./R/scoring_function_arrow.R')

if (!dir.exists('./scores/')) {
  dir.create('./scores/')
}


#### c) Score forecasts ####
# Open th dataset of forecast parquets to be scored
forecast_parquets <- './forecasts/site_id=fcre'
forecast_s3 <- arrow::SubTreeFileSystem$create(forecast_parquets)
open_parquets <- arrow::open_dataset(forecast_s3) 

# vector of unique model_ids
models <- open_parquets |>
  distinct(model_id) |> 
  collect() |> 
  pull(model_id)
# vector of unique reference_Datetimes
unique_reftime <- open_parquets |>
  distinct(reference_datetime) |> 
  collect() |> 
  pull(reference_datetime)
model_refdates <- expand.grid(model_id = models, reference_datetime = unique_reftime)

for (i in 1:nrow(model_refdates)) {
  # subset the model and reference datetime
  forecast_df <- open_parquets|>
  dplyr::filter(model_id == model_refdates$model_id[i], 
                reference_datetime == model_refdates$reference_datetime[i]) |>
    mutate(site_id = 'fcre') |>
  collect()
  
  # Score the forecast
  generate_forecast_score_arrow(targets_file = 'https://s3.flare-forecast.org/targets/ler_ms/fcre/fcre-targets-insitu.csv',
                                forecast_df = forecast_df,
                                local_directory = './scores')
  message(i, "/", nrow(model_refdates), " forecasts scored")
} 
#=============================#
