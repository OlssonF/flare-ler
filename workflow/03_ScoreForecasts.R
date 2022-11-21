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
# Open the dataset of forecast parquets to be scored
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
  
  if (nrow(forecast_df) != 0) {
    # Score the forecast
    generate_forecast_score_arrow(targets_file = 'https://s3.flare-forecast.org/targets/ler_ms/fcre/fcre-targets-insitu.csv',
                                  forecast_df = forecast_df,
                                  local_directory = './scores')
    message(i, "/", nrow(model_refdates), " forecasts scored")
  } else {
    message('no forecast for ', model_refdates$model_id[i], ' ', model_refdates$reference_datetime[i] )
  }
  
} 
#=============================#

# write the ensemble and baseline scores to s3 bucket
# Set s3 environment variables
Sys.unsetenv("AWS_DEFAULT_REGION")
Sys.unsetenv("AWS_S3_ENDPOINT")
Sys.setenv("AWS_EC2_METADATA_DISABLED"="TRUE")
Sys.setenv('USE_HTTPS' = TRUE)


output_directory <- arrow::s3_bucket(bucket = "scores/ler_ms2/parquet",
                                     endpoint_override =  "s3.flare-forecast.org")

# Only write the ensemble and baseline scores to S3 bucket
# local score files
scores_parquets <- arrow::open_dataset('./scores/site_id=fcre')

# extract a list of model_id from the parquet
new_models <- c('RW', 'climatology', 'ler', 'empirical_ler', 'empirical')
new_scores <- scores_parquets |> 
  distinct(model_id) |> 
  filter(model_id %in% new_models) |> 
  pull() 

# write the new scores to the S3 bucket
for (i in 1:length(new_scores)) {
  df <- scores_parquets |> 
    filter(model_id == new_scores[i]) |> 
    mutate(site_id = 'fcre') |> 
    collect()
  
  arrow::write_dataset(df, path = output_directory, 
                       partitioning = c("site_id","model_id","reference_datetime"))
  message(new_scores[i], ' scores written to S3')
}


