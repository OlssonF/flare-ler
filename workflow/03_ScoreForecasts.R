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
source('./R/shadow_time.R')
if (!dir.exists('./scores/')) {
  dir.create('./scores/')
}


#### Read in forecasts ####
# Open the dataset of forecast parquets to be scored
forecast_parquets <- './forecasts/site_id=fcre'
forecast_s3 <- arrow::SubTreeFileSystem$create(forecast_parquets)
open_parquets <- arrow::open_dataset(forecast_s3) 


# vector of unique model_ids
models <- open_parquets |>
  filter(model_id != 'Simstrat_2') |> 
  distinct(model_id) |> 
  collect() |> 
  pull(model_id)
# vector of unique reference_Datetimes
unique_reftime <- open_parquets |>
  distinct(reference_datetime) |> 
  collect() |> 
  arrange(reference_datetime) |> 
  pull(reference_datetime)
model_refdates <- expand.grid(model_id = models, reference_datetime = unique_reftime)

#==================================#

#### Regular scoring function ####
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

# which ref_datetimes
first_date <- scores_parquets %>%
  distinct(reference_datetime) %>%
  summarise(min(reference_datetime)) %>%
  pull()

last_date <- scores_parquets %>%
  distinct(reference_datetime) %>%
  summarise(max(reference_datetime)) %>%
  pull()

forecast_dates <- paste0(seq.Date(as.Date(first_date),as.Date(last_date), 7), ' 00:00:00')

new_scores <- scores_parquets |> 
  distinct(model_id) |> 
  filter(model_id %in% new_models) |> 
  pull() 

# write the new scores to the S3 bucket
for (i in 1:length(new_scores)) {
  df <- scores_parquets |> 
    filter(model_id == new_scores[i],
           reference_datetime %in% forecast_dates) |> 
    mutate(site_id = 'fcre') |> 
    collect()
  
  arrow::write_dataset(df, path = output_directory, 
                       partitioning = c("site_id","model_id","reference_datetime"))
  message(new_scores[i], ' scores written to S3')
}


#===================================#

### Shadowing time ####
# Calculating the shadowing time uses 1 forecast each reference_Datetime-model combination independently
# read in the targets
targets_file <- read_csv('https://s3.flare-forecast.org/targets/ler_ms/fcre/fcre-targets-insitu.csv') |> 
  mutate(site_id = paste0(site_id, '_', depth))


# takes one forecast file (one model, one reference_datetime)
shadow_summary <- NULL

# Loop through each reference_datetime-model_id combination
for (i in 1:nrow(model_refdates)) {
  
  forecast_df <- open_parquets|>
    dplyr::filter(model_id == model_refdates$model_id[i], 
                  reference_datetime == model_refdates$reference_datetime[i]) |>
    mutate(site_id = 'fcre') |>
    collect()
  
  shadow_time <- calc_shadow_time(forecast_df, targets_df = targets_file, var = 'temperature',
                                  sd = 0.1, p = c(0.975, 0.025))
  
  if (is.null(shadow_time) == FALSE) {
    
    shadow_time <- shadow_time |> 
      mutate(model_id = model_refdates$model_id[i], 
             reference_datetime = model_refdates$reference_datetime[i])
    
    shadow_summary <- bind_rows(shadow_time, shadow_summary)
    # message('shadow time calculated for ', model_refdates$model_id[i], ' ', model_refdates$reference_datetime[i])
  }
  
  
}

#=============================================================#
