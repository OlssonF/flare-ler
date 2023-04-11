# Script 3 in workflow that takes all 8 forecasts and scores them using 
# the FLAREr function 

# library(FLAREr)
library(arrow)
library(tidyverse)
library(lubridate)
options(dplyr.summarise.inform = FALSE)
options(arrow.pull_as_vector = TRUE)
rm(list = ls())

setwd(here::here())

source('./R/scoring_function_arrow.R')
source('./R/shadow_time.R')

local_path <- './scores/reruns'

if (!dir.exists(local_path)) {
  dir.create(local_path)
}


#### Read in forecasts ####
# Open the dataset of forecast parquets to be scored
forecast_parquets <- './forecasts/reruns/site_id=fcre'
forecast_s3 <- arrow::SubTreeFileSystem$create(forecast_parquets)
open_parquets <- arrow::open_dataset(forecast_s3) 


# vector of unique model_ids
models <- open_parquets |>
  # filter(model_id != 'Simstrat_2') |> 
  distinct(model_id) |> 
  pull()

# vector of unique reference_datetimes
unique_reftime <- open_parquets |>
  distinct(reference_datetime) |> 
  collect() |> 
  arrange(reference_datetime) |> 
  pull(reference_datetime)
model_refdates <- expand.grid(model_id = models, reference_datetime = unique_reftime)

to_score <- model_refdates |> 
  filter(model_id %in% c("RW", 
                         "empirical",
                         "empirical_ler",
                         "ler",
                         "climatology"))

#==================================#

#### Regular scoring function ####
for (i in 1:nrow(to_score)) {
  # subset the model and reference datetime
  forecast_df <- open_parquets|>
  dplyr::filter(model_id == to_score$model_id[i], 
                reference_datetime == to_score$reference_datetime[i]) |>
    mutate(site_id = 'fcre') |>
  collect()
  
  if (nrow(forecast_df) != 0) {
    # Score the forecast
    generate_forecast_score_arrow(targets_file = 'https://s3.flare-forecast.org/targets/ler_ms/fcre/fcre-targets-insitu.csv',
                                  forecast_df = forecast_df,
                                  local_directory = local_path)
    message(i, "/", nrow(to_score), " forecasts scored")
    
    
  } else {
    message('no forecast for ', to_score$model_id[i], ' ', to_score$reference_datetime[i] )
  }
  
} 
#=============================#

# write the ensemble and baseline scores to s3 bucket
# Set s3 environment variables
Sys.unsetenv("AWS_DEFAULT_REGION")
Sys.unsetenv("AWS_S3_ENDPOINT")
Sys.setenv("AWS_EC2_METADATA_DISABLED"="TRUE")
Sys.setenv('USE_HTTPS' = TRUE)


output_directory <- arrow::s3_bucket(bucket = "scores/ler_ms3/parquet",
                                     endpoint_override =  "s3.flare-forecast.org")

# Only write the ensemble and baseline scores to S3 bucket
# local score files
scores_parquets <- arrow::open_dataset(file.path(local_path, 'site_id=fcre'))

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
targets_df <- read_csv('https://s3.flare-forecast.org/targets/ler_ms/fcre/fcre-targets-insitu.csv') |> 
  mutate(site_id = paste0(site_id, '_', depth))


# # takes one forecast file (one model, one reference_datetime)
shadow_summary  <- purrr::map2_dfr(
  .x = model_refdates$model_id,
  .y = model_refdates$reference_datetime,
  .f = function(mod, ref_datetime) {
    
    message(mod, ' ',ref_datetime)
    
    forecast_df <- open_parquets|>
      dplyr::filter(model_id == mod, 
                    reference_datetime == ref_datetime) |>
      mutate(site_id = 'fcre') |>
      collect()
    
    shadow_time <- calc_shadow_time(forecast_df, targets_df, var = 'temperature',
                                    sd = 0.2, p = c(0.975, 0.025))  
    if (!is.null(shadow_time)) {
      shadow_time |> mutate(model_id = mod,
                            reference_datetime = ref_datetime)
    }
      
  }
)

write_csv(shadow_summary, file = 'shadow_summary.csv')
#=============================================================#

# choose 1 to have a go at plotting

example_refdate <- '2020-10-25 00:00:00' # '2021-06-20 00:00:00'
mod <- 'RW'
dep <- 8 # 1

test_forecast <- open_parquets|>
  dplyr::filter(depth == dep,
                model_id == mod, 
                reference_datetime == example_refdate) |>
  mutate(site_id = 'fcre') |>
  collect()

test_shadow <- test_forecast %>%
  select(datetime, reference_datetime, depth, site_id,
         variable, parameter, prediction, model_id) |> 
  filter(variable == 'temperature', 
         datetime >= as_datetime(reference_datetime)) |> 
  mutate(site_id = paste0(site_id, '_', depth)) |> 
  left_join(targets_df, by = c("datetime", "depth", "site_id", "variable")) |> 
  na.omit(observation) |> 
  mutate(obs_upper = qnorm(mean = observation, sd = 0.2, p = max(0.975)),
         obs_lower = qnorm(mean = observation, sd = 0.2, p = min(0.025)),
         shadow = ifelse(prediction <= obs_upper &
                           prediction >= obs_lower,
                         T, F))

p <- test_shadow  |> 
  filter(shadow == F) |> 
  group_by(parameter) |> 
  summarise(stop = min(datetime))

p2 <- p |> 
  full_join(test_shadow, by = 'parameter', multiple = 'all') |> 
  filter(datetime <= stop) |> 
  mutate(best = ifelse(stop == max(stop), T, F))

ggplot(p2, aes(x=datetime, y=prediction, group = parameter, colour = best)) +
  geom_point(aes(y=observation), colour = 'grey') +
  geom_errorbar(aes(ymax = obs_upper, ymin = obs_lower), colour = 'grey') +
  labs(title = paste('depth = ', dep, mod, example_refdate)) +
  geom_line(alpha = 0.5)  +
  scale_colour_discrete(guide = 'none')

