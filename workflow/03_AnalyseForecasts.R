# Script 3 in workflow to analyse the scored forecasts
# The forecasts include 3 process-based models within LER, 2 empirical models (random walk and climatology),
  # and 3 different ensembles (LER, LER+empirical, empirical). Each ensemble is also run with 3 different resampling methods
  # a) no resampling, b) equal model weighting with 200 members, rc) andom selection with 200 members
  # 14 forecasts in total

library(GLM3r)
library(FLAREr)
library(arrow)
library(tidyverse)
library(lubridate)

#### a) read in scored forecasts ####
score_files <- list.files('./scores/', full.names = T)

for (i in 1:length(score_files)) {
  name <- gsub('_forecast.parquet', '_scored', gsub('./scores/', '',score_files[i]))

  score_file <- arrow::read_parquet(score_files[i]) %>%
    mutate(model_id = gsub('_scored', '', name)) 
  
  assign(name, score_file)
  
  message('read in ', name)
}

# extract list of diferent types of scored forecasts for comparisons
all_df <- ls(pattern = 'scored')
empirical_forecasts <- all_df[c(grep('empirical', all_df),
                                grep('climatology', all_df),
                                grep('RW', all_df))][c(1,2,5,6)]

ensemble_forecasts <- all_df[grep('_256', all_df)]

individual_forecasts <- all_df[-unique(c(grep('empirical', all_df),
                                         grep('ler', all_df)))]



#===================================#
# combine forecast scores
ensemble_scored <- bind_rows(mget(ensemble_forecasts))
all_scored <- bind_rows(mget(c(individual_forecasts, ensemble_forecasts)))
all_empirical_scored <- bind_rows(mget(c(empirical_forecasts)))
individual_scored <- bind_rows(mget(c(individual_forecasts)))



example_forecast <- ensemble_scored %>%
  filter(variable == 'temperature',
         horizon >= 0,
         site_id == 'fcre-8') %>%
  na.omit() %>%
  ggplot(., aes(x=datetime)) +
  geom_point(aes(y=observed)) +
  geom_ribbon(aes(ymax = quantile97.5, ymin = quantile02.5, 
                  fill =model_id, group = interaction(reference_datetime, model_id)), alpha = 0.2)+
  geom_line(aes(y=mean, colour = model_id, group = interaction(reference_datetime, model_id))) +
  facet_wrap(model_id~site_id) +
  coord_cartesian(xlim = ymd_hms(c("2021-05-01 00:00:00",
                                   "2021-08-31 00:00:00")))

ggsave(example_forecast, filename = './plots/example_forecast3.png', 
       width = 10, height = 10)

example_forecast <- individual_scored %>%
  filter(variable == 'temperature',
         horizon >= 0,
         site_id == 'fcre-1') %>%
  na.omit() %>%
  ggplot(., aes(x=datetime)) +
  geom_point(aes(y=observed)) +
  geom_ribbon(aes(ymax = quantile97.5, ymin = quantile02.5, 
                  fill =model_id, group = interaction(reference_datetime, model_id)), alpha = 0.2)+
  geom_line(aes(y=mean, colour = model_id, group = interaction(reference_datetime, model_id))) +
  facet_wrap(model_id~site_id) +
  coord_cartesian(xlim = ymd_hms(c("2021-05-01 00:00:00",
                                   "2021-08-31 00:00:00")))

# Compare the different model ensemble scores
ensemble_crps_p <- ensemble_scored %>%
  mutate(bias = mean - observed,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         horizon >= 0) %>%
  group_by(horizon, model_id, site_id) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= crps, colour = model_id)) +
  geom_line() +
  facet_wrap(~site_id) +
  scale_colour_viridis_d() +
  theme_bw()

ensemble_logs_p <- ensemble_scored %>%
  mutate(bias = mean - observed,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         horizon >= 0) %>%
  group_by(horizon, model_id, site_id) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= logs, colour = model_id)) +
  geom_line() +
  facet_wrap(~site_id) +
  scale_colour_viridis_d() +
  theme_bw()



all_crps_p <-all_scored %>%
  mutate(bias = mean - observed,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         # site_id == 'fcre-1',
         horizon >= 0) %>%
  group_by(horizon, model_id, site_id) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= crps, colour = model_id)) +
  geom_line() +
  facet_wrap(~site_id)+
  scale_colour_viridis_d() +
  theme_bw()

all_logs_p <-all_scored %>%
  mutate(bias = mean - observed,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         # site_id == 'fcre-1',
         horizon >= 0) %>%
  group_by(horizon, model_id, site_id) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= logs, colour = model_id)) +
  geom_line() +
  facet_wrap(~site_id)+
  scale_colour_viridis_d() +
  theme_bw()


empirical_crps_p <- all_empirical_scored %>%
  mutate(bias = mean - observed,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         # site_id == 'fcre-1',
         horizon >= 0) %>%
  group_by(horizon, model_id, site_id) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= crps, colour = model_id)) +
  geom_line()+
  facet_wrap(~site_id) +
  scale_colour_viridis_d() +
  theme_bw()

empirical_logs_p <- all_empirical_scored %>%
  mutate(bias = mean - observed,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         # site_id == 'fcre-1',
         horizon >= 0) %>%
  group_by(horizon, model_id, site_id) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= logs, colour = model_id)) +
  geom_line()+
  facet_wrap(~site_id) +
  scale_colour_viridis_d() +
  theme_bw()



individual_crps_p <- individual_scored %>%
  mutate(bias = mean - observed,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         # site_id == 'fcre-1',
         horizon >= 0) %>%
  group_by(horizon, model_id, site_id) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= crps, colour = model_id)) +
  geom_line() +
  facet_wrap(~site_id)+
  scale_colour_viridis_d() +
  theme_bw()

individual_logs_p <- individual_scored %>%
  mutate(bias = mean - observed,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         # site_id == 'fcre-1',
         horizon >= 0) %>%
  group_by(horizon, model_id, site_id) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= logs, colour = model_id)) +
  geom_line() +
  facet_wrap(~site_id)+
  scale_colour_viridis_d() +
  theme_bw()


# seasonal analysis
as_season <- function(datetime) {
  factor(lubridate::quarter(datetime, fiscal_start = 3),
         levels = c(1,2,3,4), 
         labels = c('spring', 'summer', 'autumn', 'winter'))
}

ensemble_season_p <- ensemble_scored %>%
  mutate(season = as_season(datetime),
         bias = mean - observed,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         horizon >= 0) %>%
  group_by(horizon, model_id, site_id, season) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= crps, colour = model_id)) +
  geom_line() +
  facet_grid(season~site_id)+
  scale_colour_viridis_d() +
  theme_bw()


all_season_p <- all_empirical_scored %>%
  mutate(season = as_season(datetime),
         bias = mean - observed,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         # site_id == 'fcre-1',
         horizon >= 0) %>%
  group_by(horizon, model_id, site_id, season) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= crps, colour = model_id)) +
  geom_line() +
  facet_grid(season~site_id)+
  scale_colour_viridis_d() +
  theme_bw()









plots_save <- ls(pattern = '_p')

for(i in 1:length(plots_save)) {
  ggsave(get(plots_save[i]), 
         filename = paste0('./plots/', gsub(pattern =  "_p", ".png", plots_save[i])),
         width = 15, height = 7.5)
}

