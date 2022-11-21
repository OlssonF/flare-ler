# Script 3 in workflow to analyse the scored forecasts
# The forecasts include 3 process-based models within LER, 2 empirical models (random walk and climatology),
  # and 3 different ensembles (LER, LER+empirical, empirical). Each ensemble is also run with 3 different resampling methods
  # a) no resampling, b) equal model weighting with 200 members, rc) andom selection with 200 members
  # 14 forecasts in total
# library(FLAREr)
library(arrow)
library(tidyverse)
library(lubridate)

source('R/shadow_time.R')
source('R/time_functions.R')
#### a) read in scored forecasts ####
scores_parquets <- arrow::open_dataset('./scores/site_id=fcre')

# extract a list of model_id from the parquet
distinct_models <- scores_parquets |> distinct(model_id) |> pull()

for (i in 1:length(distinct_models)) {
  scores <- scores_parquets |> 
    filter(model_id == distinct_models[i],
           horizon > 0,
           variable == 'temperature') |> 
    collect()
  assign(paste0(distinct_models[i], '_scored'), scores)
}


#extract list of different types of scored forecasts for comparisons
all_df <- ls(pattern = 'scored')
empirical_forecasts <- all_df[c(grep('empirical', all_df),
                                grep('climatology', all_df),
                                grep('RW', all_df))]
process_forecasts <- all_df[-c(grep('empirical', all_df),
                               grep('climatology', all_df),
                               grep('RW', all_df))]

individual_forecasts <- all_df[-unique(c(grep('empirical', all_df),
                                         grep('ler', all_df)))]

ensemble_forecasts <- all_df[unique(c(grep('empirical', all_df),
                                       grep('ler', all_df)))]


#===================================#
#### b) combine forecast scores ####
ensemble_scored <- bind_rows(mget(ensemble_forecasts))
all_scored <- bind_rows(mget(c(individual_forecasts, ensemble_forecasts)))
all_empirical_scored <- bind_rows(mget(c(empirical_forecasts)))
individual_scored <- bind_rows(mget(c(individual_forecasts)))
all_process_scored <- bind_rows(mget(c(process_forecasts)))
#===================================#

# Calculate shadowing time
df_comb <- all_scored %>%
  filter(!is.na(observation)) %>%
  distinct(model_id, reference_datetime, depth) %>%
  mutate(df = paste0(model_id, '_scored')) %>%
  rename(ref_datetime = reference_datetime,
         mod = model_id,
         dep = depth)

# find the shadowing time for each forecast (1 reference_datetime, 1 depth, 1 model_id)
shadowing <- purrr::pmap_dfr(df_comb, shadow_length)

shadowing %>% group_by(depth, model_id) %>% 
  filter(model_id %in% c('empirical', 'empirical_ler', 'ler')) |> 
  summarise(sd_st = sd(shadow_time),
            shadow_time=mean(shadow_time)) %>%
  ggplot(.,aes(fill=model_id, y=shadow_time, x=as.factor(depth))) + 
  geom_col(position = 'dodge') + 
  geom_errorbar(aes(ymin=shadow_time-sd_st, 
                    ymax=shadow_time+sd_st, group = as.factor(model_id)), 
                width=0.4, alpha=0.5, size=0.5, position = position_dodge(.9))
#===================================#
#### c) plot example forecasts ####
ensemble_scored %>%
  filter(variable == 'temperature',
         horizon > 0) %>%
  na.omit() %>%
  ggplot(., aes(x=datetime)) +
  geom_point(aes(y=observation)) +
  geom_ribbon(aes(ymax = quantile97.5, ymin = quantile02.5, 
                  fill =model_id, group = interaction(reference_datetime, model_id)), alpha = 0.2)+
  geom_line(aes(y=mean, colour = model_id, group = interaction(reference_datetime, model_id))) +
  facet_grid(model_id~depth) +
  coord_cartesian(xlim = ymd_hms(c("2022-03-01 00:00:00",
                                   "2022-10-31 00:00:00")))


individual_scored %>%
  filter(variable == 'temperature',
         horizon > 0,
         depth == 1) %>%
  na.omit() %>%
  ggplot(., aes(x=datetime)) +
  geom_point(aes(y=observation)) +
  geom_ribbon(aes(ymax = quantile97.5, ymin = quantile02.5, 
                  fill =model_id, group = interaction(reference_datetime, model_id)), alpha = 0.2)+
  geom_line(aes(y=mean, colour = model_id, group = interaction(reference_datetime, model_id))) +
  facet_wrap(model_id~depth) +
  coord_cartesian(xlim = ymd_hms(c("2021-03-01 00:00:00",
                                   "2021-10-31 00:00:00")))

##====================================#

#### d) plot scores ####
ensemble_scored %>%
  mutate(bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         horizon > 0) %>%
  group_by(horizon, model_id, depth) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= crps, colour = model_id)) +
  geom_line() +
  facet_wrap(~depth) +
  scale_colour_viridis_d() +
  theme_bw()

ensemble_scored %>%
  mutate(bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         horizon > 0) %>%
  group_by(horizon, model_id, depth) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= logs, colour = model_id)) +
  geom_line() +
  facet_wrap(~depth) +
  scale_colour_viridis_d() +
  theme_bw()



all_scored %>%
  mutate(bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         # depth == 'fcre-1',
         horizon > 0) %>%
  group_by(horizon, model_id, depth) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= crps, colour = model_id)) +
  geom_line() +
  facet_wrap(~depth)+
  scale_colour_viridis_d() +
  theme_bw()

all_scored %>%
  mutate(bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         # depth == 'fcre-1',
         horizon > 0) %>%
  group_by(horizon, model_id, depth) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= logs, colour = model_id)) +
  geom_line() +
  facet_wrap(~depth)+
  scale_colour_viridis_d() +
  theme_bw()


all_empirical_scored %>%
  mutate(bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         horizon > 0) %>%
  group_by(horizon, model_id, depth) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= crps, colour = model_id)) +
  geom_line()+
  facet_wrap(~depth) +
  scale_colour_viridis_d() +
  theme_bw()

all_process_scored %>%
  filter(model_id != 'Simstrat') %>%
  mutate(bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         horizon > 0) %>%
  group_by(horizon, model_id, depth) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= crps, colour = model_id)) +
  geom_line()+
  facet_wrap(~depth) +
  scale_colour_viridis_d() +
  theme_bw()

all_empirical_scored %>%
  mutate(bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         horizon > 0) %>%
  group_by(horizon, model_id, depth) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= logs, colour = model_id)) +
  geom_line()+
  facet_wrap(~depth) +
  scale_colour_viridis_d() +
  theme_bw()



individual_scored %>%
  mutate(bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         # depth == 'fcre-1',
         horizon > 0) %>%
  group_by(horizon, model_id, depth) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= crps, colour = model_id)) +
  geom_line() +
  facet_wrap(~depth)+
  scale_colour_viridis_d() +
  theme_bw()

individual_scored %>%
  mutate(bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         horizon > 0) %>%
  group_by(horizon, model_id, depth) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= logs, colour = model_id)) +
  geom_line() +
  facet_wrap(~depth)+
  scale_colour_viridis_d() +
  theme_bw()


# seasonal analysis

individual_scored %>%
  mutate(season = as_season(datetime),
         bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         horizon > 0) %>%
  group_by(horizon, model_id, depth, season) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= crps, colour = model_id)) +
  geom_line() +
  facet_grid(season~depth)+
  scale_colour_viridis_d() +
  theme_bw()


all_empirical_scored %>%
  mutate(season = as_season(datetime),
         bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         horizon > 0) %>%
  group_by(horizon, model_id, depth, season) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= crps, colour = model_id)) +
  geom_line() +
  facet_grid(season~depth)+
  scale_colour_viridis_d() +
  theme_bw()

ensemble_scored %>%
  mutate(season = as_season(datetime),
         bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         horizon > 0) %>%
  group_by(horizon, model_id, depth, season) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= crps, colour = model_id)) +
  geom_line() +
  facet_grid(season~depth)+
  scale_colour_viridis_d() +
  theme_bw()

all_scored %>%
  mutate(season = as_season(datetime),
         year = which_year(reference_datetime),
         bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         horizon > 0, depth == 2) %>%
  group_by(horizon, model_id, depth, season) %>%
  summarise_if(is.numeric, mean, na.rm = T) |> 
  na.omit() %>%
  ggplot(., aes(x=horizon, y= crps, colour = model_id)) +
  geom_line() +
  facet_grid(~season, scales = 'free')+
  # scale_colour_viridis_d() +
  theme_bw()



