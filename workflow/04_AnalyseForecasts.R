# Exploratory visualisations only
# library(FLAREr)
library(arrow)
library(tidyverse)
library(lubridate)

source('R/shadow_time.R')
source('R/time_functions.R')
source('R/stratified_period.R')

all_scored_recode <-  all_scored |> 
  mutate(model_id = plyr::revalue(model_id, c("empirical_ler"="full_ensemble",
                                              "GLM"="PM_1",
                                              "GOTM"='PM_2',
                                              "Simstrat"='PM_3',
                                              "RW" = "persistence",
                                              "ler" = "LER")))


models1 <-  c('full_ensemble', 'persistence', 'climatology', 'PM_1', 'PM_2', 'PM_3')
models2 <- c('full_ensemble', 'persistence', 'climatology', 'ler')
models3 <- c('LER', 'PM_1', 'PM_2', 'PM_3')


cols <- c('persistence' = "#455BCDFF",
          'climatology' =  "#30B1F4FF",
          'empirical' = "#2BEFA0FF",
          'PM_1' = "#A2FC3CFF", #'#FCA50AFF',
          'PM_2' =  "#F0CC3AFF", #'#E65D30FF',
          'PM_3' = "#F9731DFF", #'#AE305CFF',
          'LER' =  "#C42503FF", #'#6B186EFF',
          'full_ensemble' = 'darkgrey')


strat_dates <- stratification_density(targets = 'https://s3.flare-forecast.org/targets/ler_ms3/fcre/fcre-targets-insitu.csv',
                       density_diff = 0.1)  %>% na.omit()

# read in scored forecasts ###
# Can be read in from local file system or from S3 bucket
local <- FALSE

if (local == TRUE) {
  scores_parquets <- arrow::open_dataset('./scores/site_id=fcre')
} else {
  s3_ler <- arrow::s3_bucket(bucket = "scores/ler_ms2/parquet",
                             endpoint_override =  "s3.flare-forecast.org",
                             anonymous = TRUE)
  
  scores_parquets <- arrow::open_dataset(s3_ler) 
}


# extract a list of model_id from the parquet
distinct_models <- scores_parquets |> 
  distinct(model_id) |> 
  filter(model_id != 'Simstrat_2') |> pull()

# only weekly forecasts
first_date <- as_datetime("2020-10-25 00:00:00")

last_date <- as_datetime("2022-10-23 00:00:00")

forecast_dates <- paste0(seq.Date(as.Date(first_date),as.Date(last_date), 7), ' 00:00:00')



for (i in 1:length(distinct_models)) {
  scores <- scores_parquets |> 
    filter(model_id == distinct_models[i],
           reference_datetime %in% forecast_dates,
           horizon < 15,
           horizon > 0,
           variable == 'temperature') |> 
    collect()|> 
    mutate(model_id = factor(model_id, levels = names(cols))) 
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
# combine forecast scores ###
ensemble_scored <- bind_rows(mget(ensemble_forecasts))
all_scored <- bind_rows(mget(c(individual_forecasts, ensemble_forecasts)))
all_empirical_scored <- bind_rows(mget(c(empirical_forecasts)))
individual_scored <- bind_rows(mget(c(individual_forecasts)))
all_process_scored <- bind_rows(mget(c(process_forecasts)))
#===================================#

# Observations ####
all_scored |> 
  na.omit() |> 
  select(datetime, observation, depth) |> 
  distinct(datetime, observation, depth) |> 
  ggplot() +
  annotate("rect", ymin = -Inf, ymax = Inf, 
                xmin = as_datetime(strat_dates$start[3]),
                xmax = as_datetime(strat_dates$end[3]), alpha = 0.2) +
  annotate("rect", ymin = -Inf, ymax = Inf, 
           xmin = as_datetime(strat_dates$start[4]),
           xmax = as_datetime(strat_dates$end[4]), alpha = 0.2) +
  geom_line(aes(x=datetime, y= observation, colour = as.factor(depth))) +
  theme_bw() +
  scale_y_continuous(name = 'Water temperature (°C)') +
  scale_x_datetime(name = 'Date', date_breaks = '3 months', date_labels = '%b %Y') +
  scale_colour_viridis_d(name = 'Depth (m)', option = "H", begin = 0.9, end = 0.1)

for_interp <- expand.grid(datetime = seq(min(all_scored$datetime),
                                        max(all_scored$datetime), 'day'),
                         depth = seq(min(all_scored$depth), 
                                     max(all_scored$depth),
                                     by = .01))


all_scored |> 
  na.omit() |> 
  select(datetime, observation, depth) |> 
  distinct(datetime, observation, depth) |>
  group_by(datetime) |> 
  mutate(observation = imputeTS::na_interpolation(observation, option = "linear")) |> 
  # full_join(for_interp) |> 
  # ungroup() |> 
  # group_by(datetime) |> 
  # mutate(observation = imputeTS::na_interpolation(observation, option = "linear", )) |>  
  ggplot(aes(x=datetime, y = as.numeric(depth))) +
  geom_tile(aes(fill = observation)) +
  scale_y_reverse(expand = c(0,0)) +
  theme_bw(base_size = 11) +
  scale_x_datetime(expand = c(0,0), 
                   date_breaks = "1 month", 
                   date_labels = "%b") +
  scale_fill_gradientn(name = "Water temperature\n(°C)",
                       limits = c(-1, 30),
                       colours = c("#313695", "#4575b4", "#74add1",
                                   "#abd9e9", "#e0f3f8", "#ffffbf", "#fee090",
                                   "#fdae61", "#f46d43", "#d73027", "#a50026" )) +
  labs(x= "Date", y = "Depth (m)", subtitle = "Modelled") +
  theme(axis.text.x = element_text(hjust = -0.4))

#### Calculate "shadowing time" ####

df_comb <- all_scored %>%
  filter(!is.na(observation)) %>%
  distinct(model_id, reference_datetime, depth) %>%
  mutate(df = paste0(model_id, '_scored')) %>%
  rename(ref_datetime = reference_datetime,
         mod = model_id,
         dep = depth)

# find the shadowing time for each forecast (1 reference_datetime, 1 depth, 1 model_id)
statistical <- purrr::pmap_dfr(df_comb, stat_shadow_length)

statistical %>% 
  # filter(model_id %in% gsub('_scored', '', process_forecasts)) |> 
  # mutate(season = as_season(reference_datetime)) %>%
  group_by(depth, model_id) %>% 
  summarise(sd_st = sd(shadow_time),
            shadow_time=mean(shadow_time)) %>%
  ggplot(.,aes(fill=model_id, 
               y=shadow_time, 
               x= factor(depth, levels = unique(shadowing$depth)))) + 
  geom_col(position = 'dodge')+
  # geom_errorbar(aes(ymin=shadow_time-sd_st, 
  #                   ymax=shadow_time+sd_st, group = as.factor(model_id)), 
  #               width=0.4, alpha=0.5, size=0.5, position = position_dodge(.9)) +
  scale_fill_manual(values = cols, limits = unique(shadowing$model_id)) +
  scale_y_continuous(expand = c(0,0), breaks = c(2,4,6,8,10,12,14)) +
  theme_bw() +
  # coord_flip()+
  labs(x= 'depth (m)') +
  theme(panel.grid.minor  = element_blank())
#===================================#

# Confidence intervals ####
# How good are the confidence intervals?
# 90% of points should fall within a 90% confidence interval.
# How many points are within the confidence intervals for each depth/horizon
all_scored %>%
  filter(horizon > 0, horizon < 15) %>% 
  na.omit() %>% 
  mutate(shadow = ifelse(observation <= quantile90 &
                           observation >= quantile10, 
                         T, F)) %>%
  group_by(model_id, horizon, depth) %>%
  summarise(n = n(),
            inside = length(which(shadow == T)),
            percent = (inside/n) * 100) %>%
  ggplot(., aes(x=horizon, y= percent, colour = model_id)) +
  geom_hline(yintercept = 80)+
  geom_line() +
  facet_wrap(~depth)  +
  scale_colour_manual(values = cols)

all_scored %>%
  filter(horizon > 0, horizon < 15) %>% 
  na.omit() %>% 
  mutate(shadow = ifelse(observation <= quantile97.5 &
                           observation >= quantile02.5, 
                         T, F)) %>%
  group_by(model_id, horizon, depth) %>%
  summarise(n = n(),
            inside = length(which(shadow == T)),
            percent = (inside/n) * 100) %>%
  ggplot(., aes(x=horizon, y= percent, colour = model_id)) +
  geom_hline(yintercept = 95)+
  geom_line(size = 0.8) +
  facet_wrap(~depth)  +
  theme_bw()+
  scale_colour_manual(values = cols)
  

all_scored %>%
  filter(horizon > 0, horizon < 15) %>% 
  na.omit() %>% 
  mutate(shadow = ifelse(observation <= quantile97.5 &
                           observation >= quantile02.5, 
                         T, F)) %>%
  group_by(model_id, horizon, depth) %>%
  summarise(n = n(),
            inside = length(which(shadow == T)),
            percent = (inside/n) * 100) %>%
  filter(depth %in% c(1,8),
         horizon %in% c(1,7,14))
#===================================#

#### d) plot example forecasts ####
ensemble_scored %>%
  filter(variable == 'temperature',
         horizon < 15) %>%
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
         horizon < 15,
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

#### e) plot scores ####
ensemble_scored %>%
  mutate(bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         horizon < 15,
         horizon > 0) %>%
  group_by(horizon, model_id, depth) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= crps, colour = model_id)) +
  geom_line(size = 0.9) +
  facet_wrap(~depth) +
  scale_colour_manual(values = cols, limits = unique(ensemble_scored$model_id)) +
  theme_bw()

ensemble_scored %>%
  mutate(bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         horizon < 15,
         horizon > 0) %>%
  group_by(horizon, model_id, depth) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= logs, colour = model_id)) +
  geom_line(size = 0.9) +
  facet_wrap(~depth) +
  scale_colour_manual(values = cols, limits = unique(ensemble_scored$model_id)) +
  theme_bw()


# comparison of scores at two depths
ensemble_scored %>%
  filter(variable == 'temperature' &
         horizon < 15 &
           depth %in% c(1, 8)) %>%
  mutate(season = as_season(datetime)) %>%
  group_by(horizon, model_id, depth, season) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  select(horizon, model_id, depth, crps, logs, season) %>%
  pivot_longer(values_to = 'value', names_to = 'score', cols = c(crps, logs)) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= value, colour = model_id, linetype = as.factor(depth))) +
  geom_line(size = 0.9) +
  facet_grid(season~score) +
  scale_colour_manual(values = cols, limits = unique(ensemble_scored$model_id)) +
  scale_linetype_manual(values = c('twodash', 'longdash'), name = 'Depth (m)')+
  theme_bw()

all_scored %>%
  mutate(bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         model_id != 'Simstrat_2',
         horizon <15,
         horizon > 0) %>%
  group_by(horizon, model_id, depth) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>% 
  mutate(class = ifelse(model_id %in% gsub('_scored', '', ensemble_forecasts),
                        'ensemble', 'individual')) %>% 
  ggplot(., aes(x=horizon, y= crps, colour = model_id)) +
  geom_line(size = 0.9, aes(linetype = class)) +
  facet_wrap(~depth)+
  scale_colour_manual(values = cols) +
  scale_linetype_manual(values = line_types) +
  theme_bw()

all_scored %>%
  mutate(bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         model_id %in% c('ler', 'climatology', 'RW'),
         horizon < 15,
         horizon > 0) %>%
  group_by(horizon, model_id, depth) %>%
  summarise_if(is.numeric, mean, na.rm = T)  %>% 
  mutate(class = ifelse(model_id %in% gsub('_scored', '', ensemble_forecasts),
                        'ensemble', 'individual')) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= crps, colour = model_id, linetype = class)) +
  geom_line(size = 0.9) +
  facet_wrap(~depth)+
  scale_colour_manual(values = cols, limits = c('ler', 'climatology', 'RW')) +
  scale_linetype_manual(values = line_types) +
  theme_bw()


all_scored %>%
  mutate(bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2,
         season = as_season(datetime)) %>% 
  filter(variable == 'temperature',
         model_id %in% c('ler', 'climatology', 'RW'),
         horizon < 14,
         horizon > 0,
         depth %in% c(1,8)) %>%
  group_by(horizon, model_id, depth, season) %>%
  summarise_if(is.numeric, mean, na.rm = T)  %>% 
  mutate(class = ifelse(model_id %in% gsub('_scored', '', ensemble_forecasts),
                        'ensemble', 'individual')) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= crps, linetype = class, colour = model_id)) +
  geom_line(size = 0.9) +
  facet_grid(season~depth)+
  scale_colour_manual(values = cols, limits = c('ler', 'climatology', 'RW')) +
  theme_bw()


all_scored %>%
  mutate(bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2,
         season = as_season(datetime)) %>% 
  filter(variable == 'temperature',
         model_id %in% c('ler', 'empirical_ler','climatology', 'RW'),
         horizon < 14,
         depth %in% c(1,8)) %>%
  group_by(horizon, model_id, depth, season) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit()  %>% 
  mutate(class = ifelse(model_id %in% gsub('_scored', '', ensemble_forecasts),
                        'ensemble', 'individual')) %>%
  ggplot(., aes(x=horizon, y= crps, colour = model_id, linetype = class)) +
  geom_line(size = 0.9) +
  facet_grid(season~depth)+
  scale_colour_manual(values = cols, limits = c('ler', 'empirical_ler','climatology', 'RW')) +
  scale_linetype_manual(values = line_types) +
  theme_bw()



all_scored %>%
  mutate(bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         # depth == 'fcre-1',
         horizon < 15) %>%
  group_by(horizon, model_id, depth) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>% 
  mutate(class = ifelse(model_id %in% gsub('_scored', '', ensemble_forecasts),
                        'ensemble', 'individual')) %>%
  ggplot(., aes(x=horizon, y= logs, colour = model_id, linetype = class)) +
  geom_line(size = 0.9) +
  facet_wrap(~depth)+
  scale_colour_manual(values = cols, limits = unique(all_scored$model_id)) +
  scale_linetype_manual(values = line_types) +
  theme_bw()


all_empirical_scored %>%
  mutate(bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         horizon < 15) %>%
  group_by(horizon, model_id, depth) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit()  %>% 
  mutate(class = ifelse(model_id %in% gsub('_scored', '', ensemble_forecasts),
                        'ensemble', 'individual')) %>%
  ggplot(., aes(x=horizon, y= crps, colour = model_id, linetype = class)) +
  geom_line(size = 0.9)+
  facet_wrap(~depth) +
  scale_color_manual(values = cols, limits = unique(all_empirical_scored$model_id)) +
  scale_linetype_manual(values = line_types) +
  theme_bw()

all_process_scored %>%
  mutate(bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         horizon <15,
         horizon > 0) %>%
  group_by(horizon, model_id, depth) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= crps, colour = model_id)) +
  geom_line(size = 0.9)+
  facet_wrap(~depth) +
  scale_colour_manual(values = cols, limits = c('GLM', 'GOTM', 'Simstrat', 'ler')) +
  theme_bw()

all_process_scored %>%
  mutate(bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         horizon <15,
         horizon > 0, 
         depth %in% c(1,8)) %>%
  group_by(horizon, model_id, depth) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= crps, colour = model_id)) +
  geom_line(size = 0.9)+
  facet_wrap(~depth) +
  scale_colour_manual(values = cols, limits = c('GLM', 'GOTM', 'Simstrat', 'ler')) +
  theme_bw()


all_empirical_scored %>%
  mutate(bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         horizon < 15, model_id != 'empirical_ler') %>%
  group_by(horizon, model_id, depth) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= crps, colour = model_id)) +
  geom_line(size = 0.9)+
  facet_wrap(~depth) +
  scale_colour_manual(values = cols, limits = levels(all_empirical_scored$model_id)[1:3]) +
  theme_bw()


ensemble_scored %>%
  mutate(bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         # depth == 'fcre-1',
         horizon < 15) %>%
  group_by(horizon, model_id, depth) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= crps, colour = model_id)) +
  geom_line(size = 0.9) +
  facet_wrap(~depth)+
  scale_colour_manual(values = cols, limits = unique(ensemble_scored$model_id)) +
  theme_bw()

individual_scored %>%
  mutate(bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         # depth == 'fcre-1',
         horizon < 15, model_id != 'Simstrat_2') %>%
  group_by(horizon, model_id, depth) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= crps, colour = model_id)) +
  geom_line(size = 0.9) +
  facet_wrap(~depth)+
  scale_colour_manual(values = cols, limits = levels(individual_scored$model_id)[c(1:2,4:6)]) +
  theme_bw()

individual_scored %>%
  mutate(bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         horizon < 15, 
         model_id != 'Simstrat_2') %>%
  group_by(horizon, model_id, depth) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= logs, colour = model_id)) +
  geom_line(size = 0.9) +
  facet_wrap(~depth)+
  scale_colour_manual(values = cols, limits = levels(individual_scored$model_id)[c(1:2,4:6)]) +
  theme_bw()


# seasonal analysis

individual_scored %>%
  filter(variable == 'temperature',
         between(horizon, 0, 15), 
         depth == 1,
         model_id != 'Simstrat_2') %>% 
  mutate(season = as_season(datetime),
         strat = is_strat(datetime, strat_dates),
         year = which_year(datetime = datetime),
         bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>%
  group_by(horizon, model_id, year, strat) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= crps, colour = model_id)) +
  geom_line(size = 0.9) +
  facet_grid(strat~year)+
  scale_colour_manual(values = cols, limits = levels(individual_scored$model_id)[c(1:2,4:6)]) +
  theme_bw() + labs(title = 'surface (1m)')

individual_scored %>%
  filter(variable == 'temperature',
         between(horizon, 0, 15), 
         depth == 8,
         model_id != 'Simstrat_2') %>% 
  mutate(season = as_season(datetime),
         strat = is_strat(datetime, strat_dates),
         year = which_year(datetime = datetime),
         bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>%
  group_by(horizon, model_id, year, strat) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= crps, colour = model_id)) +
  geom_line(size = 0.9) +
  facet_grid(strat~year)+
  scale_colour_manual(values = cols, limits = levels(individual_scored$model_id)[c(1:2,4:6)]) +
  theme_bw() +
  labs(title = 'bottom (8m)')


all_process_scored %>%
  filter(variable == 'temperature',
         between(horizon, 1, 14), 
         depth %in% c(1,8)) %>% 
  mutate(season = as_season(datetime),
         strat = is_strat(datetime, strat_dates),
         year = which_year(datetime = datetime),
         bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>%
  group_by(horizon, model_id, depth, strat) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= crps, colour = model_id)) +
  geom_line(size = 0.9) +
  facet_grid(strat~depth)+
  scale_colour_manual(values = cols, limits = levels(all_process_scored$model_id)[c(4:7)]) +
  scale_x_continuous(breaks = c(1,7,14)) +
  theme_bw()

all_scored %>%
  mutate(season = as_season(datetime),
         bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         horizon < 15, 
         depth %in% c(1,8),
         model_id %in% c('RW', 'climatology', 'empirical', 'ler', 'empirical_ler')) %>%
  group_by(horizon, model_id, depth, season) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= crps, colour = model_id)) +
  geom_line(size = 0.9) +
  facet_grid(season~depth)+
  scale_colour_manual(values = cols, limits = c('RW', 'climatology', 'empirical', 'ler', 'empirical_ler')) +
  theme_bw()

ensemble_scored %>%
  mutate(season = as_season(datetime),
         strat = is_strat(datetime, strat_dates),
         year = which_year(datetime), 
         bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         horizon < 15, horizon > 0, 
         depth %in% c(1,8)) %>%
  group_by(horizon, model_id, depth, strat) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= crps, colour = model_id)) +
  geom_line(size = 0.9) +
  facet_grid(strat~depth)+
  scale_colour_manual(values = cols, limits = unique(ensemble_scored$model_id)) +
  theme_bw()

ensemble_scored %>%
  mutate(season = as_season(datetime),
         strat = is_strat(datetime, strat_dates),
         year = which_year(datetime), 
         bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         horizon < 15, horizon > 0, 
         depth %in% c(1)) %>%
  group_by(horizon, model_id, year, strat) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= crps, colour = model_id)) +
  geom_line(size = 0.9) +
  facet_grid(strat~year)+
  scale_colour_manual(values = cols, limits = unique(ensemble_scored$model_id)) +
  theme_bw() + labs(title = 'surface (1m)')

ensemble_scored %>%
  mutate(season = as_season(datetime),
         strat = is_strat(datetime, strat_dates),
         year = which_year(datetime), 
         bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         horizon < 15, horizon > 0, 
         depth %in% c(8)) %>%
  group_by(horizon, model_id, year, strat) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= crps, colour = model_id)) +
  geom_line(size = 0.9) +
  facet_grid(strat~year)+
  scale_colour_manual(values = cols, limits = unique(ensemble_scored$model_id)) +
  theme_bw() +
  labs(title = 'bottom (8m)')

all_scored %>%
  mutate(season = as_season(datetime),
         strat = is_strat(datetime, strat_dates),
         year = which_year(datetime), 
         bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         horizon < 15, 
         horizon > 0,
         depth == 1, 
         model_id != 'Simstrat_2') %>%
  group_by(horizon, model_id, year, strat) %>%
  summarise_if(is.numeric, mean, na.rm = T) |> 
  na.omit() %>%
  ggplot(., aes(x=horizon, y= crps, colour = model_id)) +
  geom_line(size = 0.9) +
  facet_grid(strat~year, scales = 'free')+
  scale_colour_manual(values = cols, limits = levels(all_scored$model_id)) +
  theme_bw() + labs(title = 'surface (1m)')

all_scored %>%
  mutate(season = as_season(datetime),
         strat = is_strat(datetime, strat_dates),
         year = which_year(datetime), 
         bias = mean - observation,
         absolute_error = abs(bias),
         sq_error = bias^2) %>% 
  filter(variable == 'temperature',
         horizon < 15, 
         horizon > 0,
         depth == 8, 
         model_id != 'Simstrat_2') %>%
  group_by(horizon, model_id, year, strat) %>%
  summarise_if(is.numeric, mean, na.rm = T) |> 
  na.omit() %>%
  ggplot(., aes(x=horizon, y= crps, colour = model_id)) +
  geom_line(size = 0.9) +
  facet_grid(strat~year, scales = 'free')+
  scale_colour_manual(values = cols, limits = levels(all_scored$model_id)) +
  theme_bw() + labs(title = 'bottom (8m)')



