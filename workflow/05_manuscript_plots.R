library(arrow)
library(tidyverse)
library(lubridate)

source('R/shadow_time.R')
source('R/time_functions.R')
source('R/stratified_period.R')


# Working document for the final plots for the paper   #
# ====== ancillary bits and pieces ======#
all_models <-  c('full_ensemble', 'persistence', 'climatology', 'LER', 'PM_1', 'PM_2', 'PM_3')
baselines_ensembles <- c('full_ensemble', 'persistence', 'climatology', 'LER')
process_models <- c('LER', 'PM_1', 'PM_2', 'PM_3')
baseline_models <- c('climatology', 'persistence')


cols <- c('persistence' = "#455BCDFF",
          'climatology' =  "#30B1F4FF",
          'empirical' = "#2BEFA0FF",
          'PM_1' = "#A2FC3CFF", #'#FCA50AFF',
          'PM_2' =  "#F0CC3AFF", #'#E65D30FF',
          'PM_3' = "#F9731DFF", #'#AE305CFF',
          'LER' =  "#C42503FF", #'#6B186EFF',
          'full_ensemble' = 'black')#'darkgrey'

strat_dates <- stratification_density(targets = 'https://s3.flare-forecast.org/targets/ler_ms/fcre/fcre-targets-insitu.csv',
                                      density_diff = 0.1)  %>% na.omit()
#=========================================


#### Get the forecasts/scores =========================
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
    collect()
  assign(paste0(distinct_models[i], '_scored'), scores)
}


#extract list of different types of scored forecasts for comparisons
all_df <- ls(pattern = 'scored')


# combine forecast scores ###
all_scored <- bind_rows(mget(all_df))


#===================================#

# recode the model_id's to the paper names
all_scored <-  all_scored |> 
  mutate(model_id = plyr::revalue(model_id, c("empirical_ler"="full_ensemble",
                                              "GLM"="PM_1",
                                              "GOTM"='PM_2',
                                              "Simstrat"='PM_3',
                                              "RW" = "persistence",
                                              "ler" = "LER")))

### PLOT 2 - observations ####
plot2 <- all_scored |> 
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

ggsave(plot2, filename = file.path('plots', 'plot2.png'), height = 10, width = 18, units = 'cm')
#=============================#


### PLOT 3 - PM fits ====
plot3 <- all_scored %>%
  filter(variable == 'temperature',
         horizon %in%  c(1, 7, 14),
         depth %in% c(1,8),
         model_id %in% process_models) %>%
  na.omit() %>%
  ggplot(., aes(x=datetime, y= mean)) +
  annotate("rect", ymin = -Inf, ymax = Inf, 
           xmin = as_datetime(strat_dates$start[3]),
           xmax = as_datetime(strat_dates$end[3]), alpha = 0.1) +
  annotate("rect", ymin = -Inf, ymax = Inf, 
           xmin = as_datetime(strat_dates$start[4]),
           xmax = as_datetime(strat_dates$end[4]), alpha = 0.1) +
  geom_line(aes(x=datetime, y= observation, colour = as.factor(depth))) +
  facet_grid(depth~horizon, scales = 'free_x') +
  geom_ribbon(aes(ymax = quantile90, ymin = quantile10, colour = model_id), 
              fill = NA, linetype = 'dashed') +
  geom_line(aes(colour = model_id), linewidth = 0.8) +
  geom_point(aes(y = observation), alpha = 0.5, size = 0.8) +
  scale_colour_manual(values = cols, limits = process_models, name = 'Forecast') +
  labs(y = 'Water temperature (°C)') + 
  scale_y_continuous(sec.axis = sec_axis(~ . , name = "Depth (m)", breaks = NULL, labels = NULL)) +
  scale_x_datetime(date_labels = '%d %b %y', date_breaks = "6 months", name = 'Date',
                   sec.axis = sec_axis(~ . , name = "Horizon (days)", breaks = NULL, labels = NULL))  +
  theme_bw() + 
  theme(legend.position = 'top',
        axis.text.x = element_text(angle = 45, vjust = 1, hjust = 1), 
        axis.title.y.right = element_text(vjust = 1.2),
        axis.title.x.top = element_text(vjust = 1.2))

ggsave(plot3, filename = file.path('plots', 'plot3.png'), height = 12, width = 18, units = 'cm')
#=======================================#


### PLOT 4 - LER v process model IGN ####
# LER vs process models, split by stratification
plot4 <- all_scored %>%
  filter(variable == 'temperature',
         between(horizon, 1, 14), 
         depth %in% c(1,8), model_id %in% process_models) %>% 
  mutate(strat = is_strat(datetime, strat_dates)) %>%
  group_by(horizon, model_id, depth, strat) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= logs, colour = model_id)) +
  geom_line(linewidth = 0.9) +
  facet_grid(depth~strat, labeller = labeller(.rows = label_both, .cols = label_value)) +
  scale_colour_manual(values = cols, limits = process_models) +
  scale_x_continuous(breaks = c(1,7,14)) +
  labs(y = 'Ignorance score') +
  theme_bw()

ggsave(plot4, filename = file.path('plots', 'plot4.png'), height = 12, width = 15, units = 'cm')

####================================#


### PLOT 5 - MONEY PLOT ======

# bias
bias_plot <- all_scored %>%
  filter(variable == 'temperature',
         between(horizon, 1, 14), 
         model_id %in% all_models,
         depth %in% c(1,8)) %>% 
  mutate(bias = mean - observation) %>% 
  group_by(horizon, model_id, depth) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= bias, colour = model_id)) +
  geom_line(linewidth = 0.9) +
  facet_wrap(~depth, ncol = 1, labeller = label_both)+
  scale_colour_manual(values = cols, limits = all_models) +
  scale_x_continuous(breaks = c(1,7,14)) +
  labs(y = 'Bias (°C)',
       x = 'Forecast horizon (days)') +
  theme_bw() +
  guides(colour = guide_legend(nrow = 3))


# variance
variance_plot <- all_scored %>%
  filter(variable == 'temperature',
         between(horizon, 1, 14), 
         model_id %in% all_models,
         depth %in% c(1,8)) %>% 
  mutate(var = sd^2) %>% 
  group_by(horizon, model_id, depth) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= var, colour = model_id)) +
  geom_line(linewidth = 0.9) +
  facet_wrap(~depth, ncol = 1, labeller = label_both)+
  scale_colour_manual(values = cols, limits = all_models) +
  scale_x_continuous(breaks = c(1,7,14)) +
  labs(y= expression(paste("Variance (°", C^2, ")")), 
       x = 'Forecast horizon (days)') +
  theme_bw()+
  guides(colour = guide_legend(nrow = 3))

# log score
logs_plot <- all_scored %>%
  filter(variable == 'temperature',
         between(horizon, 1, 14), 
         model_id %in%  all_models,
         depth %in% c(1,8)) %>% 
  group_by(horizon, model_id, depth) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= logs, colour = model_id)) +
  geom_line(linewidth = 0.9) +
  facet_wrap(~depth, ncol = 1, labeller = label_both)+
  scale_colour_manual(values = cols, limits = all_models) +
  scale_x_continuous(breaks = c(1,7,14)) +
  labs(y = 'Ignorance score', x = 'Forecast horizon (days)') +
  theme_bw()+
  guides(colour = guide_legend(nrow = 3))


plot5 <- ggpubr::ggarrange(bias_plot, variance_plot, logs_plot, 
                           ncol  = 3, common.legend = T, align = "hv") 

ggsave(plot5, filename = file.path('plots', 'plot5.png'), height = 10, width = 15, units = 'cm')

#===============================================#



### PLOT 6 - Shadowing time plot ======
shadow_summary <- read_csv('shadow_summary.csv') |> 
  mutate(model_id = plyr::revalue(model_id, c("empirical_ler"="full_ensemble",
                                              "GLM"="PM_1",
                                              "GOTM"='PM_2',
                                              "Simstrat"='PM_3',
                                              "RW" = "persistence",
                                              "ler" = "LER"))) |> 
  filter(model_id != 'empirical')

plot6 <- shadow_summary |>
  group_by(depth, model_id) |> 
  summarise(mean_shadow = mean(shadow_length, na.rm = T)) |> 
  ggplot(aes(x=mean_shadow, y=depth)) +
  geom_point(aes(colour = model_id)) +
  scale_colour_manual(values = cols, limits = all_models)  +
  scale_y_reverse(breaks = c(0,2,4,6,8)) +
  scale_x_continuous(breaks = c(2,4,6,8,10,12,14)) +
  theme_bw() +
  labs(x= 'Mean shadowing length (days)', y = 'Depth (m)')
ggsave(plot6, filename = file.path('plots', 'plot6.png'), height = 6, width = 10, units = 'cm')

shadow_summary |>
  group_by(model_id) |> 
  summarise(mean_shadow = mean(shadow_length, na.rm = T)) 
  
#=====================================#
