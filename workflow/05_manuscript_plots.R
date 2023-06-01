library(arrow)
library(tidyverse)
library(lubridate)
library(ggh4x)

source('R/shadow_time.R')
source('R/time_functions.R')
source('R/stratified_period.R')


# Working document for the final plots for the paper   #
# ====== ancillary bits and pieces ======#
all_models <-  c('Full_Ensemble', 'PM_Ensemble', '  ',
                 'persistence', 'climatology', ' ', 
                 'PM_1', 'PM_2', 'PM_3')

baselines_ensembles <- c('Full_Ensemble', 'persistence', 'climatology', 'PM_Ensemble')
process_models <- c('PM_Ensemble', 'PM_1', 'PM_2', 'PM_3')
baseline_models <- c('climatology', 'persistence')
individual_models <-  c('persistence', 'climatology', 'PM_1', 'PM_2', 'PM_3')
ensemble_models <- c('Full_Ensemble', 'PM_Ensemble')

time_periods <- c(stratified_period = "Stratified", 
                  mixed_period = "Mixed")

cols <- c('persistence' = "#455BCDFF",
          'climatology' =  "#30B1F4FF",
          ' ' = 'white',
          '  ' = 'white',
          # 'empirical' = "#2BEFA0FF",
          'PM_1' = "#A2FC3CFF", #'#FCA50AFF',
          'PM_2' =  "#F0CC3AFF", #'#E65D30FF',
          'PM_3' = "#F9731DFF", #'#AE305CFF',
          'PM_Ensemble' =  "#C42503FF", #'#6B186EFF',
          'Full_Ensemble' = 'black')#'darkgrey'

linetypes <- c('climatology' = 'dotdash', 
               'persistence' = 'dotdash',
               ' ' = 'solid',
               ' ' = 'solid',
               'PM_1' = 'dashed',
               'PM_2' = 'dashed', 
               'PM_3' = 'dashed', 
               'PM_Ensemble' =  "solid", 
               'Full_Ensemble' = 'solid')

shapes <- c('climatology' = 15, 
               'persistence' = 15,
               'PM_1' = 16,
               'PM_2' = 16, 
               ' ' = 16,
               ' ' = 16,
               'PM_3' = 16, 
               'PM_Ensemble' =  17, 
               'Full_Ensemble' = 17)

strat_dates <- calc_strat_dates(targets = 'https://s3.flare-forecast.org/targets/ler_ms3/fcre/fcre-targets-insitu.csv',
                                      density_diff = 0.1)  %>% na.omit() 
inverse_freq <- calc_strat_freq(targets = 'https://s3.flare-forecast.org/targets/ler_ms3/fcre/fcre-targets-insitu.csv',
                                        density_diff = 0.1, inverse = T)  %>% na.omit()
strat_freq <- calc_strat_freq(targets = 'https://s3.flare-forecast.org/targets/ler_ms3/fcre/fcre-targets-insitu.csv',
                              density_diff = 0.1, inverse = F)  %>% na.omit()

#=========================================


#### Get the forecasts/scores =========================
# Can be read in from local file system or from S3 bucket
local <- FALSE

if (local == TRUE) {
  scores_parquets <- arrow::open_dataset('./scores/reruns/site_id=fcre')
} else {
  s3_ler <- arrow::s3_bucket(bucket = "scores/ler_ms3/parquet",
                             endpoint_override =  "s3.flare-forecast.org",
                             anonymous = TRUE)
  
  scores_parquets <- arrow::open_dataset(s3_ler) 
}


# extract a list of model_id from the parquet
distinct_models <- scores_parquets |> 
  distinct(model_id) |> 
  filter(model_id != 'Simstrat_2') |> pull()

# only weekly forecasts
first_date <- scores_parquets %>%
  distinct(reference_datetime) %>%
  summarise(min(reference_datetime)) %>%
  pull()

last_date <- scores_parquets %>%
  distinct(reference_datetime) %>%
  summarise(max(reference_datetime)) %>%
  pull()
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

# recode the model_id's to the paper names
all_scored <-  all_scored |> 
  mutate(model_id = plyr::revalue(model_id, c("empirical_ler"="Full_Ensemble",
                                              "GLM"="PM_1",
                                              "GOTM"='PM_2',
                                              "Simstrat"='PM_3',
                                              "RW" = "persistence",
                                              "ler" = "PM_Ensemble")))
out_dir <- 'plots/reruns'



shadow_summary <- read_csv('shadow_summary.csv') |> 
  mutate(model_id = plyr::revalue(model_id, c("empirical_ler"="Full_Ensemble",
                                              "GLM"="PM_1",
                                              "GOTM"='PM_2',
                                              "Simstrat"='PM_3',
                                              "RW" = "persistence",
                                              "ler" = "PM_Ensemble"))) |> 
  filter(model_id != 'empirical')

#=======================================#

### PLOT 2 - observations ####
plot2 <-
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
  scale_y_continuous(name = 'Water temperature (°C)', breaks = seq(0,30,5)) +
  scale_x_datetime(name = 'Date', date_breaks = '3 months', date_labels = '%d %b %y') +
  scale_colour_viridis_d(name = 'Depth (m)', option = "H", begin = 0.9, end = 0.1) +
  theme(panel.grid.minor = element_blank())

ggsave(plot2, filename = file.path(out_dir, 'plot2.png'), height = 10, width = 18, units = 'cm')

#=============================#

# === PLOT 3 - example forecasts ====
layout_design <- "
  ABC
  DE#
  FG#
"
forecast_date_plots <- c('2023-02-20 00:00:00',  '2022-08-01 00:00:00')
example_levels <- c('PM_1', 'PM_2', 'PM_3',
                    'persistence', 'climatology', 
                    'PM_Ensemble','Full_Ensemble')

forecast_example1 <- all_scored |>
  filter(reference_datetime == forecast_date_plots[1], (depth == 1),
         model_id != 'empirical') |> 
  ggplot(aes(x=horizon, y = median)) + geom_line(aes( colour = model_id),linewidth = 1) +
  geom_ribbon(aes(ymax = quantile97.5, ymin = quantile02.5, fill = model_id), alpha = 0.1) +
  theme_bw() + 
  facet_manual(~factor(model_id, levels = example_levels),
               design = layout_design) +
  geom_point(aes(y=observation)) +
  scale_y_continuous(limits = c(0,16)) +
  scale_x_continuous(breaks = c(0,2,4,6,8,10,12,14)) +
  labs(title = paste0('Forecast start: ', forecast_date_plots[1], ' & Depth: 1 m'),
       y = 'Water temperature °C') +
  scale_colour_manual(values = cols, limits = all_models, name = 'Model')  +
  scale_fill_manual(values = cols, limits = all_models, name = 'Model') +
  theme(legend.position = c(0.8,0.25))


forecast_example2 <- all_scored |>
  filter(reference_datetime == forecast_date_plots[2], (depth == 8),
         model_id != 'empirical') |> 
  ggplot(aes(x=horizon, y = median)) + geom_line(aes( colour = model_id),linewidth = 1) +
  geom_ribbon(aes(ymax = quantile97.5, ymin = quantile02.5, fill = model_id), alpha = 0.1) +
  theme_bw() + 
  facet_manual(~factor(model_id, levels = example_levels),
               design = layout_design) +
  geom_point(aes(y=observation)) +
  scale_y_continuous(limits = c(5,15), breaks = seq(6,14,2)) +
  scale_x_continuous(breaks = c(0,2,4,6,8,10,12,14)) +
  labs(title = paste0('Forecast start: ', forecast_date_plots[2], ' & Depth: 8 m'),
       y = 'Water temperature °C') +
  scale_colour_manual(values = cols, limits = all_models, name = 'Model')  +
  scale_fill_manual(values = cols, limits = all_models, name = 'Model') +
  theme(legend.position = c(0.8,0.25))


plot_3 <- ggpubr::ggarrange(forecast_example1, forecast_example2, 
                             nrow = 2, 
                             common.legend = T, legend = 'none') +
  theme(panel.grid.minor = element_blank())
plot_3a <- ggpubr::ggarrange(forecast_example1, forecast_example2, 
                             nrow = 1, 
                             common.legend = T, legend = 'none')  +
  theme(panel.grid.minor = element_blank())

ggsave(plot_3, filename = file.path(out_dir, 'plot3.png'), height = 20, width = 14, units = 'cm')
ggsave(plot_3a, filename = file.path(out_dir, 'plot3a.png'), height = 12, width = 25, units = 'cm')


#===============================#

#### PLOT 4 - summary plot ####
# bias
absbias_plot <- 
  all_scored %>%
  filter(variable == 'temperature',
         between(horizon, 1, 14), 
         model_id %in% all_models) %>% 
  mutate(abs_bias = abs(mean - observation)) %>% 
  group_by(horizon, model_id) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= abs_bias, colour = model_id, linetype = model_id)) +
  geom_line(linewidth = 0.9) +
  scale_colour_manual(values = cols, limits = all_models, name = 'Model') +
  scale_linetype_manual(values = linetypes, limits = all_models, name = 'Model') +
  scale_x_continuous(breaks = c(1,7,14)) +
  labs(y = 'Absolute Bias (°C)',
       x = 'Horizon (days)') +
  theme_bw() +
  guides(colour =guide_legend(nrow = 3, title.position = 'top', title.hjust = 0.5))


# variance
sd_plot <- all_scored %>%
  filter(variable == 'temperature',
         between(horizon, 1, 14), 
         model_id %in% all_models) %>% 
  group_by(horizon, model_id) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= sd, colour = model_id, linetype = model_id)) +
  geom_line(linewidth = 0.9) +
  scale_colour_manual(values = cols, limits = all_models, name = 'Model') +
  scale_linetype_manual(values = linetypes, limits = all_models, name = 'Model') +
  scale_x_continuous(breaks = c(1,7,14)) +
  labs(y= expression(paste("Standard deviation (°C)")), 
       x = 'Horizon (days)') +
  theme_bw()+
  guides(colour = guide_legend(nrow = 3, title.position = 'top', title.hjust = 0.5)) 

# log score
logs_plot <- all_scored %>%
  filter(variable == 'temperature',
         between(horizon, 1, 14), 
         model_id %in%  all_models) %>% 
  group_by(horizon, model_id) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= logs, colour = model_id, linetype = model_id)) +
  geom_line(linewidth = 0.9) +
  scale_colour_manual(values = cols, limits = all_models, name = 'Model') +
  scale_linetype_manual(values = linetypes, limits = all_models, name = 'Model') +
  scale_x_continuous(breaks = c(1,7,14)) +
  labs(y = 'Ignorance score', x = 'Horizon (days)') +
  theme_bw()+
  guides(colour = guide_legend(nrow = 3, title.position = 'top', title.hjust = 0.5))


plot4 <- ggpubr::ggarrange(absbias_plot, sd_plot, logs_plot, 
                           ncol  = 3, common.legend = T, align = "hv", labels = 'auto') 

ggsave(plot4, filename = file.path(out_dir, 'plot4.png'), height = 10, width = 21, units = 'cm')

### PLOT 5 - MONEY PLOT ======
facet_tags <- data.frame(depth = c(1,8),
                         metric = c('bias', 'bias',
                                    'sd', 'sd',
                                    'ign', 'ign'),
                         tag = c('a', 'b',
                                 'c', 'd', 
                                 'e', 'f'))
# bias
bias_plot <- 
  all_scored %>%
  filter(variable == 'temperature',
         between(horizon, 1, 14), 
         model_id %in% all_models,
         depth %in% c(1,8)) %>% 
  mutate(bias = mean - observation) %>% 
  group_by(horizon, model_id, depth) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot(.) +
  geom_line(aes(x=horizon, y= bias, colour = model_id, linetype = model_id), linewidth = 0.9) +
  facet_wrap(~depth, ncol = 1, labeller = label_both)+
  scale_colour_manual(values = cols, limits = all_models, name = 'Model') +
  scale_linetype_manual(values = linetypes, limits = all_models, name = 'Model') +
  scale_x_continuous(breaks = c(1,7,14)) +
  labs(y = 'Bias (°C)',
       x = 'Horizon (days)') +
  theme_bw() +
  geom_text(data = subset(facet_tags, 
                          metric == 'bias'),
            mapping = aes(x = -1, y = 2.5, label = tag), 
            size = 4, fontface = 'bold') +
  guides(colour =guide_legend(nrow = 3, title.position = 'top', title.hjust = 0.5))  + 
  coord_cartesian(xlim = c(1, 14), ylim = c(-1.8, 1.8),clip = "off")


# variance
sd_plot <-
  all_scored %>%
  filter(variable == 'temperature',
         between(horizon, 1, 14), 
         model_id %in% all_models,
         depth %in% c(1,8)) %>% 
  group_by(horizon, model_id, depth) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot() +
  geom_line(aes(x=horizon, y= sd, colour = model_id, linetype = model_id), linewidth = 0.9) +
  facet_wrap(~depth, ncol = 1, labeller = label_both)+
  scale_colour_manual(values = cols, limits = all_models, name = 'Model') +
  scale_linetype_manual(values = linetypes, limits = all_models, name = 'Model') +
  scale_x_continuous(breaks = c(1,7,14)) +
  labs(y = 'Standard deviation (°C)',
       x = 'Horizon (days)') +
  theme_bw() +
  geom_text(data = subset(facet_tags, 
                          metric == 'sd'),
            mapping = aes(x = -1, y = 4.3, label = tag), 
            size = 4, fontface = 'bold') +
  guides(colour = guide_legend(nrow = 3, title.position = 'top', title.hjust = 0.5)) + 
  coord_cartesian(xlim = c(1, 14), ylim = c(-0.5, 3.5), clip = "off")

# log score
logs_plot <- 
  all_scored %>%
  filter(variable == 'temperature',
         between(horizon, 1, 14), 
         model_id %in%  all_models,
         depth %in% c(1,8)) %>% 
  group_by(horizon, model_id, depth) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot() +
  geom_line(aes(x=horizon, y= logs, colour = model_id, linetype = model_id), linewidth = 0.9) +
  facet_wrap(~depth, ncol = 1, labeller = label_both)+
  scale_colour_manual(values = cols, limits = all_models, name = 'Model') +
  scale_linetype_manual(values = linetypes, limits = all_models, name = 'Model') +
  scale_x_continuous(breaks = c(1,7,14)) +
  labs(y = 'Ignorance score', x = 'Horizon (days)') +
  theme_bw()+
  geom_text(data = subset(facet_tags, 
                          metric == 'ign'),
            mapping = aes(x = -1, y = 4.3, label = tag), 
            size = 4, fontface = 'bold') +
  guides(colour = guide_legend(nrow = 3, title.position = 'top', title.hjust = 0.5)) + 
  coord_cartesian(xlim = c(1, 14), ylim = c(-0.5, 3.5), clip = "off")


plot5 <- ggpubr::ggarrange(bias_plot, sd_plot, logs_plot, 
                           ncol  = 3, common.legend = T, align = "hv") 

ggsave(plot5, filename = file.path(out_dir, 'plot5.png'), height = 10, width = 15, units = 'cm')

#===============================================#


### PLOT 6 - LER v process model IGN ####
facet_tags <- data.frame(depth = c(1,8),
                         strat = c('stratified_period', 'mixed_period',
                                   'mixed_period', 'stratified_period'),
                         tag = c('c)', 'b)',
                                 'a)', 'd)'))
# LER vs process models, split by stratification
plot6 <-
  all_scored %>%
  filter(variable == 'temperature',
         between(horizon, 1, 14), 
         depth %in% c(1,8), model_id %in% all_models) %>% 
  mutate(strat = is_strat(datetime, strat_dates)) %>%
  group_by(horizon, model_id, depth, strat) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() %>%
  ggplot() +
  geom_line(aes(x=horizon, y= logs, colour = model_id, linetype = model_id), linewidth = 0.9) +
  facet_grid(depth~strat, labeller = labeller(.rows = label_both, .cols = time_periods)) +
  scale_colour_manual(values = cols, 
                      limits = all_models,
                      name = 'Model') +
  scale_linetype_manual(values = linetypes, 
                        limits = all_models,
                        name = 'Model') +
  scale_x_continuous(breaks = c(1,7,14)) +
  labs(y = 'Ignorance score', x = 'Horizon') +
  theme_bw() +
  geom_text(data = facet_tags,
            mapping = aes(x = 1.5, y = 7.5, label = tag), 
            size = 4, fontface = 'bold')

ggsave(plot6, filename = file.path(out_dir, 'plot6.png'), height = 12, width = 16, units = 'cm')

`####================================#


#===============================================#



### PLOT 7 - Shadowing time plot ======
plot7 <- 
  shadow_summary |>
  mutate(strat = is_strat(reference_datetime, strat_dates)) %>%
  group_by(depth, model_id, strat) |> 
  summarise(mean_shadow = mean(shadow_length, na.rm = T)) |> 
  ggplot(aes(x=mean_shadow, y=depth)) +
  geom_point(aes(colour = model_id, shape = model_id)) +
  scale_colour_manual(values = cols, limits = all_models,
                      name = 'Model')  +
  scale_shape_manual(values = shapes, limits = all_models,
                     name = 'Model')  +
  scale_y_reverse(breaks = c(0,2,4,6,8)) +
  scale_x_continuous(breaks = seq(0,14,2), 
                     expand = c(0,0), limits = c(0,14)) +
  theme_bw() + 
  facet_wrap(~strat, labeller = labeller(.rows = time_periods)) +
  labs(x= 'Mean shadowing length (days)', y = 'Depth (m)') +
  theme(panel.spacing = unit(1, "lines"))

ggsave(plot7, filename = file.path(out_dir, 'plot7.png'), height = 7, width = 15, units = 'cm')


  
#=====================================#

### Table 1 ====
# aggregated scores
all_scored %>%
  filter(variable == 'temperature',
         between(horizon, 1, 14), 
         model_id %in% all_models) %>% 
  mutate(abs_bias = abs(mean - observation)) %>% 
  group_by(model_id) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() |> 
  select(model_id, abs_bias, sd, logs)


# summary shadow time
shadow_summary |>
  group_by(model_id) |> 
  summarise(mean_shadow = mean(shadow_length, na.rm = T)) 


### Supplementary information.... ======

# Table S1 of scores for 1 and 8 m

all_scored %>%
  filter(variable == 'temperature',
         between(horizon, 1, 14), 
         model_id %in% all_models, 
         depth %in% c(1,8)) %>% 
  mutate(abs_bias = abs(mean - observation)) %>% 
  group_by(model_id, depth) %>%
  summarise_if(is.numeric, mean, na.rm = T) %>%
  na.omit() |> 
  select(model_id, abs_bias, sd, logs, depth) |> 
  pivot_wider(names_from = depth, values_from = abs_bias:logs)


## Median strat-depth scores ####
all_scored %>%
  filter(variable == 'temperature',
         between(horizon, 1, 14), 
         depth %in% c(1,8), model_id %in% all_models) %>% 
  mutate(strat = is_strat(datetime, strat_dates)) %>%
  group_by(horizon, model_id, depth, strat) %>%
  summarise_if(is.numeric, median, na.rm = T) %>%
  na.omit() %>%
  ggplot(., aes(x=horizon, y= logs, colour = model_id, linetype = model_id)) +
  geom_line(linewidth = 0.9) +
  facet_grid(depth~strat, labeller = labeller(.rows = label_both, .cols = time_periods)) +
  scale_colour_manual(values = cols, 
                      limits = all_models,
                      name = 'Model') +
  scale_linetype_manual(values = linetypes, 
                        limits = all_models,
                        name = 'Model') +
  scale_x_continuous(breaks = c(1,7,14)) +
  labs(y = 'Ignorance score', x = 'Horizon') +
  theme_bw()

### PM fits #### 
SI_fig <- all_scored %>%
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

#=======================================#