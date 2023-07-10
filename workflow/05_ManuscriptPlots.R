library(arrow)
library(tidyverse)
library(lubridate)
library(ggh4x)

source('R/shadow_time.R')
source('R/stratified_period.R')
source('R/time_functions.R')

# Working document for the final plots for the paper   #
# ====== ancillary bits and pieces ======#
all_models <-  c('Full MME', 'PM MME', '  ',
                 'persistence', 'climatology', '   ', 
                 'PM1', 'PM2', 'PM3')

baselines_ensembles <- c('Full MME', 'persistence', 'climatology', 'PM MME')
process_models <- c('PM MME', 'PM1', 'PM2', 'PM3')
baseline_models <- c('climatology', 'persistence')
individual_models <-  c('persistence', 'climatology', 'PM1', 'PM2', 'PM3')
MME_models <- c('Full MME', 'PM MME')


time_periods <- c(stratified_period = "Stratified", 
                  mixed_period = "Mixed")


cols <- c('persistence' = "#455BCDFF",
          'climatology' =  "#30B1F4FF",
          '  ' = 'white',
          '   ' = 'white',
          'PM1' = "#A2FC3CFF", #'#FCA50AFF',
          'PM2' =  "#F0CC3AFF", #'#E65D30FF',
          'PM3' = "#F9731DFF", #'#AE305CFF',
          'PM MME' =  "#C42503FF", #'#6B186EFF',
          'Full MME' = 'black')#'darkgrey'

linetypes <- c('climatology' = 'dotdash', 
               'persistence' = 'dotdash',
               '  ' = 'solid',
               '   ' = 'solid',
               'PM1' = 'dashed',
               'PM2' = 'dashed', 
               'PM3' = 'dashed', 
               'PM MME' =  "solid", 
               'Full MME' = 'solid')

shapes <- c('climatology' = 15, 
            'persistence' = 15,
            'PM1' = 16,
            'PM2' = 16, 
            '  ' = 16,
            '   ' = 16,
            'PM3' = 16, 
            'PM MME' =  17, 
            'Full MME' = 17)

strat_dates <- calc_strat_dates(targets = 'https://s3.flare-forecast.org/targets/ler_ms3/fcre/fcre-targets-insitu.csv',
                                density_diff = 0.1)  %>% na.omit() 

mean_thermo <- calc_thermo(start = strat_dates$start[3], end = strat_dates$end[3], 
                           targets =  'https://s3.flare-forecast.org/targets/ler_ms3/fcre/fcre-targets-insitu.csv') |> 
  full_join(calc_thermo(start = strat_dates$start[4], end = strat_dates$end[4], 
                        targets =  'https://s3.flare-forecast.org/targets/ler_ms3/fcre/fcre-targets-insitu.csv'))

#=========================================#
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


# vector of model_ids 
distinct_models <- c("GLM",
                     "GOTM",
                     "Simstrat",
                     "RW", 
                     "empirical_ler",
                     "ler",
                     "climatology")

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
  mutate(model_id = plyr::revalue(model_id, c("empirical_ler"="Full MME",
                                              "GLM"="PM1",
                                              "GOTM"='PM2',
                                              "Simstrat"='PM3',
                                              "RW" = "persistence",
                                              "ler" = "PM MME")))
out_dir <- 'plots/reruns'



shadow_summary <- read_csv('shadow_summary.csv') |> 
  mutate(model_id = plyr::revalue(model_id, c("empirical_ler"="Full MME",
                                              "GLM"="PM1",
                                              "GOTM"='PM2',
                                              "Simstrat"='PM3',
                                              "RW" = "persistence",
                                              "ler" = "PM MME")))

#=======================================#
# Main text ---------------------------------------------------------------
##  PLOT 2 - observations =====
plot_2 <-
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

ggsave(plot_2, filename = file.path(out_dir, 'plot_2.png'), height = 10, width = 18, units = 'cm')

#=============================================#

##  PLOT 3 - example forecasts ====
layout_design <- "
  ABC
  DE#
  FG#
"
forecast_date_plots <- c('2023-02-20 00:00:00',  '2022-08-01 00:00:00')
example_levels <- c('PM1', 'PM2', 'PM3',
                    'persistence', 'climatology', 
                    'PM MME','Full MME')

forecast_example1 <- all_scored |>
  filter(reference_datetime == forecast_date_plots[1], (depth == 1)) |> 
  ggplot(aes(x=horizon, y = median)) + 
  geom_point(aes(y=observation)) +
  geom_line(aes(colour = model_id),linewidth = 1) +
  geom_ribbon(aes(ymax = quantile97.5, ymin = quantile02.5, fill = model_id), alpha = 0.1) +
  theme_bw() + 
  facet_manual(~factor(model_id, levels = example_levels),
               design = layout_design) +
  scale_y_continuous(limits = c(0,16)) +
  scale_x_continuous(breaks = c(0,2,4,6,8,10,12,14)) +
  labs(title = paste0('Forecast: ', forecast_date_plots[1], ' at 1 m'),
       y = 'Water temperature (°C)',
       x = 'Horizon (days)') +
  scale_colour_manual(values = cols, limits = all_models, name = 'Model')  +
  scale_fill_manual(values = cols, limits = all_models, name = 'Model') +
  theme(panel.grid.minor = element_blank())


forecast_example2 <- all_scored |>
  filter(reference_datetime == forecast_date_plots[2], 
         depth == 8) |> 
  ggplot(aes(x=horizon, y = median)) +
  geom_point(aes(y=observation)) +
  geom_line(aes( colour = model_id),linewidth = 1) +
  geom_ribbon(aes(ymax = quantile97.5, ymin = quantile02.5, fill = model_id), alpha = 0.1) +
  theme_bw() + 
  facet_manual(~factor(model_id, levels = example_levels),
               design = layout_design) +
  scale_y_continuous(breaks = seq(6,14,2)) +
  scale_x_continuous(breaks = c(0,2,4,6,8,10,12,14)) +
  labs(title = paste0('Forecast: ', forecast_date_plots[2], ' at 8 m'),
       y = 'Water temperature (°C)',
       x = 'Horizon (days)') +
  scale_colour_manual(values = cols, limits = all_models, name = 'Model')  +
  scale_fill_manual(values = cols, limits = all_models, name = 'Model')+
  theme(panel.grid.minor = element_blank())


plot_3 <- ggpubr::ggarrange(forecast_example1, forecast_example2, 
                            nrow = 1, 
                            common.legend = T, legend = 'none', labels = c('a)', 'b)')) +
  theme(panel.grid.minor = element_blank())

ggsave(plot_3, filename = file.path(out_dir, 'plot_3.png'), height = 10, width = 20, units = 'cm')

#=====================================#

##  PLOT 4 - aggregated metrics ====
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
  labs(y = 'Absolute bias (°C)',
       x = 'Horizon (days)', subtitle = 'a)') +
  theme_bw() +
  guides(colour =guide_legend(nrow = 3, title.position = 'top', title.hjust = 0.5)) +
  theme(plot.subtitle = element_text(size=  12, hjust = -0.12, vjust = -5, face = 'bold'))


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
  labs(y= expression(paste('Standard deviation (°C)')), 
       x = 'Horizon (days)', subtitle = 'b)') +
  theme_bw() +
  theme(plot.subtitle = element_text(size=  12, hjust = -0.12, vjust = -5, face = 'bold'))

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
  labs(y = 'Ignorance score', x = 'Horizon (days)', subtitle = 'c)') +
  theme_bw()+
  guides(colour = guide_legend(nrow = 3, title.position = 'top', title.hjust = 0.5)) +
  theme(plot.subtitle = element_text(size=  12, hjust = -0.12, vjust = -5, face = 'bold'))



plot_4 <- ggpubr::ggarrange(absbias_plot, sd_plot, logs_plot, 
                            ncol  = 3, common.legend = T, 
                            align = "hv") 

ggsave(plot_4, filename = file.path(out_dir, 'plot_4.png'), height = 8, width = 17, units = 'cm')


#==========================================#

##  PLOT 5 - model cross correlation ====

all_list <- all_scored |>
  mutate(bias = mean - observation,
         change_bias = bias - lag(bias)) %>%
  select(reference_datetime, model_id, horizon, depth, bias) |> 
  filter(horizon %in% c(1,7,14),
         depth %in% c(1),
         model_id %in% individual_models) |> 
  pivot_wider(names_from = model_id, values_from = bias) |> 
  na.omit() |> 
  ungroup() |> 
  # split into a list of dataframes for each horizon/depth combination
  split(horizon~depth)

# function to generate a cross-correlation matrix
ccf_matrix <- function(x, lag_max = 0) {
  
  vars <- colnames(x)
  mod_comb <- expand.grid(1:length(vars), 1:length(vars))
  n <- length(vars)
  
  r <- matrix(0, nrow = n, ncol = n)
  rownames(r) <- vars
  colnames(r) <- vars
  
  for (i in 1:nrow(mod_comb)) {
    
    r[i] <- ccf(x[,rownames(r)[mod_comb$Var1[i]]], 
                x[,rownames(r)[mod_comb$Var2[i]]], 
                lag.max = 0, plot = F, )$acf[1]
    
    
    
  }
  return(r)
}


# calculate the ccf for among model_id for all horizons
ccf_all <- lapply(all_list, function(x) ccf_matrix(x[,4:8]))

library(rstatix) # for cor_gather

facet_tags <- expand.grid(horizon = c(1,7,14),
                          depth = 1,
                          var1 = 'a',
                          var2 = 'z') |> 
  mutate(tag = c('a)', 'b)', 'c)'))

h1_ccf <-
  lapply(ccf_all, cor_gather) |> 
  bind_rows(.id = 'group') |> 
  separate(group, into = c('horizon', 'depth')) |> 
  arrange(var1, var2) |> 
  group_by(horizon, depth) |> 
  #this makes sure we only have the upper left quadrant (not sure how it works tbh)
  distinct(smaller = pmin(var1, var2),
           larger = pmax(var1, var2),
           .keep_all = TRUE) %>%
  select(-smaller, -larger) |>
  mutate(horizon = as.numeric(horizon)) |>
  filter(horizon == 1) |> 
  ggplot(aes(x=var1, y=var2)) +
  geom_tile(aes(fill = cor)) +
  geom_text(aes(label = round(cor, 2)), size = 3) +
  facet_wrap(~horizon, labeller = label_both) +
  labs(x='', y='')+
  colorspace::scale_fill_continuous_diverging(limits = c(-1,1), palette = 'Blue-Red 2',
                                              name = 'Cross correlation\ncoefficient') +
  theme_bw(base_size = 12) +
  theme(panel.grid = element_blank(),
        axis.text.x = element_text(angle = 90, hjust = 1, vjust = 0.5), 
        legend.title = element_text(vjust = 1),
        legend.position = 'top', 
        axis.ticks = element_blank()) 

h7_ccf <- lapply(ccf_all, cor_gather) |> 
  bind_rows(.id = 'group') |> 
  separate(group, into = c('horizon', 'depth')) |> 
  arrange(var1, var2) |> 
  group_by(horizon, depth) |> 
  #this makes sure we only have the upper left quadrant (not sure how it works tbh)
  distinct(smaller = pmin(var1, var2),
           larger = pmax(var1, var2),
           .keep_all = TRUE) %>%
  select(-smaller, -larger) |>
  mutate(horizon = as.numeric(horizon)) |>
  filter(horizon == 7) |> 
  ggplot(aes(x=var1, y=var2)) +
  geom_tile(aes(fill = cor)) +
  geom_text(aes(label = round(cor, 2)), size = 3) +
  facet_wrap(~horizon, labeller = label_both) +
  labs(x='', y='')+
  colorspace::scale_fill_continuous_diverging(limits = c(-1,1), palette = 'Blue-Red 2') +
  theme_bw(base_size = 12) +
  theme(panel.grid = element_blank(),
        axis.text.x = element_text(angle = 90, hjust = 1, vjust = 0.5), 
        axis.ticks = element_blank()) 


h14_ccf <- lapply(ccf_all, cor_gather) |> 
  bind_rows(.id = 'group') |> 
  separate(group, into = c('horizon', 'depth')) |> 
  arrange(var1, var2) |> 
  group_by(horizon, depth) |> 
  #this makes sure we only have the upper left quadrant (not sure how it works tbh)
  distinct(smaller = pmin(var1, var2),
           larger = pmax(var1, var2),
           .keep_all = TRUE) %>%
  select(-smaller, -larger) |>
  mutate(horizon = as.numeric(horizon)) |>
  filter(horizon == 14) |> 
  ggplot(aes(x=var1, y=var2)) +
  geom_tile(aes(fill = cor)) +
  geom_text(aes(label = round(cor, 2)), size = 3) +
  facet_wrap(~horizon, labeller = label_both) +
  labs(x='', y='')+
  colorspace::scale_fill_continuous_diverging(limits = c(-1,1), palette = 'Blue-Red 2') +
  theme_bw(base_size = 12) +
  theme(panel.grid = element_blank(),
        axis.text.x = element_text(angle = 90, hjust = 1, vjust = 0.5), 
        axis.ticks = element_blank()) 



plot_5 <- ggpubr::ggarrange(h1_ccf, h7_ccf, h14_ccf, nrow = 1,
                            labels = c('a)', 'b)', 'c)'), 
                            common.legend = T)

ggsave(plot_5, filename = file.path(out_dir, 'plot_5.png'), height = 8, width = 21, units = 'cm')
#============================================#

##  PLOT 6 - rank proportions ====
# plot of proportion of ranked forecasts
facet_tags <- expand.grid(depth = c(1,8),
                          model_id= all_models[!str_detect(all_models, '  ')]) |> 
  mutate(tag = paste0(letters[1:14],')'))

plot_6 <-
  all_scored %>%
  filter(reference_datetime != "2021-02-22 00:00:00",
         variable == 'temperature',
         horizon %in% c(1:14), 
         model_id %in% all_models, 
         depth %in% c(1,8)) |> 
  select(reference_datetime, horizon, depth, model_id, logs) |> 
  arrange(reference_datetime, horizon, depth, logs) |> 
  group_by(reference_datetime, horizon, depth) |> 
  mutate(rank = row_number()) |> 
  group_by(horizon, depth, rank, model_id) |> 
  summarise(proportion = (n()/104)) |> 
  ungroup() |>
  # make sure every rank is present in each model/depth/horizon group
  complete(rank, nesting(model_id, depth, horizon), fill = list(proportion = 0))  |> 
  
  ggplot() +
  geom_area(aes(x=horizon, y=proportion, fill=fct_rev(as.factor(rank))),
            colour="black", stat = 'identity', position = 'stack') +
  facet_grid2(depth~factor(model_id, levels = all_models[!str_detect(all_models, '  ')]), 
              axes = 'all', remove_labels = 'all') +
  scale_fill_brewer(palette = 'RdBu', name = 'Rank') +
  theme_bw() +
  theme(panel.spacing.x = unit(1.1, 'lines'),
        panel.spacing.y = unit(1.1, 'lines')) +
  coord_cartesian(ylim = c(0,1), xlim = c(1,14), clip = 'off') +
  scale_y_continuous(expand = c(0,0), name = 'Proportion of forecasts') +
  scale_x_continuous(expand = c(0,0), breaks = c(1,7,14), name = 'Horizon (days)') +
  geom_text(data = facet_tags,
            mapping = aes(x = -0.12, y = 1.1, label = tag), 
            size = 4, fontface = 'bold') 

ggsave(plot_6, filename = file.path(out_dir, 'plot_6.png'), height = 8, width = 25, units = 'cm')

#==============================================#

##  PLOT 7 - depth disaggregated scores ======
facet_tags <- data.frame(depth = c(1,8),
                         metric = c('bias', 'bias',
                                    'sd', 'sd',
                                    'ign', 'ign'),
                         tag = c('a)', 'b)',
                                 'c)', 'd)', 
                                 'e)', 'f)'))
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
  facet_wrap(~depth, ncol = 1, labeller = label_both) +
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


plot_7 <- ggpubr::ggarrange(bias_plot, sd_plot, logs_plot, 
                            ncol  = 3, common.legend = T, align = "hv") 

ggsave(plot_7, filename = file.path(out_dir, 'plot_7.png'), height = 10, width = 15, units = 'cm')
#============================================#

##  PLOT 8 - Shadowing time ======
facet_tags <- data.frame(strat = c('stratified_period', 'mixed_period'),
                         tag = c('b)', 'a)'))

plot_8 <- 
  shadow_summary |>
  mutate(strat = is_strat(reference_datetime, strat_dates)) %>%
  group_by(depth, model_id, strat) |> 
  summarise(mean_shadow = mean(shadow_length, na.rm = T)) |> 
  ggplot() +
  geom_point(aes(x=mean_shadow, y=depth, 
                 colour = model_id, shape = model_id)) +
  scale_colour_manual(values = cols, limits = all_models,
                      name = 'Model')  +
  scale_shape_manual(values = shapes, limits = all_models,
                     name = 'Model')  +
  scale_y_reverse(breaks = c(0,2,4,6,8)) +
  scale_x_continuous(breaks = seq(0,14,2), 
                     expand = c(0,0)) +
  theme_bw() + 
  facet_wrap(~strat, labeller = labeller(.rows = time_periods)) +
  labs(x= 'Mean shadowing length (days)', y = 'Depth (m)') +
  theme(panel.spacing = unit(1.1, "lines")) +
  geom_text(data = facet_tags,
            mapping = aes(x = -0.6, y = -0.9, label = tag), 
            size = 4, fontface = 'bold') +
  coord_cartesian(clip = 'off',
                  xlim = c(0,14),
                  ylim = c(9,0))

ggsave(plot_8, filename = file.path(out_dir, 'plot_8.png'), height = 7, width = 15, units = 'cm')

#=====================================#
## TABLE_1 - aggregated scores ====
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


# Supplementary Information -----------------------------------------------
##  PLOT_S1 - Parameter tuning =====


# evolution of the parameters over the spin up period before the first forecast

# which parameter are for which model are in the forecast files
param_config <- read_csv('./configuration/ler_ms/parameter_calibration_config.csv') |> 
  rename(model_id = model)

# got to the right bucket
param_values <- arrow::s3_bucket(bucket = "forecasts/ler_ms3/parquet",
                                 endpoint_override =  "s3.flare-forecast.org",
                                 anonymous = TRUE) |> 
  open_dataset() |> 
  filter(variable %in% param_config$par_names_save) |> 
  collect()

# manual layout of facets
layout_design <- "
  ABC
  DE#
  FG#
"
#only want the parameter values for the spin up (before forecast 0)
plot_s1 <- param_values |> 
  filter(reference_datetime == '2021-02-22 00:00:00') |> 
  ggplot(aes(x=datetime, y = prediction, group = parameter)) +
  geom_line() +
  scale_x_datetime(date_labels = '%d %b %Y', date_breaks = '1 month', expand = c(0.01, 0)) +
  ggh4x::facet_manual(vars(model_id,variable), 
                      scales = 'free', axes = T, 
                      design = layout_design, 
                      labeller = label_both) +
  theme_bw() +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5)) +
  labs(x = 'Date', y = 'Parameter value')

ggsave(plot_s1, filename = file.path(out_dir, 'plot_s1.png'), height = 20, width = 15, units = 'cm')


#===========================#

##  PLOT_S2 - Observational uncertainty =====
lake_directory <- here::here()
FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/271/7/71e6b946b751aa1b966ab5653b01077f",
                     file = 'FCR_Catwalk_2018_2021.csv',
                     lake_directory)

catwalk <- read_csv('./data_raw/FCR_Catwalk_2018_2021.csv') |>
  select(contains(c('datetime','ThermistorTemp'))) |>
  select(-contains('Flag')) |>
  filter(DateTime <= as_datetime('2021-10-01'))


mean_interday_sd <- 
  catwalk |>
  pivot_longer(-DateTime,
               names_to = 'depth',
               values_to = 'temperature',
               names_prefix = 'ThermistorTemp_C_') |>
  # get the date only
  mutate(date = as_date(format(DateTime, '%Y-%m-%d')),
         depth = as.numeric(ifelse(depth == 'surface', 0, depth))) |>
  group_by(depth, date) |>
  # calculate the within each day/depth sd
  summarise(sd = sd(temperature, na.rm = T), .groups = 'drop') |>
  # get the mean sd for each depth
  # ungroup() |> 
  group_by(depth) |>
  summarise(mean_sd = round(mean(sd, na.rm = T), 3)) 

wc_mean_sd <- 
  catwalk |>
  pivot_longer(-DateTime,
               names_to = 'depth',
               values_to = 'temperature',
               names_prefix = 'ThermistorTemp_C_') |>
  # get the date only
  mutate(date = as_date(format(DateTime, '%Y-%m-%d')),
         depth = as.numeric(ifelse(depth == 'surface', 0, depth))) |>
  group_by(depth, date) |>
  # calculate the within each day/depth sd
  summarise(sd = sd(temperature, na.rm = T), .groups = 'drop') |>
  summarise(mean_sd = round(mean(sd, na.rm = T), 3)) |> 
  pull()


plot_s2 <- mean_interday_sd |>
  ggplot(aes(x=mean_sd,  y=depth)) +
  geom_point() +
  geom_text(aes(label = round(mean_sd, 2)), size = 4, hjust = -0.1, vjust = -0.1) +
  scale_y_reverse(limits = c(8,0)) +
  scale_x_continuous(limits = c(0,0.6)) +
  theme_bw() +
  labs(x= 'Mean daily standard deviation (°C)', y= 'Depth (m)',
       caption = paste0('Note: Overall mean = ', wc_mean_sd))

ggsave(plot_s2, filename = file.path(out_dir, 'plot_s2.png'), height = 10, width = 10, units = 'cm')
#===========================#

##  PLOT_S3 - Forecast time series =====
plot_s3 <- 
  all_scored %>%
  filter(variable == 'temperature',
         horizon %in%  c(1, 7, 14),
         depth %in% c(1,8),
         model_id %in% individual_models) %>%
  na.omit() %>%
  
  ggplot(., aes(x=datetime, y= mean)) +
  annotate("rect", ymin = -Inf, ymax = Inf, 
           xmin = as_datetime(strat_dates$start[3]),
           xmax = as_datetime(strat_dates$end[3]), alpha = 0.1) +
  annotate("rect", ymin = -Inf, ymax = Inf, 
           xmin = as_datetime(strat_dates$start[4]),
           xmax = as_datetime(strat_dates$end[4]), alpha = 0.1) +
  facet_grid(depth~horizon) +
  geom_ribbon(aes(ymax = quantile90, ymin = quantile10, colour = model_id), 
              fill = NA, linetype = 'dashed') +
  geom_line(aes(colour = model_id), linewidth = 0.8) +
  geom_point(aes(y = observation), alpha = 0.5, size = 0.8) +
  scale_colour_manual(values = cols, limits = individual_models, name = 'Forecast') +
  labs(y = 'Water temperature (°C)') + 
  scale_y_continuous(sec.axis = sec_axis(~ . , name = "Depth (m)", breaks = NULL, labels = NULL)) +
  scale_x_datetime(date_labels = '%d %b %y', date_breaks = "6 months", name = 'Date',
                   sec.axis = sec_axis(~ . , name = "Horizon (days)", breaks = NULL, labels = NULL))  +
  theme_bw(base_size = 12) + 
  theme(legend.position = 'top',
        axis.text.x = element_text(angle = 45, vjust = 1, hjust = 1), 
        axis.title.y.right = element_text(vjust = 1.2),
        axis.title.x.top = element_text(vjust = 1.2))

ggsave(plot_s3, filename = file.path(out_dir, 'plot_s3.png'), height = 12, width = 32, units = 'cm')



#===========================#

##  PLOT_S4 - Best/worst rank proportion =====

facet_tags <- data.frame(depth = c(1,8),
                         name = c('best model', 'worst model',
                                  'worst model', 'best model'),
                         tag = c('a)', 'd)',
                                 'c)', 'b)'))

# plot of prportion of ranked forecasts
plot_s4 <- all_scored %>%
  filter(reference_datetime != "2021-02-22 00:00:00",
         variable == 'temperature',
         horizon %in% c(1:14), 
         model_id %in% all_models, 
         depth %in% c(1,8)) |> 
  select(reference_datetime, horizon, depth, model_id, logs) |> 
  arrange(reference_datetime, horizon, depth, logs) |> 
  group_by(reference_datetime, horizon, depth) |> 
  mutate(rank = row_number()) |> 
  group_by(horizon, depth, rank, model_id) |> 
  summarise(proportion = (n()/104)) |> 
  ungroup() |>
  # make sure every rank is present in each model/depth/horizon group
  complete(rank, nesting(model_id, depth, horizon), fill = list(proportion = 0))  |> 
  filter(rank %in% c(1,7)) |> 
  mutate(name = ifelse(rank == 1, 'best model', ifelse(rank == 7, 'worst model', NA))) |> 
  
  ggplot() +
  geom_area(aes(x=horizon, y=proportion, 
                fill=factor(model_id, levels = all_models[!str_detect(all_models, '  ')])),
            stat = 'identity', position = 'stack') +
  facet_grid2(depth~name, axes = 'all', remove_labels = 'all') +
  scale_fill_manual(values = cols, limits = all_models, name = 'Model')  +
  theme_bw(base_size = 14) +
  theme(panel.spacing = unit(1.2, 'lines')) +
  coord_cartesian(ylim = c(0,1), xlim = c(1,14), clip = 'off') +
  scale_y_continuous(expand = c(0,0), name = 'Proportion of forecasts') +
  scale_x_continuous(expand = c(0,0), breaks = c(1,7,14), name = 'Horizon (days)') +
  geom_text(data = facet_tags,
            mapping = aes(x = -0, y = 1.08, label = tag), 
            size = 4, fontface = 'bold') 

ggsave(plot_s4, filename = file.path(out_dir, 'plot_s4.png'), height = 12, width = 16, units = 'cm')


#===========================#

##  TABLE_S2 - Disaggregated scores =====

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

#===========================# 