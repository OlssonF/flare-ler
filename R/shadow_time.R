
# function to identify the maximum initial shadow length
shadow_rle <- function(shadow) {
  
  rle_result <- rle(shadow == T)
  
  if(rle_result$values[1] == TRUE) {
    
    shadow_length <- rle_result$lengths[1]
    
  } else {
    shadow_length <- 0
  }
  
  return(shadow_length)
}


calc_shadow_time <- function(forecast_df, targets_df, var = 'temperature', sd = 0.1, p = c(0.975, 0.025)) {
  
  # mutate the forecast df into the right format
  df1 <- forecast_df %>%
    select(datetime, reference_datetime, depth, site_id,
           variable, parameter, prediction, model_id) |> 
    filter(variable == var, 
           datetime > as_datetime(reference_datetime)) |> 
    arrange(parameter, datetime) |> 
    mutate(site_id = paste0(site_id, '_', depth)) |> 
    left_join(targets_df, by = c("datetime", "depth", "site_id", "variable")) |> 
    na.omit(observation) |> 
    mutate(obs_upper = qnorm(mean = observation, sd = sd, p = max(p)),
           obs_lower = qnorm(mean = observation, sd = sd, p = min(p)),
           shadow = ifelse(prediction <= obs_upper &
                             prediction >= obs_lower,
                           T, F))
  if (nrow(df1) > 0) {
    # for each depth and ensemble member calculate the rle length
    df2 <- df1 |> 
      dplyr::ungroup() |> 
      dplyr::group_by(depth, parameter) |> 
      dplyr::summarise(shadow_length = shadow_rle(shadow = shadow)) 
    
    # summarise this to figure out the longest shadow length for each depth
    final_df <- df2 %>%
      ungroup() |> 
      group_by(depth) |> 
      summarise(shadow_length = max(shadow_length))
    
    return(final_df)
  }
  
}
