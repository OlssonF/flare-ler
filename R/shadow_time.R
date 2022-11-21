
shadow_length <- function(df, mod, dep, ref_datetime) {
  df_use <- get(df) %>%
    filter(reference_datetime == ref_datetime & depth == dep &
             model_id == mod) |> 
    ungroup() %>%
    filter(!is.na(observation)) %>%
    arrange(datetime) %>%
    mutate(shadow = ifelse(observation <= quantile97.5 &
                             observation >= quantile02.5, 
                           T, F))
  
  shadow_rle <- rle(df_use$shadow) 
  
  if (shadow_rle$values[1] == TRUE) {
    max_shadow <- shadow_rle$lengths[min(which(shadow_rle$values == T))]
    shadow_out <- data.frame(model_id = mod,
                             depth = dep, 
                             reference_datetime = ref_datetime,
                             shadow_time = df_use$horizon[max_shadow])
  } else {
    shadow_out <- data.frame(model_id = mod,
                             depth = dep,
                             reference_datetime = ref_datetime,
                             shadow_time = 0)
  }
  message(paste(ref_datetime, dep, mod))
  return(shadow_out)
}
