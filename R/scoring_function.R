# remotes::install_github('eco4cast/neon4cast')
generate_forecast_score_2 <- function(targets_file,
                                    forecast_file,
                                    output_directory){
  
  target <- readr::read_csv(targets_file, show_col_types = FALSE) |>
    dplyr::mutate(site_id = paste0(site_id,"-",depth))
  
  if("time" %in% colnames(target)){
    target <- target |>
      dplyr::rename(datetime = time)
  }
  
  fn <- tools::file_path_sans_ext(tools::file_path_sans_ext(basename(forecast_file)))
  
  file_name <- file.path(output_directory,paste0(fn, ".parquet"))
  
  forecast <- forecast_file %>%
    read4cast::read_forecast() %>%
    # select(-any_of(pub_time)) %>%
    filter(variable_type == "state") %>%
    dplyr::mutate(filename = forecast_file,
                  site_id = paste0(site_id,"-",depth)) %>%
    score4cast::standardize_forecast() %>%
    dplyr::mutate(family = as.character(family)) %>%
    score4cast::crps_logs_score(target) %>%
    
    mutate(reference_datetime = lubridate::as_datetime(reference_datetime),
           horizon = datetime-reference_datetime) %>%
    mutate(horizon = as.numeric(lubridate::as.duration(horizon),
                                units = "seconds"),
           horizon = horizon / 86400) %>%
    arrow::write_parquet(file_name)
  
  invisible(file_name)
}
