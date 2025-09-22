
qaqc <- function(L0_file = here::here("processed_data","L0.csv")){
  ## PLACEHOLDER FOR QAQC
  
  slopes <- read_csv(L0_file, show_col_types = F) %>%
    filter(!is.na(flux_start))
  
  #Output
  write.csv(slopes, here::here("L1.csv"), row.names = FALSE)
  
  return(slopes)
}
