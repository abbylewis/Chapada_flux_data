#Source
source("./R/drop_dir.R")
source("./R/get_dropbox_token.R")
source("./R/load_file.R")
library(tidyverse)

download_met <- function(met_folder = here::here("Raw_data", "met")){
  #Identify all files
  files <- drop_dir(path = "Chapada_Loggernet_Data/archived_data")
  relevant_files <- files %>%
    filter(grepl("ClimaVue50_15min", name))
  current <- drop_dir(path = "Chapada_Loggernet_Data/current_data") %>%
    filter(grepl("ClimaVue50_15min", name))
  
  #Remove files that are already loaded
  already_loaded <- list.files(met_folder)
  relevant_files <- relevant_files %>%
    filter(!name %in% already_loaded)
  
  #Load current data
  new <- current$path_display %>%
    map(load_file, output_dir = met_folder)
  
  if(nrow(relevant_files) == 0){
    message("No new files to download")
  } else {
    message("Downloading ", nrow(relevant_files), " files")
    all_data <- relevant_files$path_display %>%
      map(load_file, output_dir = met_folder)
  }
  
  message("Processing and saving all historical met data")
  
  data <- list.files(met_folder, full.names = T) %>%
    map(read.csv, skip = 1) %>%
    bind_rows() %>%
    filter(!TIMESTAMP == "TS") %>%
    mutate(TIMESTAMP = as_datetime(TIMESTAMP)) %>%
    filter(!is.na(TIMESTAMP)) %>%
    distinct()
  
  write_csv(data %>%
              filter(TIMESTAMP >= as.Date("2025-03-18")) %>%
              select(all_of(c("TIMESTAMP", "SlrFD_W_Avg", "AirT_C_Avg", "Rain_mm_Tot"))), 
            here::here("processed_data", "met_2025_dashboard.csv"))
  return(T)
}
