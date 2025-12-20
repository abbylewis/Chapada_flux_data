#Source
source("./R/drop_dir.R")
source("./R/get_dropbox_token.R")
source("./R/load_file.R")
source("./R/load_data.R")
library(tidyverse)

download_bme <- function(bme_folder = here::here("Raw_data", "bme")){
  #Identify all files
  files <- drop_dir(path = "Chapada_Loggernet_Data/archived_data")
  relevant_files <- files %>%
    filter(grepl("BME", name))
  current <- drop_dir(path = "Chapada_Loggernet_Data/current_data") %>%
    filter(grepl("BME", name))
  
  #Remove files that are already loaded
  already_loaded <- list.files(bme_folder)
  relevant_files <- relevant_files %>%
    filter(!name %in% already_loaded,
           !grepl("backup", name))
  
  #Load current data
  new <- current %>%
    filter(!grepl("backup", name)) %>%
    pull(path_display) %>%
    map(load_file, output_dir = bme_folder)
  
  if(nrow(relevant_files) == 0){
    message("No new files to download")
  } else {
    message("Downloading ", nrow(relevant_files), " files")
    all_data <- relevant_files$path_display %>%
      map(load_file, output_dir = bme_folder)
  }
  
  message("Processing and saving all historical bme data")
  
  design <- read_csv("processed_data/design.csv",
                     show_col_types = F) %>%
    mutate(location = ifelse(grepl("H", link), "high", "low"))
  
  data <- list.files(bme_folder, full.names = T) %>%
    map(load_redox) %>%
    bind_rows() %>%
    filter(!TIMESTAMP == "TS") %>%
    mutate(TIMESTAMP = as_datetime(TIMESTAMP)) %>%
    filter(!is.na(TIMESTAMP)) %>%
    select(-RECORD, -BattV_Min, -PTemp_C) %>%
    distinct() %>%
    pivot_longer(`BP(1)`:`Temp(9)`,
                 names_to = "loggernet_variable") %>%
    left_join(design %>%
                select(loggernet_variable, research_name, link, location) %>%
                distinct()) %>%
    mutate(chamber = as.numeric(gsub("H|L", "", link)),
           value = as.numeric(value)) %>%
    select(-loggernet_variable, -link)
  
  data_hourly <- data %>%
    mutate(TIMESTAMP = round_date(TIMESTAMP, "hour")) %>%
    group_by(TIMESTAMP, location, research_name, chamber) %>%
    summarize(value = mean(value))
  
  write_csv(data_hourly %>%
              filter(TIMESTAMP >= as.Date("2025-03-18")),
            here::here("processed_data", "bme_2025_dashboard.csv"))
  return(T)
}