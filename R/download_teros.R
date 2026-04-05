#Source
source("./R/drop_dir.R")
source("./R/get_dropbox_token.R")
source("./R/load_file.R")
source("./R/load_data.R")
library(tidyverse)

download_teros <- function(teros_folder = here::here("Raw_data", "teros")){
  #Identify all files
  files <- drop_dir(path = "Chapada_Loggernet_Data/archived_data")
  relevant_files <- files %>%
    filter(grepl("Teros12", name))
  current <- drop_dir(path = "Chapada_Loggernet_Data/current_data") %>%
    filter(grepl("Teros12", name))
  
  #Remove files that are already loaded
  already_loaded <- list.files(teros_folder)
  relevant_files <- relevant_files %>%
    filter(!name %in% already_loaded,
           !grepl("backup", name))
  
  #Load current data
  new <- current %>%
    filter(!grepl("backup", name)) %>%
    pull(path_display) %>%
    map(load_file, output_dir = teros_folder)
  
  if(nrow(relevant_files) == 0){
    message("No new files to download")
  } else {
    message("Downloading ", nrow(relevant_files), " files")
    all_data <- relevant_files$path_display %>%
      map(load_file, output_dir = teros_folder)
  }
  
  message("Processing and saving all historical teros data")
  
  design <- read_csv("processed_data/design.csv",
                     show_col_types = F) %>%
    mutate(location = ifelse(grepl("H", link), "high", "low"))
  
  data <- list.files(teros_folder, full.names = T) %>%
    map(load_redox) %>%
    bind_rows() %>%
    filter(!TIMESTAMP == "TS") %>%
    mutate(TIMESTAMP = as_datetime(TIMESTAMP)) %>%
    filter(!is.na(TIMESTAMP)) %>%
    select(-RECORD, -Statname, -BattV_Avg, -PB) %>%
    distinct() %>%
    pivot_longer(contains("teros", ignore.case = T),
                 names_to = "loggernet_variable") %>%
    left_join(design %>%
                select(loggernet_variable, research_name, link, location) %>%
                distinct()) %>%
    mutate(chamber = as.numeric(gsub("H|L", "", link)),
           var = sub("_teros12", "", research_name)) %>%
    select(-research_name)
  
  data_hourly <- data  %>%
    filter(TIMESTAMP >= as.Date("2025-09-18")) %>%
    select(-link, -loggernet_variable) %>%
    mutate(TIMESTAMP = round_date(TIMESTAMP, "hour")) %>%
    group_by(TIMESTAMP, location, chamber, var) %>%
    summarize(value = mean(as.numeric(value)), .groups = "drop") %>%
    mutate(value = round(value, 1))
  
  write_csv(data_hourly,
            here::here("processed_data", "teros_2025_dashboard.csv"))
  return(T)
}