#Source
source("./R/drop_dir.R")
source("./R/get_dropbox_token.R")
source("./R/load_file.R")
source("./R/load_data.R")
library(tidyverse)

download_redox <- function(redox_folder = here::here("Raw_data", "redox")){
  #Identify all files
  files <- drop_dir(path = "Chapada_Loggernet_Data/archived_data")
  relevant_files <- files %>%
    filter(grepl("Redox15", name))
  current <- drop_dir(path = "Chapada_Loggernet_Data/current_data") %>%
    filter(grepl("Redox15", name))
  
  #Remove files that are already loaded
  already_loaded <- list.files(redox_folder)
  relevant_files <- relevant_files %>%
    filter(!name %in% already_loaded,
           !grepl("backup", name))
  
  #Load current data
  new <- current %>%
    filter(!grepl("backup", name)) %>%
    pull(path_display) %>%
    map(load_file, output_dir = redox_folder)
  
  if(nrow(relevant_files) == 0){
    message("No new files to download")
  } else {
    message("Downloading ", nrow(relevant_files), " files")
    all_data <- relevant_files$path_display %>%
      map(load_file, output_dir = redox_folder)
  }
  
  message("Processing and saving all historical redox data")
  
  design <- read_csv("processed_data/design.csv",
                     show_col_types = F) %>%
    mutate(location = ifelse(grepl("H", link), "high", "low"))
  
  data <- list.files(redox_folder, full.names = T) %>%
    map(load_redox) %>%
    bind_rows() %>%
    filter(!TIMESTAMP == "TS") %>%
    mutate(TIMESTAMP = as_datetime(TIMESTAMP)) %>%
    filter(!is.na(TIMESTAMP)) %>%
    select(-RECORD, -BattV, -Statname) %>%
    distinct() %>%
    pivot_longer(contains("redox", ignore.case = T),
                 names_to = "loggernet_variable") %>%
    left_join(design %>%
                select(loggernet_variable, research_name, link, location) %>%
                distinct()) %>%
    mutate(chamber = as.numeric(gsub("H|L", "", link)),
           depth = as.numeric(str_extract(research_name, "[1]*5")),
           ref = str_extract(research_name, "refa|refb")) %>%
    select(-research_name)
  
  data_hourly <- data  %>%
    filter(TIMESTAMP >= as.Date("2025-09-18")) %>%
    select(-link, -loggernet_variable) %>%
    mutate(TIMESTAMP = round_date(TIMESTAMP, "hour")) %>%
    group_by(TIMESTAMP, location, chamber, depth, ref) %>%
    summarize(value = mean(as.numeric(value)), .groups = "drop") %>%
    mutate(value = round(value, 1))
  
  write_csv(data_hourly,
            here::here("processed_data", "redox_2025_dashboard.csv"))
  return(T)
}