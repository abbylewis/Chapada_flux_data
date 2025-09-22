#Source
source(here::here("R","drop_dir.R"))
source(here::here("R","get_dropbox_token.R"))
source(here::here("R","load_file.R"))
library(tidyverse)

#' download_new_data
#'
#' @description
#' This function looks for data files on dropbox that are new or have been modified since we last loaded data
#' 
#' @return NULL
#' @export
#'
#' @examples
download_new_data <- function(lgr_folder = here::here("Raw_data","dropbox_downloads")){
  #Identify all files
  message("Looking for new data files on dropbox")
  relevant_files <- drop_dir(path = "Chapada_Loggernet_Data/current_data") %>%
    filter(grepl("FLUX_COMB", name)) %>%
    mutate(server_modified = as.POSIXct(server_modified, format = "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"))
  
  #Remove files that are already loaded and haven't been modified
  already_loaded <- list.files(lgr_folder)
  loaded_file_info <- file.info(list.files(lgr_folder, full.names = T)) %>%
    mutate(name = basename(row.names(.))) %>%
    select(name, mtime)
  modified <- relevant_files %>%
    select(name, server_modified) %>%
    left_join(loaded_file_info, by = "name") %>%
    filter(server_modified > (mtime - hours(3))) #Add a 3-hr buffer to re-download really recent files
  relevant_files <- relevant_files %>% #Only process files that are new or have been modified on dropbox
    filter(!name %in% already_loaded | name %in% modified$name)
  
  if(nrow(relevant_files) == 0){
    message("No new files to download")
  } else {
    message("Downloading ", nrow(relevant_files), " files")
    all_data <- relevant_files$path_display %>%
      map(load_file, output_dir = lgr_folder)
  }
}
