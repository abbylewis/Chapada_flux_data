
source(here::here("R","load_data.R"))

#' calculate_flux
#'
#' @description
#' This function calculates the raw CH4 fluxes for all files in the dropbox_downloads folder
#'
#' @param start_date earliest file to process (based on file name)
#' @param end_date latest file to process
#' @param modif_start_date only run files that have been modified/created since this date
#'
#' @return L0 slopes

calculate_flux <- function(start_date = NULL,
                           end_date = NULL,
                           reprocess = F,
                           plot = reprocess){
  ### Load files ###
  files <- list.files(here::here("Raw_data","dropbox_downloads"), full.names = T)
  #By default, only calculate slopes for files that have been modified/created since the last time we ran the script
  if(!reprocess){
    modif_start_date = file.info(here::here("processed_data","L0.csv"))$mtime
    files <- files[file.info(files)$mtime > modif_start_date]
  }
  #If a start and end date are provided, look for files that match these dates
  if(!is.null(start_date) & !is.null(end_date)){
    possible_file_names <- seq(as.Date(start_date), 
                               as.Date(end_date), 
                               by = "1 day") %>%
      format("%Y%m%d")
    if(as.Date(end_date) >= Sys.Date()) {possible_file_names <- c(possible_file_names, "current.dat")}
    files <- files[grepl(paste0(possible_file_names, collapse = "|"), files)]
  } else if (!is.null(start_date) | !is.null(end_date)){
    stop("If you provide a start or end date, you must provide both")
  } 
  
  if(length(files) == 0){
    message("No files to process")
    return(read_csv(here::here("processed_data","L0.csv"), show_col_types = F))
  }
  
  exclude <- c("INSERT_FILENAME_HERE.csv") #If you want to remove any files
  #Insert file names here if they should be excluded
  #Currently excluding last year's data
  files <- files[!grepl(paste0(exclude, collapse = "|"), files)]
  message(paste0("Calculating fluxes for ", length(files), " files"))
  
  #Load data
  data_small <- files %>%
    map(load_data) %>% #custom data loading function that deals with multiple file formats
    bind_rows()  %>%
    filter(!TIMESTAMP == "TS") %>%
    mutate(TIMESTAMP = as_datetime(TIMESTAMP, tz = "EST")) %>%
    filter(!is.na(TIMESTAMP)) %>%
    distinct()
  
  #Format data
  data_numeric <- data_small %>%
    mutate(across(c("CH4d_ppm", "CO2d_ppm", "Manifold_Timer", "Fluxing_Chamber"), as.numeric)) %>%
    mutate(CH4d_ppm = ifelse(CH4d_ppm <=0, NA, CH4d_ppm),
           CO2d_ppm = ifelse(CO2d_ppm <=0, NA, CO2d_ppm)) %>%
    filter(!is.na(Fluxing_Chamber)) %>%
    mutate(Flag = "No issues")
  
  #Remove data as specified in maintenance log
  googlesheets4::gs4_deauth() # No authentication needed
  today <- Sys.time()
  maint_log <- googlesheets4::read_sheet("https://docs.google.com/spreadsheets/d/103PpjEmjLAQkov9ywjA5KxyJiIP3nEWy7V8gWVZhd1M/edit?gid=0#gid=0",
                                         col_types = "c") %>%
    mutate(Start_time = as_datetime(Start_time, tz = "America/New_York"),
           End_time = as_datetime(End_time, tz = "America/New_York"),
           End_time = ifelse(is.na(End_time), today, End_time),
           End_time = as_datetime(End_time, tz = "America/New_York"))
  for(i in 1:nrow(maint_log)){
    data_numeric <- data_numeric %>%
      mutate(Flag = ifelse(TIMESTAMP <= maint_log$End_time[i] & 
                             TIMESTAMP >= maint_log$Start_time[i] &
                             Fluxing_Chamber %in% eval(parse(text = maint_log$Chambers[i])) &
                             location %in% eval(parse(text = maint_log$Location[i])),
                           maint_log$Flag[i],
                           Flag),
             across(c("CH4d_ppm", "CO2d_ppm"), 
                    ~ifelse(TIMESTAMP <= maint_log$End_time[i] & 
                              TIMESTAMP >= maint_log$Start_time[i] &
                              Fluxing_Chamber %in% eval(parse(text = maint_log$Chambers[i])) &
                              location %in% eval(parse(text = maint_log$Location[i])),
                            NA,
                            .x)))
  }
  
  #Group flux intervals, prep for slopes
  grouped_data <- data_numeric %>%
    mutate(date = as.Date(TIMESTAMP, tz = "Etc/GMT-3")) %>%
    #Group flux intervals
    group_by(location) %>%
    arrange(TIMESTAMP) %>%
    mutate(group = group_fun(Fluxing_Chamber)) %>%
    group_by(group, Fluxing_Chamber)  %>%
    #Record the amount of time from when chamber closed
    mutate(start = min(TIMESTAMP),
           end = max(TIMESTAMP),
           change = as.numeric(difftime(TIMESTAMP, start, units = "days")),
           change_s = as.numeric(difftime(TIMESTAMP, start, units = "secs")),
           max_s = ifelse(sum(!is.na(CH4d_ppm) > 0),
                          change_s[which.max(CH4d_ppm)],
                          NA),
           min_s = ifelse(sum(!is.na(CH4d_ppm) > 0),
                          change_s[which.min(CH4d_ppm)],
                          NA),)
  
  #Save flags for data that will be removed in the next step
  flags <- grouped_data %>%
    ungroup() %>%
    select(start, Fluxing_Chamber, Flag, date, group) %>%
    distinct() %>%
    group_by(Fluxing_Chamber, start, date, group) %>%
    filter(n() == 1 | !Flag == "No issues")
  
  #Data flags
  data_flags <- grouped_data %>%
    group_by(group, Fluxing_Chamber, date) %>%
    summarize(Flag_CO2_slope = ifelse(sum(!is.na(CO2d_ppm)) > 5, 
                                      "No issues", "Insufficient data"),
              Flag_CH4_slope = ifelse(sum(!is.na(CH4d_ppm)) > 5, 
                                      "No issues", "Insufficient data"),
              #cutoff_removed = unique(cutoff),
              #n_removed = unique(n),
              .groups = "drop") 
  
  #Filter
  start_cutoff <- 200 #Buffer of time after flux window
  end_cutoff <- 600
  filtered_data <- grouped_data %>%
    group_by(group, Fluxing_Chamber)  %>%
    mutate(n = sum(Flux_Status == 3),
           cutoff = NA) %>%
    #Remove earlier measurements
    filter(Flux_Status == 3,
           max(change_s) < 1000, #After ~15 min there is probably a problem
           n < 200 #probably some issue if this many measurements are taken
    ) 
  
  #Run lm
  slopes <- filtered_data %>%
    pivot_longer(c(CH4d_ppm, CO2d_ppm), names_to = "gas", values_to = "conc") %>%
    group_by(gas, group, Fluxing_Chamber, date, location) %>%
    mutate(n = sum(!is.na(conc))) %>%
    filter(!is.na(conc),
           n > 5) %>%
    summarize(model = list(lm(conc ~ change)),
              slope_ppm_per_day = model[[1]]$coefficients[[2]],
              R2 = summary(model[[1]])$r.squared,
              p = summary(model[[1]])$coefficients[,4][2],
              rmse = sqrt(mean(model[[1]]$residuals^2)),
              max = max(conc),
              min = min(conc),
              init = first(conc),
              max_s = unique(max_s),
              flux_start = min(TIMESTAMP),
              flux_end = max(TIMESTAMP),
              TIMESTAMP = unique(start),
              n = sum(!is.na(conc)),
              #cutoff = unique(cutoff),
              .groups = "drop") %>%
    select(-model) %>%
    mutate(gas = case_match(gas,
                            "CH4d_ppm" ~ "CH4",
                            "CO2d_ppm" ~ "CO2")) %>%
    pivot_wider(names_from = gas, 
                values_from = c(slope_ppm_per_day, R2, p, rmse, init, max, min),
                names_glue = "{gas}_{.value}") %>%
    full_join(flags, by = c("TIMESTAMP" = "start", "Fluxing_Chamber", "date", "group")) %>%
    full_join(data_flags, by = c("group", "Fluxing_Chamber", "date")) 
  
  if(!reprocess){
    #Load previously calculated slopes
    old_slopes <- read_csv(here::here("processed_data","L0.csv"), 
                           col_types = "nnDccccnnnnnnnnnnnnnnnccc",
                           show_col_types = F) %>%
      mutate(TIMESTAMP = as_datetime(TIMESTAMP, tz = "EST"),
             flux_start = as_datetime(flux_start, tz = "EST"),
             flux_end = as_datetime(flux_end, tz = "EST")) %>%
      filter(!TIMESTAMP %in% slopes$TIMESTAMP)
    slopes_comb <- bind_rows(old_slopes, slopes)
  } else {
    slopes_comb <- slopes
    #Whenever we reprocess everything, save the raw output for QAQC efforts
    round_comb <- function(x){round(as.numeric(x), 2)}
    write.csv(data_small %>%
                mutate(across(c(CO2d_ppm), round_comb)),
              here::here("processed_data","raw_small.csv"), row.names = FALSE)
  }
  
  #Output
  write.csv(slopes_comb %>% select(-max_s), 
            here::here("processed_data","L0.csv"), 
            row.names = FALSE)
  
  return(slopes_comb)
}

#Create function to assign groups for separate readings
group_fun <- function(Fluxing_Chamber) {
  group <- rep(1, length(Fluxing_Chamber))
  for (i in 2:length(Fluxing_Chamber)) {
    if(Fluxing_Chamber[i] == Fluxing_Chamber[i - 1]) {
      group[i] <- group[i - 1]
    } else {
      group[i] <- group[i - 1] + 1
    }
  }
  return(group)
}
#calculate_flux(start_date = "2024-10-01", 
#               end_date = Sys.Date()+1,
#               modif_start_date = NULL,
#               reprocess = TRUE)