#' calculate_flux
#'
#' @description
#' This function calculates the raw CH4 fluxes for all files in the dropbox_downloads folder
#'
#' @param start_date earliest file to process (based on file name)
#' @param end_date latest file to process
#' @param reprocess whether to re-process slopes that have already been calculated
#'
#' @return L0 slopes

calculate_flux <- function(start_date = NULL,
                           end_date = NULL,
                           reprocess = F){
  ### Load files ###
  files <- autochamber::choose_files(
    input_folder = here::here("Raw_data", "dropbox_downloads"),
    l0_file_path = here::here("processed_data", "L0.csv"),
    reprocess = reprocess,
    start_date = start_date,
    end_date = end_date,
    files_to_exclude = c(
      "INSERT_FILENAME_HERE.csv" # If you want to remove any files
      # Insert file names here if they should be excluded
    ))
  
  if (length(files) == 0) {
    message("No files to process")
    return(read_csv(here::here("processed_data", "L0.csv"), show_col_types = F))
  }
  
  message(paste0("Calculating fluxes for ", length(files), " files"))
  
  #Load data
  data_small <- autochamber::load_loggernet_flux_data(
    files,
    format = "Chapada"
  ) |>
    dplyr::mutate(
      Diag = as.integer(dplyr::na_if(Diag, "NAN")),
      Chamber = as.integer(Chamber)
    ) |>
    mutate(TIMESTAMP = as_datetime(TIMESTAMP, tz = "EST")) %>%
    filter(!is.na(TIMESTAMP)) %>%
    distinct()
  
  slopes <- autochamber::calculate_flux(
    data_small,
    cutoff_start = 180,
    cutoff_end = 680,
    group_cols = "location"
  )
  
  if (!reprocess | !is.null(start_date)) {
    # Load previously calculated slopes
    old_slopes <- read_csv(here::here("processed_data", "L0.csv"),
                           show_col_types = F
    ) %>%
      mutate(
        TIMESTAMP = force_tz(TIMESTAMP, tz = "EST"),
        flux_start = force_tz(flux_start, tz = "EST"),
        flux_end = force_tz(flux_end, tz = "EST")
      ) %>%
      rename(Chamber = Fluxing_Chamber)
    #Combine
    slopes_comb <- autochamber::combine_slopes(old_slopes, slopes)
  } else {
    slopes_comb <- slopes
  }
  
  slopes_out <- autochamber::add_maintenance_log(
    slopes = slopes_comb,
    gs_url = "https://docs.google.com/spreadsheets/d/103PpjEmjLAQkov9ywjA5KxyJiIP3nEWy7V8gWVZhd1M/edit?gid=0#gid=0",
    group_cols = "location"
  ) %>%
    rename(Fluxing_Chamber = Chamber) #for compatibility downstream
  
  # Output
  write.csv(slopes_out %>%
              mutate(across(where(is.numeric), 
                            signif,
                            digits = 3)), # Trim file size
            here::here("processed_data", "L0.csv"),
            row.names = FALSE
  )
  
  recent_raw <- autochamber::generate_recent_raw(data_small,
                                                 group_cols = "location")
  
  write.csv(recent_raw,
            here::here("processed_data", "raw_for_dashboard.csv"),
            row.names = FALSE
  )
  
  # Export errors
  if("Diag" %in% colnames(data_small)) {
    data_errors <- data_small |>
      dplyr::select(TIMESTAMP, location, Chamber, Diag)
    
    # Load older data
    old_errors <- readr::read_csv(here::here("processed_data", "error_codes.csv"),
                                  show_col_types = F
    ) |>
      dplyr::mutate(
        TIMESTAMP = lubridate::force_tz(TIMESTAMP, tz = "EST"),
        Diag = as.integer(Diag),
        Chamber = as.integer(Chamber)) %>%
      dplyr::rename(Chamber = Fluxing_Chamber)
    
    #Combine
    errors_comb <- autochamber::combine_slopes(new = data_errors, old = old_errors)
    
    errors_small <- errors_comb |>
      dplyr::filter(lubridate::second(TIMESTAMP) == 0)
    
    write.csv(errors_small,
              here::here("processed_data", "error_codes.csv"),
              row.names = FALSE
    )
  }
  
  return(slopes_out)
}
