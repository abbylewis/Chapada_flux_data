#' Calculate bias of manual sampling
#'
#' @param interval Time interval for sampling
#'
#' @returns Data frame with metrics describing the bias of manual sampling
#'
calc_bias <- function(interval, only_time = T) {
  # Parse interval name
  samples_per_bin <- case_when(grepl("twice", interval) ~ 2,
    .default = 1
  )
  bin_dur <- str_extract(interval, "daily|week|month|fortnight")
  bin_dur <- ifelse(bin_dur == "daily", "day", bin_dur)
  message(paste0("Running ", interval, ": \nBin: ", bin_dur, "\nSamples per bin: ", samples_per_bin))

  # Run simulation
  out_all <- df %>%
    group_by(get(bin_dur), Chamber) %>%
    reframe(
      n = n(),
      rep = seq_len(reps),
      # For every replicate, choose a flux at random and calculate the mean
      flux = replicate(
        reps,
        mean(sample(CH4_umol_m2_h, samples_per_bin))
      ),
      interval = interval
    ) %>%
    mutate(time = "all")

  if(!only_time){
    out_daytime <- df %>%
      filter(hour(DateTime_local) %in% c(9:17)) %>%
      group_by(get(bin_dur), Chamber) %>%
      reframe(
        n = n(),
        rep = seq_len(reps),
        flux = replicate(
          reps,
          mean(sample(CH4_umol_m2_h, samples_per_bin))
        ),
        interval = interval
      ) %>%
      mutate(time = "daytime")
    
    out_wl <- df %>%
      filter(Depth_cm <= 1) %>%
      group_by(get(bin_dur), Chamber) %>%
      reframe(
        n = n(),
        rep = seq_len(reps),
        flux = replicate(
          reps,
          mean(sample(CH4_umol_m2_h, samples_per_bin))
        ),
        interval = interval
      ) %>%
      mutate(time = "waterlevel")
    
    out <- bind_rows(out_all, out_daytime, out_wl)
    
    return(out)
  }

  return(out_all)
}
