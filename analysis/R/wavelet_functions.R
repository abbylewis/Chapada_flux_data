#' Map over all chambers
#'
#' @param treatment Chamber name
#' @param ch4 Data frame
#' @param var_name Variable (i.e., driver name or CH4)
#' @param timestep_s Timestep of analysis
#'
#' @returns df with results for all chambers
#'
double_map <- function(treatment, ch4, var_name, timestep_s) {
  map(treatment,
    analyze_wavelets,
    ch4 = ch4,
    var_name = var_name,
    timestep_s = timestep_s
  ) %>%
    bind_rows()
}

#' Analyze wavelets
#'
#' @param ch4 Data frame
#' @param treatment Chamber name
#' @param var_name Variable (i.e., driver name or CH4)
#' @param timestep_s imestep of analysis
#'
#' @returns Data frame with wavelet results for this variable/chamber

analyze_wavelets <- function(ch4, treatment, var_name, timestep_s) {
  # Format data (dealing with irregularly spaced data)
  test_data <- ch4 %>%
    filter(Chamber == treatment) %>%
    filter(!is.na(!!sym(var_name)))

  # Run wavelet transformation
  data <- zoo::na.approx(test_data[[var_name]])
  wavelet <- waveslim::modwt(data, "la8", n.levels = 11)

  meshed <- data.frame(
    DateTime_EST = test_data$DateTime_EST,
    data,
    l_1 = wavelet[[1]],
    l_2 = wavelet[[2]],
    l_3 = wavelet[[3]],
    l_4 = wavelet[[4]],
    l_5 = wavelet[[5]],
    l_6 = wavelet[[6]],
    l_7 = wavelet[[7]],
    l_8 = wavelet[[8]],
    l_9 = wavelet[[9]],
    l_10 = wavelet[[10]],
    l_11 = wavelet[[11]]
  ) %>%
    pivot_longer(cols = -c(data, DateTime_EST)) %>%
    mutate(
      name = as.numeric(sub("l_", "", name)),
      name = factor(name,
        levels = 1:11,
        labels = parse_time(2^(1:11) * timestep_s)
      )
    )

  recon <- data.frame(
    DateTime_EST = test_data$DateTime_EST,
    data,
    l_1 = inverse_by_level(wavelet, 1),
    l_2 = inverse_by_level(wavelet, 2),
    l_3 = inverse_by_level(wavelet, 3),
    l_4 = inverse_by_level(wavelet, 4),
    l_5 = inverse_by_level(wavelet, 5),
    l_6 = inverse_by_level(wavelet, 6),
    l_7 = inverse_by_level(wavelet, 7),
    l_8 = inverse_by_level(wavelet, 8),
    l_9 = inverse_by_level(wavelet, 9),
    l_10 = inverse_by_level(wavelet, 10),
    l_11 = inverse_by_level(wavelet, 11)
  ) %>%
    pivot_longer(
      cols = -c(data, DateTime_EST),
      values_to = "reconstructed"
    ) %>%
    mutate(
      name = as.numeric(sub("l_", "", name)),
      name = factor(name,
        levels = 1:11,
        labels = parse_time(2^(1:11) * timestep_s)
      )
    )

  df <- meshed %>%
    left_join(recon, by = join_by(DateTime_EST, data, name)) %>%
    mutate(
      Chamber = treatment,
      var_name = var_name
    )

  return(df)
}

#' Inverse wavele transform by level
#'
#' @param wavelet Wavelet object
#' @param level Level to run inverse transformation
#'
#' @returns Reconstruction at this level
inverse_by_level <- function(wavelet, level) {
  wt_selected <- wavelet

  # Get number of levels from object
  detail_names <- grep("^d[0-9]+$", names(wt_selected), value = TRUE)

  for (name in detail_names) {
    l <- as.numeric(sub("d", "", name))

    if (l != level) {
      wt_selected[[name]] <- rep(0, length(wt_selected[[name]]))
    }
  }

  # Zero scaling coefficients
  s_name <- names(wt_selected)[!names(wt_selected) %in% detail_names]
  wt_selected[[s_name]] <- rep(0, length(wt_selected[[s_name]]))

  reconstructed <- imodwt(wt_selected)
  return(reconstructed)
}

#' Parse time
#'
#' @param s_list Vector of scales (seconds)
#'
#' @returns Scales parsed into interpretable text
#'
parse_time <- function(s_list) {
  out <- numeric(length(s_list))
  for (i in 1:length(s_list)) {
    if (s_list[i] < 1.5 * 86400) {
      out[i] <- paste(round(s_list[i] / 60 / 60), "hours")
    } else {
      out[i] <- paste(round(s_list[i] / 60 / 60 / 24), "days")
    }
  }
  return(out)
}
