install.packages("rsconnect")
install.packages("here")
install.packages("slackr")
library(tidyverse)

# Check for errors
data <- read_csv(here::here("processed_data", "L0.csv"))
error_check <- data %>%
  mutate(TIMESTAMP = force_tz(TIMESTAMP, "EST")) %>%
  filter(!is.na(flux_start)) %>%
  group_by(Fluxing_Chamber, location) %>%
  filter(TIMESTAMP > (force_tz(Sys.time(), "EST") - hours(24))) %>%
  summarize(
    last_timestamp = max(TIMESTAMP),
    r2_check_ch4 = median(CH4_R2, na.rm = T),
    r2_check_co2 = median(CO2_R2, na.rm = T)
  ) %>%
  filter(r2_check_ch4 < 0.7 & r2_check_co2 < 0.7) %>%
  mutate(label = paste0(Fluxing_Chamber, " (", location, ")"))

if (nrow(error_check) > 0) {
  slackr::slackr_setup(token = Sys.getenv("SLACKRTOKEN"),
                       incoming_webhook_url = Sys.getenv("SLACKRURL"))
  slackr::slackr_msg(
    channel = "#chapada_stem",
    username = "Chapada QAQC bot",
    txt = paste0(
    "Hi team! I noticed that CO2 and CH4 R2 values have been low recently for the following chamber(s):\n",
    paste(error_check$label, collapse = ", "),
    "\nYou might want to take a quick look at the dashboard and make sure things look okay:\n",
    "https://aslewis.shinyapps.io/chapada_dashboard/",
    "\nThanks! -Chapada bot"
  ))
}

# Check for licor errors
diag_dict <- c(
  `0`   = "Normal operation (measuring)",
  `1`   = "Start frequency adjustment; measurements may be noisy",
  `2`   = "Laser temperature adjustment; measurements may be noisy",
  `4`   = "Incomplete scan resulted in missing cavity modes; measurements may be noisy",
  `8`   = "Start-up mode finished; measurements may be noisy",
  `16`  = "Start-up mode initializing; measurements may be noisy",
  `32`  = "Spectral fit residual RMS too high; measurements are invalid",
  `64`  = "Unregulated pressures or temperatures; measurements are invalid",
  `128` = "Inlet clogged; instrument enters sleep mode",
  `256` = "Instrument not ready; measurements are invalid"
)

decode_diag <- function(x, dict) {
  if (is.na(x)) {
    return("Missing data")
  }
  
  if (x == 0) {
    return("0 (Normal operation)")
  }
  
  codes <- as.numeric(names(dict))
  codes <- codes[codes > 0]
  
  matched <- codes[bitwAnd(as.integer(x), codes) == codes]
  matched <- matched[rev(order(matched))]
  
  paste0(
    matched, " (", unname(dict[as.character(matched)]), ")",
    collapse = " and "
  )
}

data <- read_csv(here::here("processed_data", "error_codes.csv")) %>%
  mutate(TIMESTAMP = with_tz(TIMESTAMP, tzone = "EST"))
error_check <- data %>%
  filter(TIMESTAMP >= (force_tz(Sys.time(), "EST") - hours(24))) %>%
  select(Diag, location) %>%
  distinct() %>%
  arrange(-Diag)

if (sum(error_check$Diag[!is.na(error_check$Diag)]) > 0) {
  slackr::slackr_setup(token = Sys.getenv("SLACKRTOKEN"),
                       incoming_webhook_url = Sys.getenv("SLACKRURL"))
  
  unique_7810 <- error_check 
  
  if ("high" %in% unique_7810$location) {
    unique_high <- unique_7810$Diag[unique_7810$location == "high"]
  } else {
    unique_high <- 0
  }
  
  if ("low" %in% unique_7810$location) {
    unique_low <- unique_7810$Diag[unique_7810$location == "low"]
  } else {
    unique_low <- 0
  }
  
  high_text <- paste(
    paste0("*   ",unique_high, ": ", vapply(unique_high, decode_diag, character(1), dict = diag_dict)),
    collapse = "\n"
  )
  
  low_text <- paste(
    paste0("*   ",unique_low, ": ", vapply(unique_low, decode_diag, character(1), dict = diag_dict)),
    collapse = "\n"
  )
  
  text <- ifelse(
    length(unique(unique_7810$location[unique_7810$Diag > 0])) > 1,
    " both of the Chapada LI-7810s have ",
    " one of the Chapada LI-7810s has "
  )
  
  slackr::slackr_msg(
    channel = "#chapada_stem",
    username = "Chapada QAQC bot",
    txt = paste0(
      "Hi team- it looks like", text, "been showing error codes. \n\n",
      "Codes today (high):\n", high_text, "\n",
      "Codes today (low):\n", low_text, "\n",
      "You can visualize when the errors happened on the dashboard:\n",
      "https://aslewis.shinyapps.io/chapada_dashboard/",
      "\n\nThanks! -Chapada bot"
    )
  )
}
