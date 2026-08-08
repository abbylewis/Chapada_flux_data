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
    "\nThanks! -chapada bot"
  ))
}

# Check for licor errors
data <- read.csv(here::here("processed_data", "error_codes.csv"))
error_check <- data %>%
  filter((!is.na(Diag) & Diag > 0)) %>%
  filter(TIMESTAMP >= (force_tz(Sys.time(), "EST") - hours(24)))

if (nrow(error_check) > 0) {
  slackr::slackr_setup(token = Sys.getenv("SLACKRTOKEN"),
                       incoming_webhook_url = Sys.getenv("SLACKRURL"))
  
  unique_7810 <- error_check%>%
    select(Diag, location) %>%
    distinct()
  
  if("high" %in% unique_7810$location){
    unique_high <- unique_7810$Diag[unique_7810$location == "high"]
  } else {
    unique_high <- 0
  }
  if("low" %in% unique_7810$location){
    unique_low <- unique_7810$Diag[unique_7810$location == "low"]
  } else {
    unique_low <- 0
  }
  
  text <- ifelse(length(unique(unique_7810$location))>1, 
                 " both of the Chapada LI-7810s have ",
                 " one of the Chapada LI-7810s has ")
  
  slackr::slackr_msg(
    channel = "#chapada_stem",
    username = "Chapada QAQC bot",
    txt = paste0(
    "Hi team- it looks like", text, "been showing error codes. \n",
    "Codes today (high): ", paste(unique_high, collapse = ", "), "\n",
    "Codes today (low): ", paste(unique_low, collapse = ", "), "\n",
    "You can visualize when the errors happened on the dashboard:\n",
    "https://aslewis.shinyapps.io/chapada_dashboard/",
    "\nThanks! -chapada bot"
  ))
}
