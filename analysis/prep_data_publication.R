### PREP DATA PUBLICATION ###

# Load packages and data
library(tidyverse)

flux_formatted <- read_csv(here::here("processed_data", "merged.csv")) %>%
  rename(AirTemp_C = Ta,
         SlrFD_W_Avg = PAR) %>%
  filter(!is.na(DateTime)) %>%
  mutate(DateTime_BRT = with_tz(DateTime, "Etc/GMT+3"),
         Site = toupper(str_extract(chamber, "high|low")),
         Chamber_number = str_extract(chamber, "[0-9]+"),
         Chamber = paste0(Site, " - ", Chamber_number),
         CH4_umol_m2_h = CH4 * (60 * 60),
         CO2_umol_m2_h = NEE * (60 * 60)) %>%
  filter(year(DateTime_BRT) == 2025) %>%
  mutate(DateTime_BRT = format(DateTime_BRT, "%Y-%m-%d %H:%M:%S"),
         CO2_R2 = if_else(is.na(CO2_umol_m2_h),
                          NA,
                          CO2_R2),
         CH4_R2 = if_else(is.na(CH4_umol_m2_h),
                          NA,
                          CH4_R2),
         CO2_se = if_else(is.na(CO2_umol_m2_h),
                          NA,
                          CH4_R2),
         CH4_se = if_else(is.na(CH4_umol_m2_h),
                          NA,
                          CH4_se)) %>%
  select(Chamber, DateTime_BRT, 
         CO2_umol_m2_h, CO2_R2, CO2_se,
         CH4_umol_m2_h, CH4_R2, CH4_se,
         SlrFD_W_Avg, AirTemp_C
         )

write_csv(flux_formatted, here::here("processed_data", "data_pub_2025.csv"))
