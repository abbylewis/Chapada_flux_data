###############################################
# CO2 flux partitioning: NEE → GPP + Reco
###############################################

# Load packages
library(tidyverse)
library(data.table)
source("R/download_met.R")
source("R/download_bme.R")
source("R/download_redox.R")
source("R/download_teros.R")
Sys.setenv(TZ = "EST")

# Load slopes
target <- read_csv("processed_data/L0.csv", show_col_types = F) %>%
  mutate(flux_start = force_tz(flux_start, tzone = "EST"),
         flux_end = force_tz(flux_end, tzone = "EST"),
         TIMESTAMP = force_tz(TIMESTAMP, tzone = "EST"),
         CH4_se = log(CH4_se),
         CO2_se = log(CO2_se)) %>%
  #rename(Chamber = chamber) %>%
  rename(flux_time = TIMESTAMP) %>%
  filter(!duplicated(flux_time))
# Update all files
download_met()
download_redox()
download_teros()
download_bme()
# Load met
met <- read_csv("processed_data/met_2025_dashboard.csv") %>%
  mutate(
    TIMESTAMP = force_tz(TIMESTAMP, tzone = "EST"),
    temp_time = TIMESTAMP) %>%
  filter(!is.na(TIMESTAMP),
         !duplicated(TIMESTAMP)) %>%
  select(-TIMESTAMP)

teros <- read_csv("processed_data/teros_2025_dashboard.csv") %>%
  mutate(
    TIMESTAMP = force_tz(TIMESTAMP, tzone = "EST"),
    teros_time = TIMESTAMP,
    chamber = paste0(location, chamber*10)) %>%
  filter(!var == "air_temperature") %>%
  pivot_wider(names_from = var, values_from = value) %>%
  filter(!is.na(TIMESTAMP)) %>%
  select(-TIMESTAMP, -location) #location is encoded in chamber

teros_clean <- teros %>%
  group_by(chamber) %>%
  arrange(teros_time) %>%
  mutate(
    volumetric_water_content = if_else(volumetric_water_content < 0,
                                       NA_real_, volumetric_water_content,
                                       missing = NA_real_),
    rolling_sd = RcppRoll::roll_sd(
      volumetric_water_content, 
      weights = c(rep(1/10, 5), 0, rep(1/10, 5)), normalize = F,
      fill = NA),
    rolling_mean = RcppRoll::roll_mean(
      volumetric_water_content, 
      weights = c(rep(1/10, 5), 0, rep(1/10, 5)), normalize = TRUE,
      fill = NA),
    outlier = abs(volumetric_water_content - rolling_mean) > 3 * rolling_sd,
    volumetric_water_content = if_else(outlier, NA_real_, volumetric_water_content)
  )

#QAQC
filt <- target %>%
  ungroup() %>%
  arrange(flux_time) %>%
  dplyr::mutate(
    #Can't have negative ebullition
    CH4_slope_ppm_per_day = ifelse(!is.na(ebullition) & 
                                     ebullition &
                                     CH4_slope_ppm_per_day_ebullition < 0,
                                   NA,
                                   CH4_slope_ppm_per_day),
  )

#Visualize
#filt %>%
#  filter(!ebullition) %>%
#  ggplot(aes(x = flux_time, y = CH4_slope_ppm_per_day)) +
#  geom_line() +
#  theme_minimal() +
#  facet_wrap(~chamber)

#### Add ebullition ####
df <- filt %>%
  mutate(
    # Some were removed intentionally
    CH4_slope_ppm_per_day_ebullition = 
      ifelse(is.na(CH4_slope_ppm_per_day),
             NA,
             CH4_slope_ppm_per_day_ebullition),
    CH4_slope_ppm_per_day = ifelse(!is.na(ebullition) & ebullition == T,
                                   CH4_slope_ppm_per_day_ebullition, 
                                   CH4_slope_ppm_per_day),
  )

# Format
df$DateTime <- as.POSIXct(df$flux_time, tz = "EST")
met$DateTime <- as.POSIXct(met$temp_time, tz = "EST")
teros_clean$DateTime <- as.POSIXct(teros_clean$teros_time, tz = "EST")

# Convert to data.table
setDT(df) 
setDT(met)
setDT(teros_clean)

# Set keys: DateTime is what will be used to join fluxes with met
setkey(df, DateTime)
setkey(met, DateTime)

# Match meteorological drivers by nearest time
merged <- met[
  df,
  on = .(DateTime),
  roll = "nearest"
]

# Time difference between flux and matched met
merged[, met_time_diff := abs(temp_time - flux_time)]

merged[met_time_diff > 30 * 60, # 30 minute window
       c("AirTC_Avg", "PAR_Den_C_Avg", "Depth_cm") := NA]

#Join and format
chamber_height_high = 100 # cm
chamber_height_low = 150 # cm
chamber_radius = 45/2 # cm
chamber_area = pi*(chamber_radius/100)^2 # m2
chamber_volume_high = chamber_height_high/100 * # m
  chamber_area * 1000 #L
chamber_volume_low = chamber_height_low/100 * # m
  chamber_area * 1000 #L

merged <- merged %>% 
  rename(Ta = AirT_C_Avg,
         PAR = SlrFD_W_Avg,
         Ebullition_yn = ebullition) %>%
  mutate(chamber_volume = ifelse(location == "high",
                                 chamber_volume_high,
                                 chamber_volume_low),
         NEE = CO2_slope_ppm_per_day * #CONVERT TO umolCO2/m2/s
           chamber_volume / (0.08206 * (Ta + 273.15)) / (60 * 60 * 24) / chamber_area,
         CH4 = CH4_slope_ppm_per_day * #CONVERT TO umolCH4/m2/s
           chamber_volume / (0.08206 * (Ta + 273.15)) / (60 * 60 * 24) / chamber_area,
         chamber = paste0(location, Fluxing_Chamber)) %>% 
  filter(!is.na(location),
         year(DateTime) >= 2025) %>%
  ungroup() %>%
  select(chamber, DateTime, flux_time, NEE, CH4, PAR, Ta, CH4_R2, CO2_R2, CH4_se, CO2_se, Ebullition_yn)

merged %>%
  group_by(chamber) %>%
  summarize(n = sum(is.na(CH4)))

# Identify nighttime
par_night_thresh <- 5  # µmol m-2 s-1 threshold to define night
merged[, is_night := PAR < par_night_thresh]

# For each chamber, fit Q10 using nighttime points
# We'll fit the log-linear Q10 via lm on log(NEE) with NEE>0 (since Reco positive release).
# Model: log(Reco) = a + b*(Ta - Tref); where b = ln(Q10)/10. We'll use Tref = 10°C.

# helper function to fit Q10 (log-linear)
fit_q10_lm <- function(dt_night, Tref = 10, min_night = 40) {
  # dt_night: data.table with columns NEE, Ta; NEE must be > 0
  dt_night <- dt_night[
    is.finite(NEE) &
      NEE > 0 &
      is.finite(Ta)
  ]
  
  if (nrow(dt_night) < min_night) {
    return(NULL)
  }
  
  X <- dt_night[, Ta - Tref]
  Y <- log(dt_night$NEE)
  fit <- try(lm(Y ~ X), silent = TRUE)
  if (inherits(fit, "try-error")) {
    return(NULL)
  }
  coef <- coefficients(fit)
  a <- coef[1]
  b <- coef[2]
  Rref <- exp(a)
  Q10 <- exp(b * 10)
  return(list(Rref = as.numeric(Rref), Q10 = as.numeric(Q10), n = nrow(dt_night), fit = fit))
}

# Function: moving-window parameter estimation per chamber
estimate_params_moving_window <- function(
    dt_ch, window_days = 100, step_days = 1,
    par_night_thresh = 5, Tref = 10
) {
  # dt_ch: data.table for one chamber
  if (nrow(dt_ch) == 0) {
    return(NULL)
  }
  start_time <- min(dt_ch$DateTime, na.rm = TRUE)
  end_time <- max(dt_ch$DateTime, na.rm = TRUE)
  centers <- seq(from = start_time, to = end_time, by = paste0(step_days, " days"))
  res_list <- vector("list", length(centers))
  for (i in seq_along(centers)) {
    center <- centers[i]
    wstart <- center - as.difftime(window_days / 2, units = "days")
    wend <- center + as.difftime(window_days / 2, units = "days")
    wnd <- dt_ch[DateTime >= wstart & DateTime <= wend]
    # nighttime points (PAR-based)
    wnd_night <- wnd[PAR < par_night_thresh & is.finite(NEE) & NEE > 0 & is.finite(Ta)]
    fit <- fit_q10_lm(wnd_night, Tref = Tref)
    if (!is.null(fit)) {
      res_list[[i]] <- data.table(
        chamber = dt_ch$chamber[1],
        center = center,
        Rref = fit$Rref,
        Q10 = fit$Q10,
        n_night = fit$n
      )
    } else {
      res_list[[i]] <- data.table(
        chamber = dt_ch$chamber[1],
        center = center,
        Rref = NA_real_,
        Q10 = NA_real_,
        n_night = nrow(wnd_night)
      )
    }
  }
  res_dt <- rbindlist(res_list)
  # drop centers with NA Rref & Q10? Keep for interpolation (will be NA)
  return(res_dt)
}

chambers <- unique(merged$chamber)
params_all <- list()

for (ch in chambers) {
  dt_ch <- merged[chamber == ch]
  params_ch <- estimate_params_moving_window(dt_ch)
  params_all[[as.character(ch)]] <- params_ch
}
params_dt <- rbindlist(params_all, use.names = TRUE, fill = TRUE)

# Remove rows where center is NA (if any)
params_dt <- params_dt[!is.na(center)]


# Interpolate Rref & Q10 to every flux timestamp
# For each chamber, use linear interpolation of Rref and Q10 over time.
# For timestamps outside params range, use nearest available (rule = 2 in approx -> constant extrapolate)
merged[, Rref_t := NA_real_]
merged[, Q10_t := NA_real_]

make_grid <- function(g) {
  data.table(
    DateTime = seq(min(g$DateTime),
                   max(g$DateTime),
                   by = "6750 sec"
    ),
    chamber = unique(g$chamber)
  )
}

grid <- merged[, make_grid(.SD), by = chamber]

setkey(merged, chamber, DateTime)
setkey(grid, chamber, DateTime)
setkey(teros_clean, chamber, DateTime)


merged_grid <- merged[grid, roll = "nearest"] #grab nearest observation
#has to be within 65 min
merged_grid[, time_diff := abs(DateTime - flux_time)]
cols <- setdiff(names(merged_grid), c("DateTime", "chamber"))
merged_grid[time_diff > (6750/2), (cols) := NA]
merged_grid[, c("Ta", "PAR") := NULL]

# Match meteorological drivers by nearest time
merged_grid_met <- met[
  merged_grid,
  on = .(DateTime),
  roll = "nearest"
]
merged_grid_final <- teros_clean[
  merged_grid_met,
  on = .(chamber, DateTime),
  roll = "nearest"
]

setnames(
  merged_grid_final,
  old = c("AirT_C_Avg", "SlrFD_W_Avg"),
  new = c("Ta", "PAR")
)

merged_grid_final[
  abs(temp_time - DateTime) > 2 * 60 * 60, # air temp and PAR need to be within 2hr
  (c("Ta", "PAR")) := NA_real_
]

merged_grid_final[
  abs(teros_time - DateTime) > 24 * 60 * 60, # VWC and EC need to be within 1d
  (c("electrical_conductivity", "volumetric_water_content")) := NA_real_
]

for (ch in chambers) {
  pch <- params_dt[chamber == ch & !is.na(Rref) & !is.na(Q10)][order(center)]
  if (nrow(pch) == 0) next
  # ensure unique centers
  pch <- unique(pch, by = "center")
  x <- as.numeric(pch$center) # seconds since epoch
  yR <- pch$Rref
  yQ <- pch$Q10
  targ_idx <- which(merged_grid_final$chamber == ch)
  xt <- as.numeric(merged_grid_final$DateTime[targ_idx])
  # approx with rule=2: use nearest outside range
  Rinterp <- approx(x = x, y = yR, xout = xt, rule = 2, ties = "ordered")$y
  Qinterp <- approx(x = x, y = yQ, xout = xt, rule = 2, ties = "ordered")$y
  merged_grid_final[targ_idx, Rref_t := Rinterp]
  merged_grid_final[targ_idx, Q10_t := Qinterp]
}

# Predict Reco using time-varying parameters
# Reco = Rref_t * Q10_t ^ ((Ta - Tref)/10)
merged_grid_final[, Reco := NA_real_]
Tref <- 10
merged_grid_final[!is.na(Rref_t) & !is.na(Q10_t) & !is.na(Ta),
                  Reco := Rref_t * (Q10_t^((Ta - Tref) / 10))]

# Compute daytime GPP = Reco - NEE
merged_grid_final[, is_day := PAR >= par_night_thresh]
merged_grid_final[, GPP := NA_real_]
day_mask <- merged_grid_final$is_day & is.finite(merged_grid_final$Reco) & is.finite(merged_grid_final$NEE)
merged_grid_final[day_mask, GPP := Reco - NEE]
# enforce non-negative GPP
merged_grid_final[day_mask & GPP < 0, GPP := 0]
merged_grid_final[is.na(NEE), GPP := NA]
# merged_grid_final[is.na(NEE), Reco := NA]

merged_export <- merged_grid_final %>%
  mutate(Fluxing_Chamber = str_extract(chamber, "[0-9]+"),
         location = str_extract(chamber, "[a-z]+")) %>%
  select(-chamber)

write_csv(merged_export, "processed_data/partitioned_co2.csv")

