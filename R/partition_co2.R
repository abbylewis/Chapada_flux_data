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

# Load slopes
df <- read_csv("processed_data/L0.csv", show_col_types = F)
# Update all files
download_met()
download_redox()
download_teros()
download_bme()
# Load met
met <- read_csv("processed_data/met_2025_dashboard.csv")

# Format
df$DateTime <- as.POSIXct(df$TIMESTAMP, tz = "EST")
met$DateTime <- as.POSIXct(met$TIMESTAMP, tz = "EST")

# Convert to data.table
setDT(df) 
setDT(met)

# Set keys: DateTime is what will be used to join fluxes with met
setkey(df, DateTime)
setkey(met, DateTime)

#Join and format
merged <- met[df, roll = "nearest"] %>% # Rolling join: nearest met to each flux
  rename(Ta = AirT_C_Avg,
         PAR = SlrFD_W_Avg) %>%
  mutate(NEE = CO2_slope_ppm_per_day * #CONVERT TO umolCO2/m2/s
           265.8 / (0.08206*(Ta + 273.15)) / (60*60*24) / 0.196,
         CH4 = CH4_slope_ppm_per_day * #CONVERT TO umolCH4/m2/s
           265.8 / (0.08206*(Ta + 273.15)) / (60*60*24) / 0.196,
         chamber = paste0(location, Fluxing_Chamber)) %>% 
  filter(!is.na(location),
         year(DateTime) >= 2025) %>%
  ungroup() %>%
  select(chamber, DateTime, NEE, CH4, PAR, Ta)

# Identify nighttime
par_night_thresh <- 5  # µmol m-2 s-1 threshold to define night
merged[, is_night := PAR < par_night_thresh]

# For each chamber, fit Q10 using nighttime points
# We'll fit the log-linear Q10 via lm on log(NEE) with NEE>0 (since Reco positive release).
# Model: log(Reco) = a + b*(Ta - Tref); where b = ln(Q10)/10. We'll use Tref = 10°C.

# helper function to fit Q10 (log-linear)
fit_q10_lm <- function(dt_night, Tref = 10, min_night = 5) {
  # dt_night: data.table with columns NEE, Ta; NEE must be > 0
  if (nrow(dt_night) < min_night) return(NULL)
  dt_night <- dt_night[NEE > 0 & is.finite(Ta)]
  if (nrow(dt_night) < min_night) return(NULL)
  X <- dt_night[, Ta - Tref]
  Y <- log(dt_night$NEE)
  fit <- try(lm(Y ~ X), silent = TRUE)
  if (inherits(fit, "try-error")) return(NULL)
  coef <- coefficients(fit)
  a <- coef[1]; b <- coef[2]
  Rref <- exp(a)
  Q10 <- exp(b * 10)
  return(list(Rref = as.numeric(Rref), Q10 = as.numeric(Q10), n = nrow(dt_night), fit = fit))
}

# Function: moving-window parameter estimation per chamber
estimate_params_moving_window <- function(
    dt_ch, window_days = 30, step_days = 1, 
    par_night_thresh = 5, Tref = 10) {
  # dt_ch: data.table for one chamber
  if (nrow(dt_ch) == 0) return(NULL)
  start_time <- min(dt_ch$DateTime, na.rm = TRUE)
  end_time   <- max(dt_ch$DateTime, na.rm = TRUE)
  centers <- seq(from = start_time, to = end_time, by = paste0(step_days, " days"))
  res_list <- vector("list", length(centers))
  for (i in seq_along(centers)) {
    center <- centers[i]
    wstart <- center - as.difftime(window_days/2, units = "days")
    wend   <- center + as.difftime(window_days/2, units = "days")
    wnd <- dt_ch[DateTime >= wstart & DateTime <= wend]
    # nighttime points (PAR-based)
    wnd_night <- wnd[PAR < par_night_thresh & is.finite(NEE) & NEE > 0 & is.finite(Ta)]
    fit <- fit_q10_lm(wnd_night, Tref = Tref)
    if (!is.null(fit)) {
      res_list[[i]] <- data.table(chamber = dt_ch$chamber[1],
                                  center = center,
                                  Rref = fit$Rref,
                                  Q10 = fit$Q10,
                                  n_night = fit$n)
    } else {
      res_list[[i]] <- data.table(chamber = dt_ch$chamber[1],
                                  center = center,
                                  Rref = NA_real_,
                                  Q10 = NA_real_,
                                  n_night = ifelse(is.null(wnd_night), 0, nrow(wnd_night)))
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

for (ch in chambers) {
  pch <- params_dt[chamber == ch & !is.na(Rref) & !is.na(Q10)]
  if (nrow(pch) == 0) next
  # ensure unique centers
  pch <- unique(pch, by = "center")
  x <- as.numeric(pch$center)  # seconds since epoch
  yR <- pch$Rref
  yQ <- pch$Q10
  targ_idx <- which(merged$chamber == ch)
  xt <- as.numeric(merged$DateTime[targ_idx])
  # approx with rule=2: use nearest outside range
  Rinterp <- approx(x = x, y = yR, xout = xt, rule = 2, ties = "ordered")$y
  Qinterp <- approx(x = x, y = yQ, xout = xt, rule = 2, ties = "ordered")$y
  merged[targ_idx, Rref_t := Rinterp]
  merged[targ_idx, Q10_t  := Qinterp]
}

# Predict Reco using time-varying parameters
# Reco = Rref_t * Q10_t ^ ((Ta - Tref)/10)
merged[, Reco := NA_real_]
Tref = 10
ok_mask <- is.finite(merged$Rref_t) & is.finite(merged$Q10_t) & is.finite(merged$Ta)
merged[ok_mask, Reco := Rref_t * (Q10_t ^ ((Ta[ok_mask] - Tref)/10))]

# Compute daytime GPP = Reco - NEE
merged[, is_day := PAR >= par_night_thresh]
merged[, GPP := NA_real_]
day_mask <- merged$is_day & is.finite(merged$Reco) & is.finite(merged$NEE)
merged[day_mask, GPP := Reco - NEE]
# enforce non-negative GPP if desired
merged[day_mask & GPP < 0, GPP := 0]

#Parse chamber names
merged <- merged %>%
  mutate(Fluxing_Chamber = str_extract(merged$chamber, "[0-9]+"),
         location = str_extract(merged$chamber, "[a-z]+")) %>%
  select(-chamber)

write_csv(merged, "processed_data/partitioned_co2.csv")

