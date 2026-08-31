### GAP FILL GPP AND CH4 ###

# Load packages and data
library(tidyverse)
library(data.table)
library(randomForest)

flux_reg <- read_csv(here::here("processed_data", "partitioned_co2.csv")) %>%
  rename(TIMESTAMP = DateTime) %>%
  filter(!is.na(TIMESTAMP)) %>%
  mutate(TIMESTAMP = with_tz(TIMESTAMP, "EST"))

#evi <- read_csv(here::here("processed_data", "evi.csv")) %>%
#  filter(!duplicated(Date))

setDT(flux_reg)
setkey(flux_reg, location, Fluxing_Chamber, TIMESTAMP)

# Consistent timestamp
flux_reg <- flux_reg %>%
  mutate(
    #time_join = round_date(TIMESTAMP, "15 minutes"),
    date_join = as_date(TIMESTAMP)
  )

ch4 <- flux_reg %>%
  #left_join(met %>% rename(time_join = TIMESTAMP), by = "time_join") %>%
  #left_join(evi %>% rename(date_join = Date), by = "date_join") %>%
  mutate(
    is_day = case_when(
      !is.na(is_day) ~ is_day,
      !is.na(PAR) & PAR >= 5 ~ TRUE,
      !is.na(PAR) & PAR < 5 ~ FALSE,
      TRUE ~ NA
    ),
    yday = yday(date_join)) %>%
  arrange(location, Fluxing_Chamber) 

# Confirm CH4 looks reasonable
ch4 %>%
  ggplot(aes(x = TIMESTAMP, y = CH4)) +
  geom_point() +
  geom_smooth() +
  facet_grid(location~Fluxing_Chamber)

# Check GPP params
ch4 %>%
  ggplot(aes(x = TIMESTAMP, y = Rref_t)) +
  geom_point() +
  geom_smooth() +
  facet_grid(location~Fluxing_Chamber)

ch4 %>%
  ggplot(aes(x = TIMESTAMP, y = Q10_t)) +
  geom_point() +
  geom_smooth() +
  facet_grid(location~Fluxing_Chamber)

# How many are missing?

look <- ch4 %>%
  group_by(location, Fluxing_Chamber) %>%
  summarize(
    gpp_nas = sum(is.na(GPP)[!is_night], na.rm = T),
    gpp_pct = gpp_nas / sum(!is.na(is_night) & !is_night) * 100,
    reco_nas = sum(is.na(Reco)),
    reco_pct = reco_nas / n() * 100,
    ch4_nas = sum(is.na(CH4)),
    ch4_pct = ch4_nas / n() * 100
  )


### Gap fill ###

## GPP

train <- ch4[is_day == TRUE & !is.na(GPP)]

rf_gpp_models <- lapply(split(train, ~train$location + train$Fluxing_Chamber), function(dt_ch) {
  randomForest(
    GPP ~ Ta + PAR + yday + Reco,
    data = dt_ch,
    na.action = na.omit,
    ntree = 500
  )
})

oob_metrics <- lapply(rf_gpp_models, function(model) {
  data.frame(
    OOB_MSE = tail(model$mse, 1),
    OOB_R2 = tail(model$rsq, 1)
  )
})

oob_metrics <- do.call(rbind, oob_metrics)
oob_metrics$ID <- names(rf_gpp_models)
oob_metrics

for (id in names(rf_gpp_models)) {
  loc <- sub("\\.[0-9]+","", id)
  ch <- sub("[a-z]+\\.","", id)
  model <- rf_gpp_models[[id]]
  idx <- ch4$location == loc & ch4$Fluxing_Chamber == ch
  ch4[idx, GPP_rf := predict(model, ch4[idx])]
}

ch4 %>%
  ggplot(aes(x = TIMESTAMP, y = GPP)) +
  geom_point() +
  geom_point(aes(y = GPP_rf), color = "red") +
  facet_grid(location~Fluxing_Chamber)

ch4[, GPP_filled := GPP]
ch4[is.na(GPP_filled) & is_day == TRUE, GPP_filled := GPP_rf]
ch4[is_day == FALSE, GPP_filled := 0]

ch4 %>%
  group_by(location, Fluxing_Chamber) %>%
  summarize(nas = sum(is.na(GPP_filled)))

ch4 %>%
  group_by(location, Fluxing_Chamber) %>%
  summarize(nas = sum(is.na(Reco)))

ch4 %>%
  ggplot(aes(x = TIMESTAMP, y = Reco))+
  geom_line()+
  facet_grid(location~Fluxing_Chamber)

### CH4 ###

train <- ch4[!is.na(CH4)]

rf_ch4_models <- lapply(split(train, ~train$location + train$Fluxing_Chamber), function(dt_ch) {
  randomForest(
    CH4 ~ Ta + PAR + GPP_filled + Reco + yday,
    data = dt_ch,
    na.action = na.omit,
    ntree = 500
  )
})

for (id in names(rf_ch4_models)) {
  loc <- sub("\\.[0-9]+","", id)
  ch <- sub("[a-z]+\\.","", id)
  model <- rf_ch4_models[[id]]
  idx <- ch4$location == loc & ch4$Fluxing_Chamber == ch
  ch4[idx, CH4_rf := predict(model, ch4[idx])]
}

oob_metrics <- lapply(rf_ch4_models, function(model) {
  data.frame(
    OOB_MSE = tail(model$mse, 1),
    OOB_R2 = tail(model$rsq, 1)
  )
})

oob_metrics <- do.call(rbind, oob_metrics)
oob_metrics$ID <- names(rf_ch4_models)
oob_metrics

ch4 %>%
  ggplot(aes(x = TIMESTAMP, y = CH4)) +
  geom_point() +
  geom_point(aes(y = CH4_rf), color = "red") +
  facet_grid(location~Fluxing_Chamber)

ch4 %>%
  #filter(MIU_VALVE == 8) %>%
  ggplot(aes(x = CH4, y = CH4_rf, color = hour(TIMESTAMP))) +
  geom_point() +
  geom_abline(slope = 1) +
  facet_grid(location~Fluxing_Chamber)

### BIAS CORRECTION

# empirical distribution matching function
edm_correct <- function(obs, pred, pred_all = pred) {
  
  # keep only complete pairs for fitting the mapping
  ok <- complete.cases(obs, pred)
  obs_fit <- obs[ok]
  pred_fit <- pred[ok]
  
  if (length(obs_fit) < 10) {
    return(pred_all)
  }
  
  # empirical CDF of predictions
  F_pred <- ecdf(pred_fit)
  
  # quantiles of observed values
  p <- F_pred(pred_all)
  
  # map predicted quantiles to observed quantiles
  q_obs <- quantile(
    obs_fit,
    probs = p,
    na.rm = TRUE,
    type = 8
  )
  
  as.numeric(q_obs)
}

ch4[, CH4_rf_edm := NA_real_]

for (id in names(rf_ch4_models)) {
  loc <- sub("\\.[0-9]+","", id)
  ch <- sub("[a-z]+\\.","", id)
  
  idx_all <- ch4$location == loc & ch4$Fluxing_Chamber == ch
  idx_train <- idx_all & !is.na(ch4$CH4)
  
  ch4[idx_all, CH4_rf_edm :=
        edm_correct(
          obs = ch4[idx_train, CH4],
          pred = ch4[idx_train, CH4_rf],
          pred_all = ch4[idx_all, CH4_rf]
        )
  ]
}

ch4 %>%
  ggplot(aes(x = CH4)) +
  geom_point(aes(y = CH4_rf), colour = "red", alpha = 0.5) +
  geom_point(aes(y = CH4_rf_edm), colour = "blue", alpha = 0.5) +
  geom_abline(slope = 1) +
  facet_grid(location~Fluxing_Chamber)

ch4[, CH4_filled := CH4]
ch4[is.na(CH4), CH4_filled := CH4_rf_edm]

ch4 %>%
  group_by(location, Fluxing_Chamber) %>%
  summarize(nas = sum(is.na(CH4_filled)))

ch4 %>%
  ggplot(aes(x = TIMESTAMP)) +
  geom_point(aes(y = CH4_rf_edm), color = "red")+
  geom_point(aes(y = CH4), shape = 21) +
  facet_grid(location~Fluxing_Chamber)

ch4 %>%
  ggplot(aes(x = TIMESTAMP)) +
  geom_point(aes(y = CH4_rf_edm), color = "red")+
  facet_grid(location~Fluxing_Chamber)

ch4_out <- ch4 %>%
  rename(DateTime = TIMESTAMP,
         AirTemp_C = Ta,
         SlrFD_W_Avg = PAR) %>%
  mutate(
    CH4_umol_m2_h = CH4_filled * (60 * 60), # h instead of s
    CO2_umol_m2_h_not_gapfilled = NEE * (60 * 60), 
    GPP_umol_m2_h = GPP_filled * (60 * 60),
    Reco_umol_m2_h = Reco * (60 * 60),
    CO2_umol_m2_h = -GPP_umol_m2_h + Reco_umol_m2_h,
    DateTime_EST = with_tz(DateTime, "EST"),
    date = as_date(DateTime)
  ) %>%
  rename(CH4_umol_m2_h_not_gapfilled = CH4,
         GPP_umol_m2_h_not_gapfilled = GPP) %>%
  select(DateTime_EST, location, Fluxing_Chamber, 
         CH4_umol_m2_h, CH4_umol_m2_h_not_gapfilled, CH4_R2, CH4_se, Ebullition_yn,
         CO2_umol_m2_h, CO2_umol_m2_h_not_gapfilled, CO2_R2, CO2_se, 
         Reco_umol_m2_h, GPP_umol_m2_h, GPP_umol_m2_h_not_gapfilled, 
         AirTemp_C, SlrFD_W_Avg) 

write_csv(ch4_out, here::here("processed_data", "L2- partitioned_and_gap_filled.csv"))
