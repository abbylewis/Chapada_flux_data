load_data <- function(file){
  data_raw <- read_csv(file, col_types = cols(.default = "c"), skip = 1)
  location = str_extract(tolower(file), "high|low")
  
  data_small <- data_raw %>%
    rename(CH4d_ppb = CH4,
           CO2d_ppm = CO2) %>%
    filter(!CH4d_ppb == "Smp") %>%
    mutate(CH4d_ppm = as.numeric(CH4d_ppb)/1000,
           location = location) %>%
    select(location, TIMESTAMP, CH4d_ppm, CO2d_ppm, 
           Fluxing_Chamber, Manifold_Timer, Flux_Status)
  
  return(data_small)
}
