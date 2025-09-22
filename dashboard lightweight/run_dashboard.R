install.packages("rsconnect")
install.packages("quarto")
install.packages("shiny")
install.packages("plotly")
install.packages("here")
library(quarto)

unlink(here::here("dashboard lightweight","data","*"))
copy_target <- file.copy(here::here("processed_data","L0.csv"), here::here("dashboard lightweight", "data"), overwrite = T)

# Deploy
quarto_publish_app(input = here::here("dashboard lightweight"), 
                   server = "shinyapps.io")
