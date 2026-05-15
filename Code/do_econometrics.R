library(tidyverse)
library(fixest)
library(sf)
library(modelsummary)

out_panel <- readRDS("Data/out_panel.rds")


#### TWFE MODEL ####

# Make a clean panel
main_df <- out_panel %>%
  # Geometry not needed here (only for graphs)
  st_drop_geometry %>%
  # Create variables for regression
  mutate(
    year_week_label = paste(year, week, sep = "-"),
    weight = pmax(valid_pixel_share_mean, 0.01)
  )

na_test <- main_df %>%
  filter(is.na(ntl_mean) | is.na(max_max_wind_poly) | is.na(valid_pixel_share_mean) | is.na(GEOID) | is.na(year_week_label))

nrow(na_test)

## Set up helper functions

# Models

# The NOTE message is because the first twelve weeks (that are being created by the lag operator)
# are being dropped since there is obviously no data yet

run_twfe_model <- function(
  indept,
  dept = ntl_mean,
  y_transformation = "asinh",
  lags = 12
) {
  indept_name <- deparse(substitute(indept))
  dept_name <- deparse(substitute(dept))


  model_formula <- as.formula(
    paste0(
      y_transformation,
      "(",
      dept_name,
      ") ~ l(",
      indept_name,
      ", 0:",
      lags,
      ") + valid_pixel_share_mean | GEOID + year_week_label"
    )
  )

  mod <- feols(
      model_formula,
      data = main_df,
      panel.id = ~ GEOID + year_week_label,
      # The weights give more weight to observations with better valid-pixel coverage
      weights = ~ weight,
      vcov = ~ GEOID
    )
}

# Plots

make_lag_plot <- function(
  model,
  variable,
  plot_title
) {
lag_df <- broom::tidy(model, conf.int = TRUE) %>%
  filter(str_detect(term, paste0("^l\\(", variable))) %>%
  mutate(lag = as.integer(str_extract(term, "-?\\d+")))

  ggplot(lag_df, aes(x = lag, y = estimate)) +
    geom_hline(yintercept = 0, linetype = 2) +
    geom_point() +
    geom_errorbar(aes(ymin = conf.low, ymax = conf.high), width = 0.2) +
    labs(
      x = "Lag (weeks)",
      y = "Effect on asinh(nightlights)",
      title = plot_title
    ) +
    theme_minimal()
}


## Run models and make graphs

# max_wind

mod_max_wind <- run_twfe_model(indept = max_max_wind_poly, lags = 30)
summary(mod_max_wind)

make_lag_plot(
    model = mod_max_wind,
    variable = "max_max_wind_poly",
    plot_title = "Distributed-lag effect of storm exposure"
    )

ggsave("Output/model_graphs/mod_max_wind_candle_graph.png")

# mean_wind

mod_mean_wind <- run_twfe_model(indept = mean_wind)
summary(mod_mean_wind)

make_lag_plot(
  model = mod_mean_wind,
  variable = "mean_wind",
  plot_title = "Distributed-lag effect of storm exposure"
  )

# day_count

mod_day_count <- run_twfe_model(indept = day_count)
summary(mod_day_count)

make_lag_plot(
  model = mod_day_count,
  variable = "day_count",
  plot_title = "Distributed-lag effect of storm exposure"
  )

# total_prec

mod_total_prec <- run_twfe_model(indept = precip_total_mm)
summary(mod_total_prec)

make_lag_plot(
  model = mod_total_prec,
  variable = "precip_total_mm",
  plot_title = "Distributed-lag effect of total week's precipitation")

# max_prec

mod_max_prec <- run_twfe_model(indept = precip_max_hourly_mm)
summary(mod_max_prec)

make_lag_plot(
  model = mod_max_prec,
  variable = "precip_max_hourly_mm",
  plot_title = "Distributed-lag effect of mean weekly precipitation")

# mean_prec

mod_mean_prec <- run_twfe_model(indept = precip_mean_hourly_mm)
summary(mod_mean_prec)

make_lag_plot(
  model = mod_max_prec,
  variable = "precip_mean_hourly_mm",
  plot_title = "Distributed-lag effect of mean weekly precipitation")



#### Event Study ####

