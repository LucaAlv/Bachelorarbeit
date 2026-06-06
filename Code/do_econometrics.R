library(tidyverse)
library(fixest)

out_panel <- readRDS("Data/out_panel.rds")
stacked_event_panel <- attr(out_panel, "stacked_event_panel")

write_csv(out_panel, "out_panel.csv")
write_csv(stacked_event_panel, "stacked_event_panel.csv")

#### Base TWFE Model ####

base_mod_storm <- feols(
  ntl_ihs ~ storm_period_max_wind + hq_share_mean |
    GEOID + period_id,
  data = out_panel,
  vcov = ~ GEOID
)

base_mod_wind <- feols(
  ntl_ihs ~ max_windgust_max_mps + hq_share_mean |
    GEOID + period_id,
  data = out_panel,
  vcov = ~ GEOID
)

base_mod_prec <- feols(
  ntl_ihs ~ precip_total_mm + hq_share_mean |
    GEOID + period_id,
  data = out_panel,
  vcov = ~ GEOID
)

base_mod_weather <- feols(
  ntl_ihs ~ max_windgust_max_mps + precip_total_mm + hq_share_mean |
    GEOID + period_id,
  data = out_panel,
  vcov = ~ GEOID
)

etable(base_mod_storm, base_mod_wind, base_mod_prec, base_mod_weather)


#### Stacked Event Study ####

dir.create("Output/model_graphs", recursive = TRUE, showWarnings = FALSE)

if (is.null(stacked_event_panel)) {
  stop(
    "Data/out_panel.rds does not contain stacked_event_panel. ",
    "Re-run Code/do_get_data.R before running the event-study models.",
    call. = FALSE
  )
}

# Drop county-periods where another storm is active in the same event stack.
# This keeps the first pass simple and avoids obvious contamination.
stacked_event_model_df <- stacked_event_panel %>%
  filter(!period_has_other_storm) %>%
  filter(!is.na(ntl_ihs), !is.na(hq_share_mean), !is.na(rel_period))

stacked_event_mod <- feols(
  ntl_ihs ~ i(rel_period, treated_for_event, ref = -1) + hq_share_mean |
    geoid_storm_id + storm_period_id,
  data = stacked_event_model_df,
  vcov = ~ GEOID + storm_name
)

summary(stacked_event_mod)

png(
  filename = "Output/model_graphs/stacked_event_study.png",
  width = 2400,
  height = 1600,
  res = 300
)
iplot(
  stacked_event_mod,
  ref.line = -1,
  main = "Stacked event study",
  xlab = "Relative period",
  ylab = "Effect on asinh(nightlights)"
)
dev.off()


#### Single-storm Event Study: Hurricane Maria ####

maria_storm_name <- stacked_event_model_df %>%
  distinct(storm_name) %>%
  mutate(storm_name_chr = as.character(storm_name)) %>%
  filter(str_detect(storm_name_chr, "MARIA")) %>%
  pull(storm_name_chr) %>%
  first()

if (is.na(maria_storm_name)) {
  stop("No storm with `MARIA` in storm_name found in stacked_event_panel.", call. = FALSE)
}

maria_event_panel <- stacked_event_model_df %>%
  filter(as.character(storm_name) == maria_storm_name)

maria_event_mod <- feols(
  ntl_ihs ~ i(rel_period, treated_for_event, ref = -1) + hq_share_mean |
    GEOID + period_id,
  data = maria_event_panel,
  vcov = ~ GEOID
)

summary(maria_event_mod)

png(
  filename = "Output/model_graphs/maria_event_study.png",
  width = 2400,
  height = 1600,
  res = 300
)
iplot(
  maria_event_mod,
  ref.line = -1,
  main = paste("Single-storm event study:", maria_storm_name),
  xlab = "Relative period",
  ylab = "Effect on asinh(nightlights)"
)
dev.off()

event_study_table <- etable(
  stacked_event_mod,
  maria_event_mod,
  headers = c("Stacked", maria_storm_name)
)

writeLines(
  capture.output(event_study_table),
  "Output/model_graphs/event_study_models.txt"
)












#### Distributed lag TWFE MODEL ####

## Set up helper functions

# Models

# The NOTE message is because the first twelve weeks (that are being created by the lag operator)
# are being dropped since there is obviously no data yet

run_twfe_model <- function(
  indept,
  dept = ntl_mean,
  lags = 12
) {
  indept_name <- deparse(substitute(indept))
  dept_name <- deparse(substitute(dept))


  model_formula <- as.formula(
    paste0(
      dept_name,
      " ~ l(",
      indept_name,
      ", 0:",
      lags,
      ") + hq_share_mean | GEOID + period_id"
    )
  )

  mod <- feols(
      model_formula,
      data = out_panel,
      panel.id = ~ GEOID + period_id,
      # The weights give more weight to observations with better valid-pixel coverage
      weights = ~ cloud_free_share_mean,
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

mod <- feols(
      ntl_ihs ~ l(max_windgust_max_mps, -4:12) | GEOID + period_id,
      data = out_panel,
      panel.id = ~ GEOID + period_id,
      # The weights give more weight to observations with better valid-pixel coverage
      weights = ~ cloud_free_share_mean,
      vcov = ~ GEOID
    )

summary(mod)

mod_max_wind <- run_twfe_model(indept = storm_period_max_wind, lags = 30)
summary(mod_max_wind)

make_lag_plot(
    model = mod_max_wind,
    variable = "storm_period_max_wind",
    plot_title = "Distributed-lag effect of storm exposure"
    )

ggsave("Output/model_graphs/mod_max_wind_candle_graph.png")

# mean wind gust

mod_mean_windgust <- run_twfe_model(indept = mean_windgust_max_mps)
summary(mod_mean_windgust)

make_lag_plot(
  model = mod_mean_windgust,
  variable = "mean_windgust_max_mps",
  plot_title = "Distributed-lag effect of mean weekly wind gust"
  )

# storm days

mod_storm_days <- run_twfe_model(indept = storm_period_days)
summary(mod_storm_days)

make_lag_plot(
  model = mod_storm_days,
  variable = "storm_period_days",
  plot_title = "Distributed-lag effect of storm-exposed days"
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
  model = mod_mean_prec,
  variable = "precip_mean_hourly_mm",
  plot_title = "Distributed-lag effect of mean weekly precipitation")
