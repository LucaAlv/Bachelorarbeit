library(tidyverse)
library(fixest)
library(sf)

out_panel <- readRDS("Data/out_panel.rds")

# Make a clean panel
main_df <- out_panel %>%
  # Geometry not needed here (only for graphs)
  st_drop_geometry %>%
  # Clean the data
  filter(
    !is.na(ntl_mean),
    !is.na(max_wind),
    valid_pixel_share_mean >= 0.5
  ) %>%
  # Create variables for regression
  mutate(
    year_week_label = paste(year, week, sep = "-"),
    weight = pmax(valid_pixel_share_mean, 0.01)
  )

na_test <- main_df %>%
  filter(is.na(ntl_mean) | is.na(max_wind) | is.na(valid_pixel_share_mean) | is.na(GEOID) | is.na(year_week_label))

nrow(na_test)

# The NOTE message is because the first twelve weeks (that are being created by the lag operator)
# are being dropped since there is obviously no data yet
m_dl <- feols(
  asinh(ntl_mean) ~ l(max_wind, 0:12) + valid_pixel_share_mean | GEOID + year_week_label,
  data = main_df,
  panel.id = ~ GEOID + year_week_label,
  # The weights give more weight to observations with better valid-pixel coverage
  weights = ~ weight,
  vcov = ~ GEOID
)

summary(m_dl)

lag_df <- broom::tidy(m_dl, conf.int = TRUE) %>%
  filter(str_detect(term, "^l\\(max_wind")) %>%
  mutate(lag = as.integer(str_extract(term, "-?\\d+")))

ggplot(lag_df, aes(x = lag, y = estimate)) +
  geom_hline(yintercept = 0, linetype = 2) +
  geom_point() +
  geom_errorbar(aes(ymin = conf.low, ymax = conf.high), width = 0.2) +
  labs(
    x = "Lag (weeks)",
    y = "Effect on asinh(nightlights)",
    title = "Distributed-lag effect of storm exposure"
  ) +
  theme_minimal()