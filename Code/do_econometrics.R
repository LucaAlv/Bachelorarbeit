library(tidyverse)
library(fixest)

out_panel <- readRDS("Output/out_panel.rds")

# Make a clean panel
main_df <- out_panel %>%
  filter(
    !is.na(ntl_mean),
    !is.na(max_wind),
    valid_pixel_share_mean >= 0.5
  ) %>%
  mutate(
    year_week_label = paste(year, week, sep = "-"),
    w = pmax(valid_pixel_share_mean, 0.01)
  )

m_dl <- feols(
  asinh(ntl_mean) ~ l(max_wind, 0:12) + valid_pixel_share_mean | GEOID + year_week_label,
  data = main_df,
  panel.id = ~ GEOID + year_week_label,
  weights = ~ w,
  vcov = ~ GEOID
)

lag_df <- broom::tidy(m_dl, conf.int = TRUE) %>%
  filter(str_detect(term, "^l\\(treat")) %>%
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