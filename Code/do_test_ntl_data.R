

# Monthly panel for graphs
panel_pr_ntl_storm_era5_monthly <- panel_pr_ntl_storm_era5 %>%
  mutate(
    year = year(date),
    month = month(date)
  ) %>%
  group_by(month, year) %>%
  summarize(
    ntl_mean_mmean = mean(ntl_mean, na.rm = TRUE),
    ntl_median_mmean = mean(ntl_median, na.rm = TRUE),
    n_pixels_mmean = mean(n_pixels, na.rm = TRUE),
    n_non_na_pixels_mmean = mean(n_non_na_pixels, na.rm = TRUE),
    valid_pixel_share_mmean = mean(valid_pixel_share, na.rm = TRUE),
    valid_pixel_share_mmedian = median(valid_pixel_share, na.rm = TRUE),
    cloud_free_share_mmean = mean(cloud_free_share, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
     month_year = paste(month, year, sep = "-"),
  )

View(panel_pr_ntl_storm_era5)
View(panel_pr_ntl_storm_era5_daily)
View(panel_pr_ntl_storm_era5_monthly)

## Nightlight Data

# Check if there is a strong correlation between missing pixels on storm days
valid_pixel_share_storm_cov <- cov(
  panel_pr_ntl_storm_era5$valid_pixel_share,
  panel_pr_ntl_storm_era5$tc_day,
  use = "complete.obs"
)

# Check for how many days we don't have valid information
share_days_ntl_pixels_na <- nrow(panel_pr_ntl_storm[panel_pr_ntl_storm$n_non_na_pixels == 0, ]) / nrow(panel_pr_ntl_storm)
share_days_no_valid_ntl_pixels <- nrow(panel_pr_ntl_storm[panel_pr_ntl_storm$valid_pixel_share == 0, ]) / nrow(panel_pr_ntl_storm)


## Nightlight Data

ggplot(data = panel_pr_ntl_storm_era5) +
  geom_point(aes(x = date, y = ntl_mean)) +
  geom_point(aes(x = date, y = ntl_mean))




ntl_monthly_timeseries <- ggplot(panel_pr_ntl_storm_era5_monthly, aes(x = month-year, y = valid_pixel_share_mmedian)) +
  geom_point() +
  labs(
    x = "Date",
    y = "Valid pixel share",
    title = "Daily valid pixel share over time"
  ) +
  theme_minimal()

ggsave("Output/ntl_monthly_timeseries.png", ntl_monthly_timeseries)

ntl_daily_valid_heatmap <- ggplot(panel_pr_ntl_storm_era5_daily, aes(x = day, y = factor(month_year), fill = valid_pixel_share)) +
  geom_tile() +
  scale_fill_viridis_c(na.value = "red") +
  labs(
    x = "Day of year",
    y = "Year",
    fill = "Valid pixel share",
    title = "Seasonal pattern in valid pixel share"
  ) +
  theme_minimal()

ggsave("Output/ntl_daily_valid_heatmap.png", ntl_daily_valid_heatmap)