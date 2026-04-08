#### Summary Statistics ####

## Quick load data


## Nightlight Data ##

valid_pixel_share_storm_cov <- cov(panel_pr_ntl_storm$valid_pixel_share, panel_pr_ntl_storm$tc_day)

share_missing_ntl_days <- nrow(panel_pr_ntl_storm[panel_pr_ntl_storm$n_non_na_pixels == 0, ]) / nrow(panel_pr_ntl_storm)

panel_pr_sum_month <- panel_pr_ntl_storm %>%
  group_by(GEOID, month(date)) %>%
  summarize(
    mean_ntl = mean(ntl_mean, na.rm = TRUE),
    mean_valid_pixel_share = mean(valid_pixel_share, na.rm = TRUE),
    mean_cloud_free_share = mean(cloud_free_share, na.rm = TRUE),
    mean_tc_max_wind = mean(tc_max_wind, na.rm = TRUE),
    mean_tc_day = mean(tc_day, na.rm = TRUE),
    .groups = "drop"
  )

panel_pr_sum_total <- panel_pr_ntl_storm %>%
  group_by(GEOID) %>%
  summarize(
    mean_ntl = mean(ntl_mean, na.rm = TRUE),
    mean_valid_pixel_share = mean(valid_pixel_share, na.rm = TRUE),
    mean_cloud_free_share = mean(cloud_free_share, na.rm = TRUE),
    mean_tc_max_wind = mean(tc_max_wind, na.rm = TRUE),
    mean_tc_day = mean(tc_day, na.rm = TRUE),
    .groups = "drop"
  )

mod1 <- lm(ntl_median ~ tc_poly_mean_wind, data = panel_pr_ntl_storm)




panel_pr_ntl_storm[panel_pr_ntl_storm$tc_max_wind == max(panel_pr_ntl_storm$tc_max_wind), ]