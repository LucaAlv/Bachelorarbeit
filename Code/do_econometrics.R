library(tidyverse)
library(fixest)
library(fwildclusterboot)

print("Setting up output directories")
dir.create("Output/model_graphs", recursive = TRUE, showWarnings = FALSE)
dir.create("Output/model_tables", recursive = TRUE, showWarnings = FALSE)

print("Loading Data")
out_panel <- readRDS("Output/data_output/out_panel.rds")
stacked_event_panel <- attr(out_panel, "stacked_event_panel")
treated_event_panel <- attr(out_panel, "treated_event_panel")

#write_csv(out_panel, "out_panel.csv")
#write_csv(stacked_event_panel, "stacked_event_panel.csv")



#### BASE TWFE

## NOLAG

print("Running base TWFE models")

base_mod_stormbi <- feols(
  ntl_ihs ~ storm_is_happening |
    GEOID + period_id,
  data = out_panel,
  weights = ~ hq_share_mean,
  vcov = ~ GEOID
)

base_mod_storm <- feols(
  ntl_ihs ~ storm_period_max_wind |
    GEOID + period_id,
  data = out_panel,
  weights = ~ hq_share_mean,
  vcov = ~ GEOID
)

base_mod_wind <- feols(
  ntl_ihs ~ max_windgust_max_mps |
    GEOID + period_id,
  data = out_panel,
  weights = ~ hq_share_mean,
  vcov = ~ GEOID
)

base_mod_prec <- feols(
  ntl_ihs ~ precip_total_mm |
    GEOID + period_id,
  data = out_panel,
  weights = ~ hq_share_mean,
  vcov = ~ GEOID
)

base_mod_weather <- feols(
  ntl_ihs ~ max_windgust_max_mps + precip_total_mm |
    GEOID + period_id,
  data = out_panel,
  weights = ~ hq_share_mean,
  vcov = ~ GEOID
)

etable(base_mod_stormbi, base_mod_storm, base_mod_wind, base_mod_prec, base_mod_weather)


## LAG

print("Running base lagged TWFE models")

# The weights give more weight to observations with better valid-pixel coverage

base_lag_mod_stormbi <- feols(
  ntl_ihs ~ l(storm_is_happening, -4:20) |
    GEOID + period_id,
  data = out_panel,
  panel.id = ~ GEOID + period_id,
  weights = ~ hq_share_mean,
  vcov = ~ GEOID
)

base_lag_mod_storm <- feols(
  ntl_ihs ~ l(storm_period_max_wind, -4:20) |
    GEOID + period_id,
  data = out_panel,
  panel.id = ~ GEOID + period_id,
  weights = ~ hq_share_mean,
  vcov = ~ GEOID
)

base_lag_mod_wind <- feols(
  ntl_ihs ~ l(max_windgust_max_mps, -4:20) |
    GEOID + period_id,
  data = out_panel,
  panel.id = ~ GEOID + period_id,
  weights = ~ hq_share_mean,
  vcov = ~ GEOID
)

base_lag_mod_prec <- feols(
  ntl_ihs ~ l(precip_total_mm, -4:20) |
    GEOID + period_id,
  data = out_panel,
  panel.id = ~ GEOID + period_id,
  weights = ~ hq_share_mean,
  vcov = ~ GEOID
)

base_lag_mod_weather <- feols(
  ntl_ihs ~ l(max_windgust_max_mps, -4:20) + l(precip_total_mm, -4:20) |
    GEOID + period_id,
  data = out_panel,
  panel.id = ~ GEOID + period_id,
  weights = ~ hq_share_mean,
  vcov = ~ GEOID
)

print("Making base lagged TWFE model plots and graphs")

etable(
  base_mod_stormbi, base_mod_storm, base_mod_wind, base_mod_prec, base_mod_weather,
  base_lag_mod_stormbi, base_lag_mod_storm, base_lag_mod_wind, base_lag_mod_prec, base_lag_mod_weather
)

gc()










#### STACKED EVENT STUDY 

# Check for unsupported periods
# i.e those periods that only have treated observations or only control observations
check_period_support <- function(data) {

  maria_unsupported_rel_periods <- data %>%
    group_by(rel_period) %>%
    summarise(
      # are there any treated observations in this period?
      has_treated = any(treated_for_event),
      # are there any control observations in this period?
      has_control = any(!treated_for_event),
      .groups = "drop"
    ) %>%
    filter(!has_treated | !has_control) %>%
    pull(rel_period)

  if (length(maria_unsupported_rel_periods) > 0) {
    message(
      "Dropping unsupported Maria relative periods: ",
      paste(maria_unsupported_rel_periods, collapse = ", ")
    )

    data <- data %>%
      filter(!rel_period %in% maria_unsupported_rel_periods)
  }

  return(data)

}


### ALL STORMS



# sem = stacked stacked event study model

## FULL MODEL BINARY TREATMENT

# Full combined_local_rel_period
df_full_binary <- stacked_event_panel %>%
  filter(!is.na(combined_local_rel_period)) %>%
  # Important modeling choice: duplicate storms are removed
  filter(!period_has_other_storm) %>%
  filter(!is.na(ntl_ihs), !is.na(hq_share_mean)) %>%
  check_period_support()

sem_full_binary <- feols(
  ntl_ihs ~ i(combined_local_rel_period, treated_for_event, ref = -5)  |
    geoid_storm_id + storm_period_id,
  data = df_full_binary,
  weights = ~ (hq_share_mean * stack_weight),
  vcov = ~ GEOID + storm_name
)

iplot(
  sem_full_binary,
  ref.line = -1,
  main = "Stacked event study",
  xlab = "Relative period",
  ylab = "Effect on asinh(nightlights)"
)

##!! FULL MODEL BINARY TREATMENT COMBINED RELATIVE PRE-PERIODS

event_bin_levels <- c(
  "pre_-8_-3",
  "pre_-2_-1",
  "impact_0",
  "post_1_3",
  "post_4_8",
  "post_9_20"
)

df_full_prebase <- stacked_event_panel %>%
  filter(!is.na(combined_local_rel_period)) %>%
  # Important modeling choice: duplicate storms are removed
  filter(!period_has_other_storm) %>%
  filter(!is.na(ntl_ihs), !is.na(hq_share_mean)) %>%
  check_period_support() %>%
  mutate(
    event_bin = case_when(
      between(combined_local_rel_period, -8, -3) ~ "pre_-8_-3",
      between(combined_local_rel_period, -2, -1) ~ "pre_-2_-1",
      combined_local_rel_period == 0 ~ "impact_0",
      between(combined_local_rel_period, 1, 3) ~ "post_1_3",
      between(combined_local_rel_period, 4, 8) ~ "post_4_8",
      between(combined_local_rel_period, 9, 20) ~ "post_9_20",
      TRUE ~ NA_character_
    ),
    event_bin = factor(event_bin, levels = event_bin_levels, ordered = TRUE)
  ) %>%
  filter(!is.na(event_bin)) %>%
  check_period_support()

sem_full_prebase <- feols(
  ntl_ihs ~ i(event_bin, treated_for_event, ref = "pre_-8_-3") |
    geoid_storm_id + storm_period_id,
  data = filter(df_full_prebase, !is.na(event_bin)),
  weights = ~ (hq_share_mean * stack_weight),
  vcov = ~ GEOID + storm_name
)

etable(sem_full_prebase)

iplot(
  sem_full_prebase,
  ref.line = -1,
  main = "Stacked event study",
  xlab = "Relative period",
  ylab = "Effect on asinh(nightlights)"
)

## FULL MODEL CONTINUOUS TREATMENT

df_full_cont <- stacked_event_panel %>%
  filter(!is.na(combined_local_rel_period)) %>%
  # Important modeling choice: duplicate storms are removed
  filter(!period_has_other_storm) %>%
  filter(!is.na(ntl_ihs), !is.na(hq_share_mean)) %>%
  check_period_support()

sem_full_cont <- feols(
  ntl_ihs ~ i(combined_local_rel_period, event_storm, ref = -1)  |
    geoid_storm_id + storm_period_id,
  data = df_full_cont,
  weights = ~ (hq_share_mean * stack_weight),
  vcov = ~ GEOID + storm_name
)

iplot(sem_full_cont)
#summary(sem_full_cont)

## FULL MODEL CONTINUOUS TREATMENT BINNED EXPOSURE

df_full_binned <- stacked_event_panel %>%
  filter(!is.na(combined_local_rel_period)) %>%
  # Important modeling choice: duplicate storms are removed
  filter(!period_has_other_storm) %>%
  filter(!is.na(ntl_ihs), !is.na(hq_share_mean)) %>%
  check_period_support()

sem_full_binned <- feols(
  ntl_ihs ~
    i(combined_local_rel_period, treated_low, ref = -5) +
    i(combined_local_rel_period, treated_medium, ref = -5) +
    i(combined_local_rel_period, treated_high, ref = -5) |
    geoid_storm_id + storm_period_id,
  data = df_full_binned,
  weights = ~ (hq_share_mean * stack_weight),
  vcov = ~ GEOID + storm_name
)

etable(sem_full_binned)

iplot(
  sem_full_binned,
  ref.line = -1,
  main = "Stacked event study",
  xlab = "Relative period",
  ylab = "Effect on asinh(nightlights)"
)




##!! FULL MODEL CONTINUOUS TREATMENT BINNED EXPOSURE AND COMBINED RELATIVE PRE-PERIODS 

df_full_binned_prebase <- stacked_event_panel %>%
  filter(!is.na(combined_local_rel_period)) %>%
  # Important modeling choice: duplicate storms are removed
  filter(!period_has_other_storm) %>%
  filter(!is.na(ntl_ihs), !is.na(hq_share_mean)) %>%
  mutate(event_bin = case_when(
    between(combined_local_rel_period, -8, -3) ~ "pre_-8_-3",
    between(combined_local_rel_period, -2, -1) ~ "pre_-2_-1",
    combined_local_rel_period == 0 ~ "impact_0",
    between(combined_local_rel_period, 1, 3) ~ "post_1_3",
    between(combined_local_rel_period, 4, 8) ~ "post_4_8",
    between(combined_local_rel_period, 9, 20) ~ "post_9_20",
    TRUE ~ NA_character_
    ),
    event_bin = factor(event_bin, levels = event_bin_levels, ordered = TRUE)
  ) %>%
  filter(!is.na(event_bin)) %>%
  check_period_support()

# event_bin = binned periods / treated_* = separate coefficients per exposure group
sem_full_binned_prebase <- feols(
  ntl_ihs ~
    i(event_bin, treated_low, ref = "pre_-8_-3") +
    i(event_bin, treated_medium, ref = "pre_-8_-3") +
    i(event_bin, treated_high, ref = "pre_-8_-3") |
    geoid_storm_id + storm_period_id,
  data = df_full_binned_prebase,
  weights = ~ (hq_share_mean * stack_weight),
  vcov = ~ GEOID + storm_name
)

etable(sem_full_binned_prebase)

iplot(
  sem_full_binned_prebase,
  ref.line = -1,
  main = "Stacked event study - binned exposure and periods",
  xlab = "Relative period",
  ylab = "Effect on asinh(nightlights)"
)

## BOOTSTRAPPING

#boot_t0_storm <- boottest(
#  sem_full_binary,
#  param = "combined_local_rel_period::0:treated_for_event",
#  clustid = "storm_name",
#  B = 9999,
#  type = "rademacher",
#  impose_null = TRUE,
#  conf_int = TRUE
#)
#
#boot_t0_storm





# MANUEL

# boottest() dummy-expands fixest fixed effects when the model uses weights.
# Residualizing first keeps the weighted model equivalent without the huge FE matrix.
boot_t0_param <- "combined_local_rel_period::0:treated_for_event"
boot_t0_dm <- as.data.frame(demean(sem_full_binary, as.matrix = TRUE), check.names = FALSE)

if (nrow(boot_t0_dm) != nrow(df_full_binary)) {
  stop("The demeaned bootstrap data no longer aligns with df_full_binary.")
}

boot_t0_original_names <- names(boot_t0_dm)
boot_t0_safe_names <- make.names(boot_t0_original_names, unique = TRUE)
names(boot_t0_dm) <- boot_t0_safe_names

boot_t0_lm_param <- boot_t0_safe_names[match(boot_t0_param, boot_t0_original_names)]
if (is.na(boot_t0_lm_param)) {
  stop("Could not find the requested bootstrap coefficient in the demeaned model matrix.")
}

boot_t0_data <- boot_t0_dm %>%
  mutate(storm_name = df_full_binary$storm_name)

boot_t0_lm_formula <- reformulate(
  termlabels = setdiff(boot_t0_safe_names, boot_t0_safe_names[1]),
  response = boot_t0_safe_names[1],
  intercept = FALSE
)

boot_t0_weights <- weights(sem_full_binary)
boot_t0_lm <- lm(
  boot_t0_lm_formula,
  data = boot_t0_data,
  weights = boot_t0_weights
)

set.seed(123)
if (requireNamespace("dqrng", quietly = TRUE)) {
  dqrng::dqset.seed(123)
}

boot_t0 <- boottest(
  boot_t0_lm,
  param = boot_t0_lm_param,
  clustid = "storm_name",
  B = 9999,
  type = "rademacher",
  bootstrap_type = "fnw11",
  impose_null = TRUE,
  conf_int = TRUE
)

#summary(boot_t0)
boot_t0_tidy <- generics::tidy(boot_t0) %>%
  mutate(term = str_replace(term, fixed(boot_t0_lm_param), boot_t0_param))
boot_t0_tidy

rm(boot_t0_dm, boot_t0_data, boot_t0_lm, boot_t0_weights)
gc()







## FULL MODEL TABLES AND GRAPHS

print("Setting up main tables and graphs")

etable(
  sem_full_binary, sem_full_binned, sem_full_cont, sem_full_prebase,
  tex = FALSE,
  file = "Output/model_tables/table_2_main_results.txt",
  replace = TRUE
)


















### ROBUSTNESS CHECKS

print("Running robustness models")

## CHECK FOR REFERENCE PERIOD
# Full combined_local_rel_period with different reference
df_rob_ref1 <- stacked_event_panel %>%
  filter(!is.na(combined_local_rel_period)) %>%
  # Important modeling choice: duplicate storms are removed
  filter(!period_has_other_storm) %>%
  filter(!is.na(ntl_ihs), !is.na(hq_share_mean)) %>%
  check_period_support()

sem_rob_ref1 <- feols(
  ntl_ihs ~ i(combined_local_rel_period, treated_for_event, ref = -2) |
    geoid_storm_id + storm_period_id,
  data = df_rob_ref1,
  weights = ~ (hq_share_mean * stack_weight),
  vcov = ~ GEOID + storm_name
)

#summary(sem_rob_ref1)


## CHECK FOR MULTI STORM EFFECTS
# Eliminate all periods with multiple storms
df_rob_nomulti <- stacked_event_panel %>%
  group_by(storm_name, GEOID) %>%
  mutate(other_storm_in_window = any(period_has_other_storm)) %>%
  ungroup() %>%
  filter(!other_storm_in_window) %>%
  check_period_support()

sem_rob_nomulti <- feols(
  ntl_ihs ~ i(combined_local_rel_period, treated_for_event, ref = -1) |
    geoid_storm_id + storm_period_id,
  data = df_rob_nomulti,
  weights = ~ (hq_share_mean * stack_weight),
  vcov = ~ GEOID + storm_name
)

#summary(sem_rob_nomulti)

# Shorter window sample: less chance of contamination
df_rob_short <- stacked_event_panel %>%
  filter(combined_local_rel_period >= -4, combined_local_rel_period <= 8) %>%
  # Important modeling choice: duplicate storms are removed
  filter(!period_has_other_storm) %>%
  check_period_support()

sem_rob_short <- feols(
  ntl_ihs ~ i(combined_local_rel_period, treated_for_event, ref = -1) |
    geoid_storm_id + storm_period_id,
  data = df_rob_short,
  weights = ~ (hq_share_mean * stack_weight),
  vcov = ~ GEOID + storm_name
)

#summary(sem_rob_short)




## CHECK FOR STORM HETEROGENEITY
# Drop each storm
df_rob_loo <- stacked_event_panel %>%
  # Important modeling choice: duplicate storms are removed
  filter(!period_has_other_storm) %>%
  filter(!is.na(ntl_ihs), !is.na(hq_share_mean)) %>%
  check_period_support()


storms <- unique(df_rob_loo$storm_name)

sem_rob_loo <- map_dfr(storms, function(s) {
  df_s <- df_rob_loo %>%
    filter(storm_name != s) %>%
    check_period_support()

  mod_s <- feols(
    ntl_ihs ~ i(combined_local_rel_period, treated_for_event, ref = -1) |
      geoid_storm_id + storm_period_id,
    data = df_s,
    weights = ~ (hq_share_mean * stack_weight),
    vcov = ~ GEOID + storm_name
  )

  broom::tidy(mod_s) %>%
    mutate(dropped_storm = s)
})

filter(sem_rob_loo, p.value < 0.05)


# Drop small storms
storm_sizes <- stacked_event_panel %>%
  group_by(storm_name) %>%
  summarise(n_treated_geo = n_distinct(GEOID[treated_for_event]), .groups = "drop")

df_rob_loo_s <- stacked_event_panel %>%
  # Important modeling choice: duplicate storms are removed
  filter(!period_has_other_storm) %>%
  filter(!is.na(ntl_ihs), !is.na(hq_share_mean)) %>%
  left_join(storm_sizes, by = "storm_name") %>%
  filter(n_treated_geo >= 30) %>%
  check_period_support()

sem_rob_loo_s <- feols(
  ntl_ihs ~ i(combined_local_rel_period, treated_for_event, ref = -1) |
    geoid_storm_id + storm_period_id,
  data = df_rob_loo_s,
  weights = ~ (hq_share_mean * stack_weight),
  vcov = ~ GEOID + storm_name
)

#summary(sem_rob_loo_s)





## CHECK FOR GEOGRAPHIC HETEROGENEITY (esp. PR)
# Drop Puerto Rico

df_rob_nopr <- stacked_event_panel %>%
  filter(!is.na(combined_local_rel_period)) %>%
  # Important modeling choice: duplicate storms are removed
  filter(!period_has_other_storm) %>%
  filter(!is.na(ntl_ihs), !is.na(hq_share_mean)) %>%
  filter(!startsWith(GEOID, "72")) %>%
  check_period_support()

sem_rob_nopr <- feols(
  ntl_ihs ~ i(combined_local_rel_period, treated_for_event, ref = -1) |
    geoid_storm_id + storm_period_id,
  data = df_rob_nopr,
  weights = ~ (hq_share_mean * stack_weight),
  vcov = ~ GEOID + storm_name
)

#summary(sem_rob_nopr)



## CHECK FOR QUALITY FLAGS
# Model without hq_share

df_rob_noqf <- stacked_event_panel %>%
  filter(!is.na(combined_local_rel_period)) %>%
  # Important modeling choice: duplicate storms are removed
  filter(!period_has_other_storm) %>%
  filter(!is.na(ntl_ihs), !is.na(hq_share_mean)) %>%
  check_period_support()

sem_rob_noqf <- feols(
  ntl_ihs ~ i(combined_local_rel_period, treated_for_event, ref = -1) |
    geoid_storm_id + storm_period_id,
  data = df_rob_noqf,
  weights = ~ stack_weight,
  vcov = ~ GEOID + storm_name
)

#summary(sem_rob_noqf)



# Quality restricted sample

df_rob_hqonly <- stacked_event_panel %>%
  filter(!is.na(combined_local_rel_period)) %>%
  # Important modeling choice: duplicate storms are removed
  filter(!period_has_other_storm) %>%
  filter(!is.na(ntl_ihs), !is.na(hq_share_mean)) %>%
  filter(hq_share_mean >= 0.6) %>%
  check_period_support()

sem_rob_hqonly <- feols(
  ntl_ihs ~ i(combined_local_rel_period, treated_for_event, ref = -1) |
    geoid_storm_id + storm_period_id,
  data = df_rob_hqonly,
  weights = ~ (hq_share_mean * stack_weight),
  vcov = ~ GEOID + storm_name
)

#summary(sem_rob_hqonly)




## ALTERNATIVE OUTCOME VARIABLES

# untransformed mean ntl
df_rob_meanntl <- stacked_event_panel %>%
  filter(!is.na(combined_local_rel_period)) %>%
  # Important modeling choice: duplicate storms are removed
  filter(!period_has_other_storm) %>%
  filter(!is.na(ntl_ihs), !is.na(hq_share_mean)) %>%
  check_period_support()

sem_rob_meanntl <- feols(
  ntl_mean ~ i(combined_local_rel_period, treated_for_event, ref = -1) |
    geoid_storm_id + storm_period_id,
  data = df_rob_meanntl,
  weights = ~ (hq_share_mean * stack_weight),
  vcov = ~ GEOID + storm_name
)

#summary(sem_rob_meanntl)


# log ntls
df_rob_logntl <- stacked_event_panel %>%
  filter(!is.na(combined_local_rel_period)) %>%
  # Important modeling choice: duplicate storms are removed
  filter(!period_has_other_storm) %>%
  filter(!is.na(ntl_ihs), !is.na(hq_share_mean)) %>%
  check_period_support()

sem_rob_logntl <- feols(
  ntl_log ~ i(combined_local_rel_period, treated_for_event, ref = -1) |
    geoid_storm_id + storm_period_id,
  data = df_rob_logntl,
  weights = ~ (hq_share_mean * stack_weight),
  vcov = ~ GEOID + storm_name
)

#summary(sem_rob_logntl)



## NO FEs

df_rob_nofes <- stacked_event_panel %>%
  filter(!is.na(combined_local_rel_period)) %>%
  # Important modeling choice: duplicate storms are removed
  filter(!period_has_other_storm) %>%
  filter(!is.na(ntl_ihs), !is.na(hq_share_mean)) %>%
  check_period_support()

sem_rob_nofes <- feols(
  ntl_ihs ~ i(combined_local_rel_period, treated_for_event, ref = -1),
  data = df_rob_nofes,
  weights = ~ (hq_share_mean * stack_weight),
  vcov = ~ GEOID + storm_name
)

#summary(sem_rob_nofes)



## ROBUSTNESS TABLES AND GRAPHS

print("Setting up robustness tables and graphs")

etable(
  sem_full_binary, sem_rob_ref1, sem_rob_nomulti, sem_rob_short, sem_rob_loo_s, sem_rob_nopr,
  tex = FALSE,
  file = "Output/model_tables/table_3_main_robustness.txt",
  replace = TRUE
)

etable(
  sem_full_binary, sem_rob_noqf, sem_rob_hqonly, sem_rob_meanntl, sem_rob_logntl,
  tex = FALSE,
  file = "Output/model_tables/table_4_secondary_robustness.txt",
  replace = TRUE
)
















### ROBUSTNESS CHECK - ERA5

print("Running robustness models with ERA5 data")

## CHECK FOR CLEAR TREATMENT DEFINITION WITH ERA5

df_rob_era5 <- stacked_event_panel %>%
  filter(!is.na(combined_local_rel_period)) %>%
  # Important modeling choice: duplicate storms are removed
  filter(!period_has_other_storm) %>%
  filter(!is.na(max_windgust_max_mps)) %>%
  check_period_support()

sem_rob_era5_wind <- feols(
  max_windgust_max_mps ~ i(combined_local_rel_period, treated_for_event, ref = -1) |
    geoid_storm_id + storm_period_id,
  data = df_rob_era5,
  vcov = ~ GEOID + storm_name
)

sem_rob_era5_precip <- feols(
  precip_total_mm ~ i(combined_local_rel_period, treated_for_event, ref = -1) |
    geoid_storm_id + storm_period_id,
  data = df_rob_era5,
  vcov = ~ GEOID + storm_name
)


## SHOW WEATHER PRE TRENDS WITH ERA5

df_rob_era5_pre <- stacked_event_panel %>%
  filter(!is.na(combined_local_rel_period)) %>%
  # Important modeling choice: duplicate storms are removed
  filter(!period_has_other_storm) %>%
  filter(!is.na(ntl_ihs), !is.na(hq_share_mean)) %>%
  check_period_support()

df_rob_era5_pre_balanced <- df_rob_era5_pre %>%
  filter(combined_local_rel_period >= -8, combined_local_rel_period <= -2) %>%
  group_by(treated_for_event, combined_local_rel_period) %>%
  summarise(
    ntl_ihs = mean(ntl_ihs, na.rm = TRUE),
    precip_total_mm = mean(precip_total_mm, na.rm = TRUE),
    max_windgust_max_mps = mean(max_windgust_max_mps, na.rm = TRUE),
    hq_share_mean = mean(hq_share_mean, na.rm = TRUE),
    cloud_free_share_mean = mean(cloud_free_share_mean, na.rm = TRUE),
    .groups = "drop"
  )

df_rob_era5_pre_balanced_long <- df_rob_era5_pre_balanced %>%
  pivot_longer(
    cols = c(
      ntl_ihs,
      precip_total_mm,
      max_windgust_max_mps,
      hq_share_mean,
      cloud_free_share_mean
    ),
    names_to = "metric",
    values_to = "mean_value"
  ) %>%
  mutate(
    treated_for_event = factor(
      if_else(treated_for_event, "Treated", "Control"),
      levels = c("Control", "Treated")
    ),
    metric = factor(
      recode(
        metric,
        ntl_ihs = "Nightlights (IHS)",
        precip_total_mm = "Precipitation (mm)",
        max_windgust_max_mps = "Max wind gust (m/s)",
        hq_share_mean = "High-quality pixel share",
        cloud_free_share_mean = "Cloud-free pixel share"
      ),
      levels = c(
        "Nightlights (IHS)",
        "Precipitation (mm)",
        "Max wind gust (m/s)",
        "High-quality pixel share",
        "Cloud-free pixel share"
      )
    )
  )


## EXCLUDE PERIODS WITH OTHER SEVERE WEATHER EXPOSURE

df_rob_ear5_no_other_weather <- stacked_event_panel %>%
  filter(
    treated_for_event |
      (
        precip_total_mm < quantile(precip_total_mm, 0.95, na.rm = TRUE) &
        max_windgust_max_mps < quantile(max_windgust_max_mps, 0.95, na.rm = TRUE)
      )
  ) %>%
  filter(!is.na(combined_local_rel_period)) %>%
  # Important modeling choice: duplicate storms are removed
  filter(!period_has_other_storm) %>%
  filter(!is.na(ntl_ihs), !is.na(hq_share_mean)) %>%
  check_period_support()

sem_rob_ear5_no_other_weather <- feols(
  ntl_ihs ~ i(combined_local_rel_period, treated_for_event, ref = -1) |
    geoid_storm_id + storm_period_id,
  data = df_rob_ear5_no_other_weather,
  weights = ~ (hq_share_mean * stack_weight),
  vcov = ~ GEOID + storm_name
)

#summary(sem_rob_ear5_no_other_weather)


## ROBUSTNESS CHECKS WITH ERA5 TABLES AND GRAPHS

print("Setting up robustness with ERA5 tables and graphs")

balance_era5_plot <- ggplot(
  df_rob_era5_pre_balanced_long,
  aes(
    x = combined_local_rel_period,
    y = mean_value,
    color = treated_for_event,
    group = treated_for_event
  )
) +
  geom_line(linewidth = 0.8) +
  geom_point(size = 2) +
  facet_wrap(~ metric, scales = "free_y", ncol = 2) +
  scale_x_continuous(breaks = -8:-2) +
  scale_color_manual(values = c("Control" = "#4E79A7", "Treated" = "#E15759")) +
  labs(
    x = "Relative period before storm",
    y = "Group mean",
    color = NULL,
    title = "Pre-trends in ERA5 and nightlight variables"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    legend.position = "top",
    panel.grid.minor = element_blank(),
    strip.text = element_text(face = "bold")
  )

balance_era5_plot















### INDIVIDUAL STORMS

## EVENT STUDY MODELS

print("Running models for individual storms")

# MARIA

print("Running model for Maria")

df_event_maria <- stacked_event_panel %>%
  filter(!is.na(combined_local_rel_period)) %>%
  # Important modeling choice: duplicate storms are removed
  filter(!period_has_other_storm) %>%
  filter(!is.na(ntl_ihs), !is.na(hq_share_mean)) %>%
  filter(storm_name == "MARIA-2017") %>%
  check_period_support()

sem_maria <- feols(
  ntl_ihs ~ i(rel_period, treated_for_event, ref = -3) |
    GEOID + period_id,
  data = df_event_maria,
  vcov = ~ GEOID
)

#summary(sem_maria)

# IRMA

print("Running model for Irma")

df_event_irma <- stacked_event_panel %>%
  filter(!is.na(combined_local_rel_period)) %>%
  # Important modeling choice: duplicate storms are removed
  filter(!period_has_other_storm) %>%
  filter(!is.na(ntl_ihs), !is.na(hq_share_mean)) %>%
  filter(storm_name == "IRMA-2017") %>%
  check_period_support()

sem_irma <- feols(
  ntl_ihs ~ i(rel_period, treated_for_event, ref = -1) |
    GEOID + period_id,
  data = df_event_irma,
  vcov = ~ GEOID
)

#summary(sem_irma)

# MICHAEL

print("Running model for Michael")

df_event_michael <- stacked_event_panel %>%
  filter(!is.na(combined_local_rel_period)) %>%
  # Important modeling choice: duplicate storms are removed
  filter(!period_has_other_storm) %>%
  filter(!is.na(ntl_ihs), !is.na(hq_share_mean)) %>%
  filter(storm_name == "MICHAEL-2018") %>%
  check_period_support()

sem_michael <- feols(
  ntl_ihs ~ i(rel_period, treated_for_event, ref = -1) |
    GEOID + period_id,
  data = df_event_michael,
  vcov = ~ GEOID
)

#summary(sem_michael)


## DESCRIPTIVE MODELS

# MARIA

df_desc_maria <- treated_event_panel %>%
  filter(storm_name == "MARIA-2017") %>%
  filter(!is.na(local_event_period_id)) %>%
  filter(!is.na(event_storm)) %>%
  filter(!is.na(ntl_ihs)) %>%
  filter(!is.na(hq_share_mean))

mod_desc_maria <- feols(
  ntl_ihs ~ i(rel_period),
  data = df_desc_maria,
  weights = ~ hq_share_mean
)

#summary(mod_desc_maria)

# IRMA

df_desc_irma <- treated_event_panel %>%
  filter(storm_name == "IRMA-2017") %>%
  filter(!is.na(local_event_period_id)) %>%
  filter(!is.na(event_storm)) %>%
  filter(!is.na(ntl_ihs)) %>%
  filter(!is.na(hq_share_mean))

mod_desc_irma <- feols(
  ntl_ihs ~ i(rel_period),
  data = df_desc_irma,
  weights = ~ hq_share_mean
)

#summary(mod_desc_irma)

# MICHAEL

df_desc_michael <- treated_event_panel %>%
  filter(storm_name == "MICHAEL-2018") %>%
  filter(!is.na(local_event_period_id)) %>%
  filter(!is.na(event_storm)) %>%
  filter(!is.na(ntl_ihs)) %>%
  filter(!is.na(hq_share_mean))

mod_desc_michael <- feols(
  ntl_ihs ~ i(rel_period),
  data = df_desc_michael,
  weights = ~ hq_share_mean
)

#summary(mod_desc_michael)



## TABLES AND GRAPHS FOR DESCRIPTIVE MODELS

etable(mod_desc_maria)

etable(mod_desc_maria, mod_desc_irma, mod_desc_michael)

iplot(
  mod_desc_maria,
  ref.line = -1,
  main = "Linear dynamic Model for Hurricane Maria",
  xlab = "Relative period",
  ylab = "Effect on asinh(nightlights)")

iplot(
  mod_desc_irma,
  ref.line = -1,
  main = "Linear dynamic Model for Hurricane Irma",
  xlab = "Relative period",
  ylab = "Effect on asinh(nightlights)")

iplot(
  mod_desc_michael,
  ref.line = -1,
  main = "Linear dynamic Model for Hurricane Michael",
  xlab = "Relative period",
  ylab = "Effect on asinh(nightlights)")



### PROGRESS UPDATE ARTIFACTS

print("Exporting progress-update figures and table")

save_iplot <- function(mod, file, ...) {
  png(file.path("Output/model_graphs", file), width = 1600, height = 1000, res = 200)
  iplot(mod, ref.line = -1, xlab = "Relative period", ylab = "Effect on asinh(nightlights)", ...)
  dev.off()
}

save_iplot(sem_full_binary,         "fig_full_binary_granular.png", main = "Binary treatment - dynamic")
save_iplot(sem_full_prebase,        "fig_full_binary_binned.png",   main = "Binary treatment - binned periods")
save_iplot(sem_full_binned,         "fig_full_dose_granular.png",   main = "Exposure dose-response - dynamic")
save_iplot(sem_full_binned_prebase, "fig_full_dose_binned.png",     main = "Exposure dose-response - binned periods")
save_iplot(sem_maria,               "fig_maria_event.png",          main = "Maria - event study (with controls)")
save_iplot(mod_desc_maria,          "fig_maria_descriptive.png",    main = "Maria - descriptive (treated only)")

etable(
  sem_full_prebase, sem_full_binned_prebase,
  file = "Output/model_tables/table_progress_update.txt", replace = TRUE
)
