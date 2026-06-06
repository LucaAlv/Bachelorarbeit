library(tidyverse)
library(sf)

out_dir <- "Output/data_overview"
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

out_panel <- readRDS("Data/out_panel.rds")
out_panel_geo <- if (file.exists("Data/out_panel_geo.rds")) {
  tryCatch(
    readRDS("Data/out_panel_geo.rds"),
    error = function(e) {
      message("Could not read Data/out_panel_geo.rds; skipping spatial plot. Error: ", e$message)
      NULL
    }
  )
} else {
  NULL
}

storm_period_event_panel <- attr(out_panel, "storm_period_event_panel")
storm_event_df <- attr(out_panel, "storm_event_df")
storm_event_exposure_df <- attr(out_panel, "storm_event_exposure_df")
treated_event_panel <- attr(out_panel, "treated_event_panel")
stacked_event_panel <- attr(out_panel, "stacked_event_panel")
event_window <- attr(out_panel, "event_window")

if (is.null(event_window)) {
  event_window <- -8:20
}

if (!is.null(storm_period_event_panel)) {
  storm_period_event_panel <- storm_period_event_panel %>%
    mutate(
      GEOID = as.character(GEOID),
      period_id_int = as.integer(as.character(period_id)),
      period_start = as.Date(period_start),
      period_end = as.Date(period_end)
    )

  storm_event_df <- storm_period_event_panel %>%
    arrange(storm_name, period_id_int) %>%
    group_by(storm_name) %>%
    summarise(
      storm_event_period_id = first(period_id_int),
      storm_event_period_start = first(period_start),
      storm_event_period_end = first(period_end),
      .groups = "drop"
    )

  storm_event_exposure_df <- storm_period_event_panel %>%
    arrange(GEOID, storm_name, period_id_int) %>%
    group_by(GEOID, storm_name) %>%
    summarise(
      local_event_period_id = first(period_id_int),
      local_event_period_start = first(period_start),
      local_event_period_end = first(period_end),
      event_storm = if (all(is.na(storm_period_max_wind))) {
        NA_real_
      } else {
        max(storm_period_max_wind, na.rm = TRUE)
      },
      event_storm_days = sum(storm_period_days, na.rm = TRUE),
      event_storm_weeks = n_distinct(period_id_int),
      event_has_multi_storm_period = any(multi_storm_period),
      .groups = "drop"
    )
}

if (
  !is.null(storm_event_df) &&
  !"storm_event_period_start" %in% names(storm_event_df) &&
  "event_date" %in% names(storm_event_df)
) {
  storm_event_df <- storm_event_df %>%
    mutate(storm_event_period_start = as.Date(event_date))
}

if (!is.null(storm_event_exposure_df)) {
  storm_event_exposure_df <- storm_event_exposure_df %>%
    mutate(GEOID = as.character(GEOID))
}

event_outcome_base <- out_panel %>%
  mutate(
    GEOID = as.character(GEOID),
    period_id_int = as.integer(as.character(period_id))
  ) %>%
  rename(
    period_storm_name = storm_name,
    period_storm_max_wind = storm_period_max_wind,
    period_storm_days = storm_period_days
  )

if (
  is.null(treated_event_panel) &&
  !is.null(storm_event_exposure_df) &&
  "local_event_period_id" %in% names(storm_event_exposure_df)
) {
  treated_event_panel <- storm_event_exposure_df %>%
    expand_grid(rel_period = event_window) %>%
    mutate(
      rel_period = as.integer(rel_period),
      period_id_int = local_event_period_id + rel_period
    ) %>%
    left_join(event_outcome_base, by = c("GEOID", "period_id_int")) %>%
    filter(!is.na(period_id)) %>%
    mutate(
      treated_for_event = TRUE,
      period_storm_count = if_else(
        is.na(period_storm_name) | !nzchar(period_storm_name),
        0L,
        str_count(period_storm_name, fixed(";")) + 1L
      ),
      period_has_event_storm = map2_lgl(period_storm_name, storm_name, ~ {
        if (is.na(.x) || is.na(.y)) {
          FALSE
        } else {
          .y %in% trimws(str_split(.x, pattern = ";\\s*", simplify = FALSE)[[1]])
        }
      }),
      period_has_other_storm = period_storm_count > as.integer(period_has_event_storm),
      geoid_storm_id = interaction(GEOID, storm_name, drop = TRUE)
    )
}

if (
  is.null(stacked_event_panel) &&
  !is.null(storm_event_df) &&
  !is.null(storm_event_exposure_df) &&
  "storm_event_period_id" %in% names(storm_event_df) &&
  "local_event_period_id" %in% names(storm_event_exposure_df)
) {
  event_units <- storm_event_df %>%
    crossing(GEOID = unique(event_outcome_base$GEOID)) %>%
    left_join(storm_event_exposure_df, by = c("GEOID", "storm_name")) %>%
    mutate(
      treated_for_event = !is.na(local_event_period_id),
      local_event_period_id = as.integer(local_event_period_id)
    )

  stacked_event_panel <- event_units %>%
    expand_grid(rel_period = event_window) %>%
    mutate(
      rel_period = as.integer(rel_period),
      period_id_int = storm_event_period_id + rel_period,
      local_rel_period = if_else(
        treated_for_event,
        period_id_int - local_event_period_id,
        NA_integer_
      )
    ) %>%
    left_join(event_outcome_base, by = c("GEOID", "period_id_int")) %>%
    filter(!is.na(period_id)) %>%
    mutate(
      period_storm_count = if_else(
        is.na(period_storm_name) | !nzchar(period_storm_name),
        0L,
        str_count(period_storm_name, fixed(";")) + 1L
      ),
      period_has_event_storm = map2_lgl(period_storm_name, storm_name, ~ {
        if (is.na(.x) || is.na(.y)) {
          FALSE
        } else {
          .y %in% trimws(str_split(.x, pattern = ";\\s*", simplify = FALSE)[[1]])
        }
      }),
      period_has_other_storm = period_storm_count > as.integer(period_has_event_storm),
      geoid_storm_id = interaction(GEOID, storm_name, drop = TRUE),
      storm_period_id = interaction(storm_name, period_id, drop = TRUE)
    )
}

dataset_registry <- tibble(
  dataset = c(
    "out_panel",
    "out_panel_geo",
    "storm_period_event_panel",
    "storm_event_df",
    "storm_event_exposure_df",
    "treated_event_panel",
    "stacked_event_panel",
    "event_window"
  ),
  storage = c(
    "Data/out_panel.rds",
    "Data/out_panel_geo.rds",
    'attr(out_panel, "storm_period_event_panel") / Data/event_study_files/storm_period_event_panel.rds after rebuild',
    'attr(out_panel, "storm_event_df") / Data/event_study_files/storm_event_df.rds after rebuild',
    'attr(out_panel, "storm_event_exposure_df") / Data/event_study_files/storm_event_exposure_df.rds after rebuild',
    'attr(out_panel, "treated_event_panel") / Data/event_study_files/treated_event_panel.rds after rebuild',
    'attr(out_panel, "stacked_event_panel") / Data/event_study_files/stacked_event_panel.rds after rebuild',
    'attr(out_panel, "event_window")'
  ),
  rows = c(
    nrow(out_panel),
    if (is.null(out_panel_geo)) NA_integer_ else nrow(out_panel_geo),
    if (is.null(storm_period_event_panel)) NA_integer_ else nrow(storm_period_event_panel),
    if (is.null(storm_event_df)) NA_integer_ else nrow(storm_event_df),
    if (is.null(storm_event_exposure_df)) NA_integer_ else nrow(storm_event_exposure_df),
    if (is.null(treated_event_panel)) NA_integer_ else nrow(treated_event_panel),
    if (is.null(stacked_event_panel)) NA_integer_ else nrow(stacked_event_panel),
    length(event_window)
  ),
  columns = c(
    ncol(out_panel),
    if (is.null(out_panel_geo)) NA_integer_ else ncol(out_panel_geo),
    if (is.null(storm_period_event_panel)) NA_integer_ else ncol(storm_period_event_panel),
    if (is.null(storm_event_df)) NA_integer_ else ncol(storm_event_df),
    if (is.null(storm_event_exposure_df)) NA_integer_ else ncol(storm_event_exposure_df),
    if (is.null(treated_event_panel)) NA_integer_ else ncol(treated_event_panel),
    if (is.null(stacked_event_panel)) NA_integer_ else ncol(stacked_event_panel),
    1L
  ),
  source = c(
    "TIGER county polygons x seven-day calendar, joined to IBTrACS storm exposure, Black Marble night lights, and ERA5 weather.",
    "out_panel joined back to TIGER county geometries.",
    "IBTrACS daily whole-county storm data grouped by GEOID, period, and storm_name.",
    "storm_period_event_panel grouped to one row per storm.",
    "storm_period_event_panel grouped to one row per exposed GEOID and storm.",
    "storm_event_exposure_df expanded over event_window and joined to out_panel outcomes.",
    "storm_event_df crossed with all GEOIDs, expanded over event_window, treatment added from storm_event_exposure_df, then joined to out_panel outcomes.",
    "Relative periods used to expand event-study panels."
  ),
  key = c(
    "GEOID + period_id",
    "GEOID + period_id + geometry",
    "GEOID + period_id + storm_name",
    "storm_name",
    "GEOID + storm_name",
    "GEOID + storm_name + rel_period",
    "storm_name + GEOID + rel_period",
    "rel_period"
  ),
  purpose = c(
    "Main analysis panel for TWFE/distributed-lag models and descriptive outcome/weather summaries.",
    "Mapping version of the main panel.",
    "Audit trail for storm-specific exposures before collapsing to event-study units.",
    "Storm-level event calendar and timing reference.",
    "County-storm treatment intensity table: timing, max wind, days exposed, and multi-storm flags.",
    "Treated-only event-study panel for visualizing dynamic paths around exposed county-storm events.",
    "Stacked event-study design with treated and untreated counties for each storm.",
    "Defines how many leads and lags are included in treated/stacked event panels."
  )
) %>%
  mutate(available = !is.na(rows))

write_csv(dataset_registry, file.path(out_dir, "final_dataset_overview.csv"))

variable_overview <- tibble(variable = names(out_panel)) %>%
  mutate(
    source_domain = case_when(
      variable %in% c("GEOID", "period_id", "period_start", "period_end") ~ "Panel keys/calendar",
      str_detect(variable, "^storm_|^period_storm_|storm_name") ~ "IBTrACS storm exposure",
      str_detect(variable, "^ntl_|cloud_free|hq_share") ~ "Black Marble night lights and quality",
      str_detect(variable, "precip|windgust") ~ "ERA5 weather",
      TRUE ~ "Other"
    ),
    role = case_when(
      variable %in% c("GEOID", "period_id") ~ "Panel identifier",
      variable %in% c("period_start", "period_end") ~ "Calendar timing",
      variable %in% c("ntl_mean", "ntl_log", "ntl_ihs") ~ "Outcome candidate",
      variable %in% c("cloud_free_share_mean", "hq_share_mean") ~ "Quality control / regression weight candidate",
      variable %in% c("storm_period_max_wind", "storm_period_days", "precip_total_mm", "precip_max_hourly_mm", "max_windgust_max_mps") ~ "Treatment or weather shock candidate",
      variable == "storm_name" ~ "Descriptive storm label in the collapsed period panel",
      TRUE ~ "Auxiliary measure"
    )
  )

write_csv(variable_overview, file.path(out_dir, "out_panel_variable_overview.csv"))

md_escape <- function(x) {
  str_replace_all(as.character(x), "\\|", "/")
}

markdown_table <- dataset_registry %>%
  transmute(
    Dataset = dataset,
    Rows = if_else(is.na(rows), "not available", as.character(rows)),
    Columns = if_else(is.na(columns), "not available", as.character(columns)),
    Source = source,
    Purpose = purpose
  )

markdown_lines <- c(
  "# Final Dataset Overview",
  "",
  paste0("Generated from `Data/out_panel.rds` on ", Sys.time(), "."),
  "",
  "| Dataset | Rows | Columns | Source | Purpose |",
  "|---|---:|---:|---|---|",
  pmap_chr(markdown_table, function(Dataset, Rows, Columns, Source, Purpose) {
    paste(
      "",
      md_escape(Dataset),
      md_escape(Rows),
      md_escape(Columns),
      md_escape(Source),
      md_escape(Purpose),
      "",
      sep = " | "
    )
  }),
  "",
  "See `out_panel_variable_overview.csv` for column-level source domains and roles."
)

writeLines(markdown_lines, file.path(out_dir, "final_dataset_overview.md"))

dataset_registry %>%
  filter(available, dataset != "event_window") %>%
  mutate(dataset = fct_reorder(dataset, rows)) %>%
  ggplot(aes(x = rows, y = dataset)) +
  geom_col(fill = "#3f6b7f") +
  scale_x_log10(labels = scales::label_comma()) +
  labs(
    title = "Final Dataset Sizes",
    x = "Rows, log scale",
    y = NULL
  ) +
  theme_minimal(base_size = 12)

ggsave(file.path(out_dir, "dataset_sizes.png"), width = 8, height = 4.5, dpi = 300)

out_panel_time <- out_panel %>%
  mutate(
    has_ibtracs_storm = !is.na(storm_name),
    storm_period_max_wind_zero = replace_na(storm_period_max_wind, 0),
    state_fips = substr(as.character(GEOID), 1, 2)
  ) %>%
  group_by(period_start) %>%
  summarise(
    counties = n(),
    storm_counties = sum(has_ibtracs_storm),
    mean_ntl_ihs = mean(ntl_ihs, na.rm = TRUE),
    mean_precip_total_mm = mean(precip_total_mm, na.rm = TRUE),
    mean_era5_windgust_mps = mean(max_windgust_max_mps, na.rm = TRUE),
    max_ibtracs_wind = max(storm_period_max_wind_zero, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  pivot_longer(
    cols = c(storm_counties, mean_ntl_ihs, mean_precip_total_mm, mean_era5_windgust_mps, max_ibtracs_wind),
    names_to = "measure",
    values_to = "value"
  ) %>%
  mutate(
    measure = recode(
      measure,
      storm_counties = "Counties with named IBTrACS storm",
      mean_ntl_ihs = "Mean asinh(NTL)",
      mean_precip_total_mm = "Mean weekly precipitation",
      mean_era5_windgust_mps = "Mean ERA5 max wind gust",
      max_ibtracs_wind = "Max IBTrACS wind"
    )
  )

out_panel_time %>%
  ggplot(aes(x = period_start, y = value)) +
  geom_line(color = "#355c7d", linewidth = 0.6) +
  facet_wrap(vars(measure), scales = "free_y", ncol = 1) +
  labs(
    title = "Main Panel Over Time",
    x = NULL,
    y = NULL
  ) +
  theme_minimal(base_size = 12)

ggsave(file.path(out_dir, "out_panel_time_series.png"), width = 9, height = 8, dpi = 300)

out_panel %>%
  mutate(
    state_fips = substr(as.character(GEOID), 1, 2),
    state = recode(
      state_fips,
      "12" = "Florida",
      "22" = "Louisiana",
      "37" = "North Carolina",
      "45" = "South Carolina",
      "48" = "Texas",
      "72" = "Puerto Rico",
      .default = state_fips
    ),
    has_storm = !is.na(storm_name)
  ) %>%
  group_by(state) %>%
  summarise(
    observations = n(),
    storm_periods = sum(has_storm),
    share_storm_periods = mean(has_storm),
    mean_ntl_ihs = mean(ntl_ihs, na.rm = TRUE),
    mean_precip_total_mm = mean(precip_total_mm, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  pivot_longer(
    cols = c(share_storm_periods, mean_ntl_ihs, mean_precip_total_mm),
    names_to = "measure",
    values_to = "value"
  ) %>%
  mutate(
    measure = recode(
      measure,
      share_storm_periods = "Share of county-periods with named storm",
      mean_ntl_ihs = "Mean asinh(NTL)",
      mean_precip_total_mm = "Mean weekly precipitation"
    )
  ) %>%
  ggplot(aes(x = reorder(state, value), y = value)) +
  geom_col(fill = "#4f7f67") +
  coord_flip() +
  facet_wrap(vars(measure), scales = "free_x") +
  labs(
    title = "Main Panel by State",
    x = NULL,
    y = NULL
  ) +
  theme_minimal(base_size = 12)

ggsave(file.path(out_dir, "out_panel_state_summary.png"), width = 10, height = 5.5, dpi = 300)

if (!is.null(storm_event_df) && !is.null(storm_event_exposure_df)) {
  storm_event_plot_df <- storm_event_df %>%
    left_join(
      storm_event_exposure_df %>%
        group_by(storm_name) %>%
        summarise(
          affected_counties = n_distinct(GEOID),
          max_event_wind = max(event_storm, na.rm = TRUE),
          median_event_days = median(event_storm_days, na.rm = TRUE),
          .groups = "drop"
        ),
      by = "storm_name"
    ) %>%
    mutate(
      storm_event_period_start = as.Date(storm_event_period_start),
      storm_name = fct_reorder(storm_name, storm_event_period_start)
    )

  storm_event_plot_df %>%
    ggplot(aes(x = storm_event_period_start, y = storm_name)) +
    geom_point(aes(size = affected_counties, color = max_event_wind), alpha = 0.85) +
    scale_color_viridis_c(option = "plasma", na.value = "grey70") +
    scale_size_area(max_size = 9) +
    labs(
      title = "Storm Event Calendar",
      subtitle = "Point size is affected county count; color is max county-level storm wind.",
      x = NULL,
      y = NULL,
      color = "Max wind",
      size = "Affected counties"
    ) +
    theme_minimal(base_size = 12)

  ggsave(file.path(out_dir, "storm_event_calendar.png"), width = 9, height = 6.5, dpi = 300)

  storm_event_exposure_df %>%
    ggplot(aes(x = event_storm, y = event_storm_days)) +
    geom_point(aes(color = event_has_multi_storm_period), alpha = 0.45, size = 1.6) +
    scale_color_manual(values = c("FALSE" = "#3f6b7f", "TRUE" = "#b85c38")) +
    labs(
      title = "County-Storm Exposure Distribution",
      subtitle = "Each point is one exposed county-storm pair.",
      x = "Max storm wind in event",
      y = "Storm-exposed days",
      color = "Multi-storm period"
    ) +
    theme_minimal(base_size = 12)

  ggsave(file.path(out_dir, "storm_event_exposure_distribution.png"), width = 8, height = 5.5, dpi = 300)
}

if (!is.null(treated_event_panel)) {
  treated_event_panel %>%
    count(rel_period, name = "rows") %>%
    ggplot(aes(x = rel_period, y = rows)) +
    geom_col(fill = "#5b6f9f") +
    geom_vline(xintercept = 0, linetype = 2) +
    labs(
      title = "Treated Event Panel Coverage",
      subtitle = "Rows by relative period. Lower counts at edges come from sample boundaries.",
      x = "Relative period",
      y = "Rows"
    ) +
    theme_minimal(base_size = 12)

  ggsave(file.path(out_dir, "treated_event_window_coverage.png"), width = 8, height = 4.5, dpi = 300)

  treated_event_panel %>%
    group_by(rel_period) %>%
    summarise(
      mean_ntl_ihs = mean(ntl_ihs, na.rm = TRUE),
      mean_event_wind = mean(period_storm_max_wind, na.rm = TRUE),
      share_other_storm = mean(period_has_other_storm, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    pivot_longer(
      cols = c(mean_ntl_ihs, mean_event_wind, share_other_storm),
      names_to = "measure",
      values_to = "value"
    ) %>%
    mutate(
      measure = recode(
        measure,
        mean_ntl_ihs = "Mean asinh(NTL)",
        mean_event_wind = "Mean period storm wind",
        share_other_storm = "Share with another storm in period"
      )
    ) %>%
    ggplot(aes(x = rel_period, y = value)) +
    geom_line(color = "#355c7d", linewidth = 0.7) +
    geom_vline(xintercept = 0, linetype = 2) +
    facet_wrap(vars(measure), scales = "free_y", ncol = 1) +
    labs(
      title = "Treated Event Panel Paths",
      x = "Relative period",
      y = NULL
    ) +
    theme_minimal(base_size = 12)

  ggsave(file.path(out_dir, "treated_event_panel_paths.png"), width = 8, height = 7, dpi = 300)
}

if (!is.null(stacked_event_panel)) {
  stacked_event_panel %>%
    count(storm_name, treated_for_event, name = "rows") %>%
    group_by(storm_name) %>%
    mutate(total_rows = sum(rows)) %>%
    ungroup() %>%
    mutate(storm_name = fct_reorder(storm_name, total_rows)) %>%
    ggplot(aes(x = storm_name, y = rows, fill = treated_for_event)) +
    geom_col() +
    coord_flip() +
    scale_fill_manual(values = c("FALSE" = "#b7b7b7", "TRUE" = "#3f6b7f")) +
    labs(
      title = "Stacked Event Panel Design",
      subtitle = "Rows by storm and treatment status.",
      x = NULL,
      y = "Rows",
      fill = "Treated"
    ) +
    theme_minimal(base_size = 12)

  ggsave(file.path(out_dir, "stacked_event_panel_design.png"), width = 9, height = 7, dpi = 300)
}

if (!is.null(out_panel_geo) && inherits(out_panel_geo, "sf")) {
  out_panel_geo %>%
    mutate(storm_period_max_wind_zero = replace_na(storm_period_max_wind, 0)) %>%
    group_by(GEOID) %>%
    summarise(
      max_storm_wind = max(storm_period_max_wind_zero, na.rm = TRUE),
      mean_ntl_ihs = mean(ntl_ihs, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    ggplot() +
    geom_sf(aes(fill = max_storm_wind), color = "white", linewidth = 0.05) +
    scale_fill_viridis_c(option = "magma", name = "Max wind") +
    coord_sf(datum = NA) +
    labs(
      title = "Maximum Storm Wind by County",
      subtitle = "Computed from the final main panel across all periods."
    ) +
    theme_void(base_size = 12) +
    theme(legend.position = "bottom")

  ggsave(file.path(out_dir, "map_max_storm_wind.png"), width = 10, height = 6, dpi = 300)
}
