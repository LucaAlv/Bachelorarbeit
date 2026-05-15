## The main goal of this file is to create one final panel 
## It calls the various get_ and do_get files to do this

#### Load packages ####

library(tidyverse)
library(blackmarbler)
library(sf)
library(terra)
library(purrr)
library(lubridate)
library(exactextractr)  # for custom cloud-share extraction
library(tigris)
options(tigris_use_cache = TRUE)
library(tidync)
library(StormR)
library(terra)
library(ecmwfr)
library(arrow)
library(tidycensus)
library(httr2)
library(jsonlite)
library(archive)
library(fs)
library(janitor)



# For now do everything for 2016 -> 2018 for Puerto Rico

#### Set relevant states and load county shapefiles ####

# 12: Florida
# 48: Texas
# 45: South Carolina
# 37: North Carolina
# 22: Louisiana
# 72: Puerto Rico

states_list <- c(
  "12", "48", "45", "37", "22", "72"
  )
states_names_list <- c(
  "Florida", "Texas", "South Carolina", "North Carolina", "Louisiana", "Puerto Rico"
  )
state_file_prefixes <- c("fl", "tx", "sc", "nc", "la", "pr")

# Use the tigris package to pull USCensus county shapefiles

county_polygons_all <- tigris::counties(state = states_list, class = "sf") %>%
  st_transform(4326) %>%
  dplyr::select(GEOID, NAME, geometry)

county_polygons_fl <- tigris::counties(state = "12", class = "sf") %>%
  st_transform(4326) %>%
  dplyr::select(GEOID, NAME, geometry)

county_polygons_tx <- tigris::counties(state = "48", class = "sf") %>%
  st_transform(4326) %>%
  dplyr::select(GEOID, NAME, geometry)

county_polygons_sc <- tigris::counties(state = "45", class = "sf") %>%
  st_transform(4326) %>%
  dplyr::select(GEOID, NAME, geometry)

county_polygons_nc <- tigris::counties(state = "37", class = "sf") %>%
  st_transform(4326) %>%
  dplyr::select(GEOID, NAME, geometry)

county_polygons_la <- tigris::counties(state = "22", class = "sf") %>%
  st_transform(4326) %>%
  dplyr::select(GEOID, NAME, geometry)

county_polygons_pr <- tigris::counties(state = "72", class = "sf") %>%
  st_transform(4326) %>%
  dplyr::select(GEOID, NAME, geometry)







#### Make sure directories exist ####
# Global folders
dir.create("Code", recursive = TRUE, showWarnings = FALSE)
dir.create("Data", recursive = TRUE, showWarnings = FALSE)
dir.create("Output", recursive = TRUE, showWarnings = FALSE)
# Data subfolders
dir.create("Data/bm_files", recursive = TRUE, showWarnings = FALSE)
dir.create("Data/ibtracs_files", recursive = TRUE, showWarnings = FALSE)
dir.create("Data/ibtracs_files/panels", recursive = TRUE, showWarnings = FALSE)
dir.create("Data/ibtracs_files/raw", recursive = TRUE, showWarnings = FALSE)
dir.create("Data/era5_files", recursive = TRUE, showWarnings = FALSE)
dir.create("Data/pep_files", recursive = TRUE, showWarnings = FALSE)
dir.create("Data/bm_files/period_panels", recursive = TRUE, showWarnings = FALSE)
# Output subfolders
dir.create("Output/era5_output", recursive = TRUE, showWarnings = FALSE)
dir.create("Output/ntl_output", recursive = TRUE, showWarnings = FALSE)
dir.create("Output/ibtracs_output", recursive = TRUE, showWarnings = FALSE)







#### Load functions ####

source("Code/get_blackmarble_data.R")
source("Code/get_ibtracs_data.R")
source("Code/get_era5_data.R")












#### Define output panel ####

panel_years <- 2016:2018
panel_start_date <- as.Date(paste0(min(panel_years), "-01-01"))
panel_end_date <- as.Date(paste0(max(panel_years), "-12-31"))
period_days <- 7L

# Weekly time intervals are weird because number of days usually doesn't perfectly divide by number of weeks
# This function solves this by creating periods (that are very similar to weeks) but flow across years
# Resulting in only one period (the last one) that may not have exactly seven days

dates <- seq.Date(
  from = panel_start_date,
  to = panel_end_date,
  by = "day"
)

time_calendar <- tibble(date = dates) %>%
  mutate(
    period_index = as.integer(date - panel_start_date) %/% period_days,
    period_id = period_index + 1L,
    period_start = panel_start_date + period_days * period_index,
    period_end = period_start + period_days - 1L,
    year = year(period_start)
  ) %>%
  select(-period_index)

time_periods <- time_calendar %>%
  count(
    period_id,
    period_start,
    period_end,
    year,
    name = "period_days_in_sample"
  ) %>%
  mutate(
    period_label = paste(period_start, period_end, sep = "_"),
    interval_days = period_days
  ) %>%
  arrange(period_id)

time_join_keys <- c("GEOID", "period_id", "period_start", "period_end", "year")

add_period_vars <- function(data) {
  data %>%
    mutate(date = as.Date(date)) %>%
    filter(date >= panel_start_date, date <= panel_end_date) %>%
    select(-any_of(c("period_id", "period_start", "period_end", "year"))) %>%
    left_join(time_calendar, by = "date")
}

# Function to prevent having to write too many readRDS statements
read_panel_rds_files <- function(file_grid, label) {
  missing_files <- file_grid$file[!file.exists(file_grid$file)]

  if (length(missing_files) > 0) {
    stop(
      "Missing ",
      label,
      " file(s):\n",
      paste(missing_files, collapse = "\n"),
      call. = FALSE
    )
  }

  file_grid %>%
    mutate(data = map(file, readRDS)) %>%
    pull(data) %>%
    bind_rows()
}

# Create panel dataframe that we will join to later
out_panel_raw <- county_polygons_all %>%
  # drop geometry to make joining easier and faster
  st_drop_geometry() %>%
  crossing(time_periods)

















#### Get storm wind data ####

## Extract storm data 

storm_measures_centroid_twfe_file <- "Data/ibtracs_files/panels/storm_measures_centroid_twfe.rds"
storm_measures_centroid_event_file <- "Data/ibtracs_files/panels/storm_measures_centroid_event.rds"

storm_measures_whole_county_twfe_file <- "Data/ibtracs_files/panels/storm_measures_whole_county_twfe.rds"
storm_measures_whole_county_event_file <- "Data/ibtracs_files/panels/storm_measures_whole_county_event_file.rds"

if (
  !file.exists(storm_measures_centroid_twfe_file) |
  !file.exists(storm_measures_centroid_event_file) |
  !file.exists(storm_measures_whole_county_twfe_file) |
  !file.exists(storm_measures_whole_county_event_file)
)  {
  source("Code/do_get_ibtracs_data.R")
}

# Small helper function to concatenate the storm names when aggregating to 7 day intervals
collapse_period_storm_names <- function(x) {
  x <- x[!is.na(x) & nzchar(x)]

  # If there is no storm in this period
  if (length(x) == 0) {
    return(NA_character_)
  }

  storm_names <- stringr::str_split(x, pattern = ";\\s*", simplify = FALSE) %>%
    unlist(use.names = FALSE) %>%
    trimws()

  storm_names <- sort(unique(storm_names[nzchar(storm_names)]))

  if (length(storm_names) == 0) NA_character_ else paste(storm_names, collapse = "; ")
}

## Load data and create 7-day period panels

storm_measures_centroid_period_file <- file.path(
  "Data/ibtracs_files/panels",
  paste0(
    "storm_measures_centroid_",
    period_days,
    "day_allus_",
    min(panel_years),
    "_",
    max(panel_years),
    ".rds"
  )
)

storm_measures_whole_county_period_file <- file.path(
  "Data/ibtracs_files/panels",
  paste0(
    "storm_measures_whole_county_",
    period_days,
    "day_allus_",
    min(panel_years),
    "_",
    max(panel_years),
    ".rds"
  )
)

if (!exists(storm_measures_centroid_period_file)) {

  storm_measures_centroid_raw <- readRDS(storm_measures_centroid_file)

  storm_measures_centroid_period <- storm_measures_centroid_raw %>%
    add_period_vars() %>%
    group_by(GEOID, period_id, period_start, period_end, year) %>%
    summarize(
      n_days_cent = n(),
      storm_names_cent = collapse_period_storm_names(storm_name),
      # max max wind at centroid
      max_max_wind_cent = max(tc_max_wind, na.rm = TRUE),
      # mean max wind at centroid (i.e. we have a centroid max for every day of the period - this is the mean of these)
      mean_max_wind_cent = mean(tc_max_wind, na.rm = TRUE),
      day_count_cent = sum(tc_day, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    arrange(period_id, GEOID)

  saveRDS(storm_measures_centroid_period, storm_measures_centroid_period_file)

} else {
  storm_measures_centroid_period <- readRDS(storm_measures_centroid_period_file)
}

if (!exists(storm_measures_whole_county_period_file)) {

  storm_measures_whole_county_raw <- readRDS(storm_measures_whole_county_file)

  storm_measures_whole_county_period <- storm_measures_whole_county_raw %>%
    add_period_vars() %>%
    group_by(GEOID, period_id, period_start, period_end, year) %>%
    summarize(
      n_days_poly = n(),
      # max max wind over whole county (i.e. each county had one point with maximum wind exposure each day - this doesn't have to be the centroid) of these we take the period max
      storm_names_poly = collapse_period_storm_names(storm_name),
      max_max_wind_poly = max(tc_poly_max_wind, na.rm = TRUE),
      # mean max wind
      mean_max_wind_poly = mean(tc_poly_max_wind, na.rm = TRUE),
      # max mean wind
      max_mean_wind_poly = max(tc_poly_mean_wind, na.rm = TRUE),
      # mean mean wind over whole county (i.e. we have the mean wind speed over the whole county for every day - from these we take the mean)
      # the problem here with taking the mean is, that on days where tc_poly_mean_wind is 0 the wind speed doesn't actually have to be 0
      # for these days there is just no official storm recording so stormR didn't create any values for this
      # I manually filled these days up with zeroes
      # just taking the mean here thus would make the strong assumption that there was no wind outside of the storms
      mean_mean_wind_poly = {
        storm_days <- !is.na(tc_poly_day) & tc_poly_day == 1
        if (any(storm_days)) mean(tc_poly_mean_wind[storm_days], na.rm = TRUE) else 0
      },
      day_count_poly = sum(tc_poly_day, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    arrange(period_id, GEOID)

  saveRDS(storm_measures_whole_county_period, storm_measures_whole_county_period_file)

} else {
  storm_measures_whole_county_period <- readRDS(storm_measures_whole_county_period_file)
}

storm_panel <- storm_measures_whole_county_period %>%
  left_join(storm_measures_centroid_period, by = time_join_keys)














#### Get blackmarble data ####

## Process data
source("Code/do_get_ntl_data.R")

## Read in previously processed data

ntl_panel_files <- crossing(
  state = state_file_prefixes,
  year = panel_years
) %>%
  mutate(
    file = file.path(
      "Data/bm_files/tract_day",
      paste0(state, "_gapfilled_", year),
      "bm_panel.rds"
    )
  )


## Turn raw data into nice panels and combine into larger dataframe

make_ntl_panel_period <- function(raw_data) {

  out <- raw_data %>%
    add_period_vars() %>%
    group_by(GEOID, period_id, period_start, period_end, year) %>%
    summarise(
      ntl_days_in_period = n(),
      ntl_valid_days = sum(!is.na(ntl_mean)),
      ntl_mean = if (all(is.na(ntl_mean))) NA_real_ else mean(ntl_mean, na.rm = TRUE),
      valid_pixel_share_mean = if (all(is.na(valid_pixel_share))) NA_real_ else mean(valid_pixel_share, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    arrange(period_id, GEOID)

  return(out)
}

ntl_panel_raw <- read_panel_rds_files(ntl_panel_files, "Black Marble")
ntl_panel <- make_ntl_panel_period(ntl_panel_raw)























#### Get ERA5 data ####

variables_full = c(
  "10m_wind_gust_since_previous_post_processing",
  "total_precipitation"
)

## Process data

source("Code/do_get_era5_data.R")

## Read in previously processed data 

era5_tp_files <- crossing(
  state = state_file_prefixes,
  year = panel_years
) %>%
  mutate(
    file = file.path(
      "Data/era5_files/panels",
      paste0("era5_", state, "_", year, "_tp_panel.rds")
    )
  )

era5_wg_files <- crossing(
  state = state_file_prefixes,
  year = panel_years
) %>%
  mutate(
    file = file.path(
      "Data/era5_files/panels",
      paste0("era5_", state, "_", year, "_wg_panel.rds")
    )
  )

## Combine into larger data frame

make_era5_tp_panel_period <- function(raw_data) {

  out <- raw_data %>%
    add_period_vars() %>%
    group_by(GEOID, period_id, period_start, period_end, year) %>%
    summarise(
      era5_tp_days_in_period = n(),
      era5_tp_hours = sum(n_hours, na.rm = TRUE),
      precip_total_mm = sum(precip_total_mm, na.rm = TRUE),
      precip_max_hourly_mm = max(precip_max_hourly_mm, na.rm = TRUE),
      precip_mean_hourly_mm = mean(precip_mean_hourly_mm, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    arrange(period_id, GEOID)

  return(out)
}

make_era5_wg_panel_period <- function(raw_data) {

  out <- raw_data %>%
    add_period_vars() %>%
    group_by(GEOID, period_id, period_start, period_end, year) %>%
    summarise(
      era5_wg_days_in_period = n(),
      era5_wg_hours = sum(n_hours, na.rm = TRUE),
      max_windgust_max_mps = max(windgust_max_hourly_mps, na.rm = TRUE),
      mean_windgust_max_mps = mean(windgust_max_hourly_mps, na.rm = TRUE),
      max_windgust_mean_mps = max(windgust_mean_hourly_mps, na.rm = TRUE),
      mean_windgust_mean_mps = mean(windgust_mean_hourly_mps, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    arrange(period_id, GEOID)

  return(out)
}

era5_panel_tp_raw <- read_panel_rds_files(era5_tp_files, "ERA5 precipitation")
era5_panel_tp <- make_era5_tp_panel_period(era5_panel_tp_raw)

era5_panel_wg_raw <- read_panel_rds_files(era5_wg_files, "ERA5 wind gust")
era5_panel_wg <- make_era5_wg_panel_period(era5_panel_wg_raw)

era5_panel <- left_join(era5_panel_tp, era5_panel_wg, by = time_join_keys)

















## Output panel

out_panel <- out_panel_raw %>%
  left_join(storm_panel, by = time_join_keys) %>%
  left_join(ntl_panel, by = time_join_keys) %>%
  left_join(era5_panel, by = time_join_keys) %>%
  left_join(
    county_polygons_all %>% dplyr::select(GEOID, geometry),
    by = "GEOID"
  ) %>%
  st_as_sf() %>%
  arrange(period_id, GEOID)

saveRDS(out_panel, "Data/out_panel.rds")
