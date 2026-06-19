## The main goal of this file is to create one final panel 
## It calls the various get_ and do_get files to do this

#### Load packages ####

print("Loading packages")

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
library(data.table)



# For now do everything for 2016 -> 2018 for Puerto Rico

#### Set relevant states and load county shapefiles ####

print("Loading shapefiles")

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
#state_file_prefixes <- c("pr")

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

print("Creating directories")

# Global folders
dir.create("Code", recursive = TRUE, showWarnings = FALSE)
dir.create("Data", recursive = TRUE, showWarnings = FALSE)
dir.create("Output", recursive = TRUE, showWarnings = FALSE)

# Data subfolders
# BM
dir.create("Data/bm_files", recursive = TRUE, showWarnings = FALSE)
dir.create("Data/bm_files/period_panels", recursive = TRUE, showWarnings = FALSE)
dir.create("Data/bm_files/h5", recursive = TRUE, showWarnings = FALSE)
dir.create("Data/bm_files/tract_day", recursive = TRUE, showWarnings = FALSE)
dir.create("Data/bm_files/ntl_daily", recursive = TRUE, showWarnings = FALSE)
dir.create("Data/bm_files/cloud_daily", recursive = TRUE, showWarnings = FALSE)
dir.create("Data/bm_files/hq_daily", recursive = TRUE, showWarnings = FALSE)
dir.create("Data/bm_files/logs", recursive = TRUE, showWarnings = FALSE)
# ibtracs
dir.create("Data/ibtracs_files", recursive = TRUE, showWarnings = FALSE)
dir.create("Data/ibtracs_files/panels", recursive = TRUE, showWarnings = FALSE)
dir.create("Data/ibtracs_files/propanels", recursive = TRUE, showWarnings = FALSE)
dir.create("Data/ibtracs_files/raw", recursive = TRUE, showWarnings = FALSE)
# event study
dir.create("Data/event_study_files", recursive = TRUE, showWarnings = FALSE)
# era5
dir.create("Data/era5_files", recursive = TRUE, showWarnings = FALSE)
# pep
dir.create("Data/pep_files", recursive = TRUE, showWarnings = FALSE)

# Output subfolders
dir.create("Output/era5_output", recursive = TRUE, showWarnings = FALSE)
dir.create("Output/ntl_output", recursive = TRUE, showWarnings = FALSE)
dir.create("Output/ibtracs_output", recursive = TRUE, showWarnings = FALSE)
dir.create("Output/data_output", recursive = TRUE, showWarnings = FALSE)








#### Helper Functions ####

# Function to prevent having to write too many readRDS statements
read_panel_rds_files <- function(file_grid, label) {
  if (is.character(file_grid) || inherits(file_grid, "fs_path")) {
    file_grid <- tibble(file = as.character(file_grid))
  }

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












#### Define output panel ####

print("Preparing output panel")

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

# Set up "calendar" with period indizes for each seven day period
time_calendar <- tibble(date = dates) %>%
  mutate(
    # create a temporary index for every week 
    period_index = as.integer(date - panel_start_date) %/% period_days,
    # turn index into id that acutally starts at 1
    period_id = period_index + 1L,
    period_start = panel_start_date + period_days * period_index,
    period_end = period_start + period_days - 1L,
    year = year(period_start)
  ) %>%
  select(-period_index)

# Create counter for how many days are in a period
# And a period label 
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

# Small helper function to make joining less repetitive
time_join_keys <- c("GEOID", "period_id", "period_start", "period_end", "year")

add_period_vars <- function(data) {
  data %>%
    mutate(date = as.Date(date)) %>%
    filter(date >= panel_start_date, date <= panel_end_date) %>%
    select(-any_of(c("period_id", "period_start", "period_end", "year"))) %>%
    left_join(time_calendar, by = "date")
}

# Create panel dataframe that we will join to later
out_panel_raw <- county_polygons_all %>%
  # drop geometry to make joining easier and faster
  st_drop_geometry() %>%
  # add time information and make panel
  crossing(time_periods)

















#### Get storm wind data ####

print("Settin up storm data")

## Extract storm data 

# Set paths for raw daily panel files

storm_measures_centroid_file <- "Data/ibtracs_files/panels/storm_measures_centroid.rds"
storm_measures_whole_county_file <- "Data/ibtracs_files/panels/storm_measures_whole_county.rds"

storm_measures_centroid_measuredRMW_file <- "Data/ibtracs_files/panels/storm_measures_centroid_measuredRMW.rds"
storm_measures_whole_county_measuredRMW_file <- "Data/ibtracs_files/panels/storm_measures_whole_county_measuredRMW.rds"

# Set dates
start_date <- "2010-01-01"
end_date <- "2025-12-31"

# Run preprocessing if necessary
if (
  !file.exists(storm_measures_centroid_file) |
  !file.exists(storm_measures_whole_county_file)
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

  storm_names <- str_split(x, pattern = ";\\s*", simplify = FALSE) %>%
    unlist(use.names = FALSE) %>%
    trimws()

  storm_names <- sort(unique(storm_names[nzchar(storm_names)]))

  if (length(storm_names) == 0) NA_character_ else paste(storm_names, collapse = "; ")
}



make_ibtracs_panel_period <- function(raw_data) {

  out <- raw_data %>%
    add_period_vars() %>%
    group_by(GEOID, period_id, period_start, period_end, year) %>%
    summarize(
      storm_period_max_wind = if (all(is.na(tc_poly_max_wind))) {
        NA_real_
      } else {
        max(tc_poly_max_wind, na.rm = TRUE)
      },
      storm_name = collapse_period_storm_names(storm_name),
      storm_period_days = sum(tc_poly_day, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    arrange(period_id, GEOID)

  return(out)
}

# This function builds a storm-period event panel
make_storm_period_event_panel <- function(raw_data) {

  # Raw data has one row per GEOID x day
  out <- raw_data %>%
    add_period_vars() %>%
    # Drop all county-days with no storm 
    filter(!is.na(storm_name), nzchar(storm_name)) %>%

    # At this point this is still daily
    # The next step will make this weekly (or actually periodly)

    # Note the important grouping here is by GEOID, period_id and storm_name
    # So there is a unique row per GEOID, period_id and storm
    # This is basically just weekly aggregation
    # The only special thing here is that there can be multiple rows per county x period if there are multiple storms
    group_by(GEOID, period_id, period_start, period_end, year, storm_name) %>%
    summarize(
      storm_period_max_wind = if (all(is.na(tc_poly_max_wind))) {
        NA_real_
      } else {
        max(tc_poly_max_wind, na.rm = TRUE)
      },
      storm_period_days = sum(tc_poly_day, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    # Check if there are multiple storms per geoid
    group_by(GEOID, period_id) %>%
    mutate(
      storms_in_period = n_distinct(storm_name),
      multi_storm_period = storms_in_period > 1
    ) %>%
    ungroup() %>%
    arrange(period_id, GEOID, storm_name)

  return(out)
}

# The main panel is going to be the non-zero-filled whole county data.
# In this IBTrACS panel, non-storm days have tc_poly_day == 0 but wind is NA.
# raw = daily
ibtracs_panel_raw <- read_panel_rds_files(storm_measures_whole_county_file, "IBTrACS whole-county")

# turn daily panel into weekly
# only difference between these two panels is in the case where a county experiences two storms at the same time
# the ibtracs panel will still keep one row per period x GEOID and simply concatenate the storm names

## THIS IS THE TWFE OUTCOME PANEL:
ibtracs_panel <- make_ibtracs_panel_period(ibtracs_panel_raw)




## PREPARE EVENT STUDY PANEL
# the storm_period_event_panel (which is made for the event study) will keep a separate row for each storm
# this means that there can be more than one row for every GEOID x period combination
# apart from that this is the same as the ibtracs panel above
storm_period_event_panel <- make_storm_period_event_panel(ibtracs_panel_raw)

# Do some formatting
storm_period_event_panel_clean <- storm_period_event_panel %>%
  # Remove last and incomplete period
  filter(period_id != 157) %>%
  mutate(
    period_id = as.integer(period_id),
    period_start = as.Date(period_start),
    period_end = as.Date(period_end)
  ) %>%
  select(-year) %>%
  arrange(period_id, GEOID, storm_name)

# List of storms with the first period. This is the storm-level event timing.
storm_event_df <- storm_period_event_panel_clean %>%
  arrange(storm_name, period_id) %>%
  group_by(storm_name) %>%
  summarise(
    # this is the first period where the storm appears anywhere in the sample
    storm_event_period_id = first(period_id), 
    storm_event_period_start = first(period_start),
    storm_event_period_end = first(period_end),
    .groups = "drop"
   )

# Define county-specifiy exposure across periods
# Exposure data with max wind and amount of exposed days for every storm in every affected geoid.
# The local event timing can differ from the storm-level timing when counties are reached in different periods.
storm_event_exposure_df <- storm_period_event_panel_clean %>%
  arrange(GEOID, storm_name, period_id) %>%
  group_by(GEOID, storm_name) %>%
  summarise(
    local_event_period_id = first(period_id), # this is the first period in which the specific county is affected by a specific storm
    local_event_period_start = first(period_start),
    local_event_period_end = first(period_end),
    # this is the maximum wind in a county for every storm across periods 
    event_storm = if (all(is.na(storm_period_max_wind))) {
      NA_real_
    } else {
      max(storm_period_max_wind, na.rm = TRUE)
    },
    # This is how many days a county was exposed to a certain storm
    event_storm_days = sum(storm_period_days, na.rm = TRUE),
    # This is how many weeks a county was exposed to a certain storm
    event_storm_weeks = n_distinct(period_id),
    event_has_multi_storm_period = any(multi_storm_period),
    .groups = "drop"
  )










#### Get blackmarble data ####

print("Setting up blackmarble data")

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
      cloud_free_share_mean = if (all(is.na(cloud_free_share))) NA_real_ else mean(cloud_free_share, na.rm = TRUE),
      hq_share_mean = if (all(is.na(hq_share))) NA_real_ else mean(hq_share, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    arrange(period_id, GEOID)

  return(out)
}

ntl_panel_raw <- read_panel_rds_files(ntl_panel_files, "Black Marble")
ntl_panel <- make_ntl_panel_period(ntl_panel_raw)
















#### Get ERA5 data ####

print("Preparing ERA5 data")

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















#### BUILD POOLED OUT PANEL

print("Setting up pooled panel")

out_panel_noclean <- out_panel_raw %>%
  left_join(ibtracs_panel, by = time_join_keys) %>%
  left_join(ntl_panel, by = time_join_keys) %>%
  left_join(era5_panel, by = time_join_keys)

nrow(filter(out_panel_noclean, interval_days != 7))
nrow(filter(out_panel_noclean, ntl_days_in_period != 7))
nrow(filter(out_panel_noclean, era5_tp_days_in_period != 7))
nrow(filter(out_panel_noclean, era5_wg_days_in_period != 7))
nrow(filter(out_panel_noclean, period_days_in_sample != 7))

nrow(filter(out_panel_noclean, era5_tp_hours != 168))
nrow(filter(out_panel_noclean, era5_wg_hours != 168))
# In every GEOID at year end there is a small bug
# I am assigning the 00:00 UTC value to the previous day
# So the 2016-12-31 value gets assigned to 2017-01-01
# But I am also filtering for the year in do_get_era5_data
# So this one hour value gets dropped from both years and thus from the final panel
# This is minor, but should probably fix this


nrow(filter(out_panel_noclean, valid_pixel_share_mean != 1))
# Here, the quality filtering seems to not have worked perfectly
# Valid pixel share is still consistently very high here, so probably no problem
nrow(filter(out_panel_noclean, ntl_valid_days != 7))

out_panel <- out_panel_noclean %>%
  filter(period_id != 157) %>% # Leave out incomplete final period
  mutate(
    GEOID = as.factor(GEOID),
    period_start = as.Date(period_start),
    period_end = as.Date(period_end),
    period_id = as.factor(period_id),
    storm_period_max_wind = replace_na(storm_period_max_wind, 0),
    storm_period_days = replace_na(storm_period_days, 0),
    storm_is_happening = ifelse(is.na(storm_period_max_wind) | storm_period_max_wind == 0, 0, 1),
    ntl_log = log1p(ntl_mean),
    ntl_ihs = asinh(ntl_mean),
  ) %>% 
  relocate(GEOID, period_id, period_start, period_end, storm_name, storm_period_max_wind, ntl_mean, ntl_log, ntl_ihs, cloud_free_share_mean, hq_share_mean, precip_total_mm, precip_max_hourly_mm, max_windgust_max_mps) %>%
  select(-NAME, -year, -period_label, -interval_days, -ntl_days_in_period, -ntl_valid_days, -era5_tp_days_in_period, -era5_tp_hours, -era5_wg_days_in_period, -era5_wg_hours, -period_days_in_sample, -valid_pixel_share_mean) %>%
  arrange(period_id, GEOID)

out_panel_geo <- out_panel %>%
  left_join(
    county_polygons_all %>% select(GEOID, geometry),
    by = "GEOID"
  ) %>%
  st_as_sf()






#### BUILD EVENT OUT PANEL

print("Setting up event panel")

event_window <- -8:20

# Outcome columns are joined after the final panel is built. This avoids treating
# storm_name in pre/post periods as if it were the event identifier.
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

# County-specific event time: one row per exposed GEOID x storm x relative period.
# Note this panel only has actually treated counties, since we are building it from storm_event_exposure_df
# There are no non-treated control counties in here
# Remember storm_event_exposure_df has one row per exposed GEOID x storm
# The period per row is the period where the GEOID gets hit the first time
treated_event_panel <- storm_event_exposure_df %>%
  mutate(
    GEOID = as.character(GEOID),
    # Reminder local_event_period_id is the first period when a county is hit by a storm
    local_event_period_id = as.integer(local_event_period_id)
  ) %>%
  # This expands the data, by duplicating every row a bunch of times
  expand_grid(rel_period = event_window) %>%
  mutate(
    # rel_period is measured relative to the storm-level event period, not necessarily the county's own first exposure period
    rel_period = as.integer(rel_period),
    # This is now a moving index
    # Before the local index was the same for every county x GEOID
    period_id_int = local_event_period_id + rel_period
  ) %>%
  # Add ibtracs data outcome and controls
  left_join(event_outcome_base, by = c("GEOID", "period_id_int")) %>%
  filter(!is.na(period_id)) %>%
  mutate(
    # this is a non-time-varying variable saying was this county ever affected by this storm
    # counties for which this is false may appear as control counties in a given stack
    treated_for_event = TRUE,
    period_storm_count = map_int(period_storm_name, ~ {
      if (is.na(.x) || !nzchar(.x)) {
        0L
      } else {
        storm_names <- str_split(.x, pattern = ";\\s*", simplify = FALSE)[[1]] %>%
          trimws()

        length(unique(storm_names[nzchar(storm_names)]))
      }
    }),
    period_has_event_storm = map2_lgl(period_storm_name, storm_name, ~ {
      if (is.na(.x) || is.na(.y)) {
        FALSE
      } else {
        storm_names <- str_split(.x, pattern = ";\\s*", simplify = FALSE)[[1]] %>%
          trimws()

        .y %in% storm_names
      }
    }),
    period_has_other_storm = period_storm_count > as.integer(period_has_event_storm),
    geoid_storm_id = interaction(GEOID, storm_name, drop = TRUE)
  ) %>%
  arrange(storm_name, GEOID, period_id_int)



# event units a dataset with one row for every treated period and every GEOID. In the treated GEOIDs it has storm data
# storm_event_df is a list of all storms with the period where it first occured in the data
event_units <- storm_event_df %>%
  # Expand this to every GEOID
  # So now there is one row for every storm x GEOID
  crossing(GEOID = unique(event_outcome_base$GEOID)) %>%
  left_join(
    # Remember storm_event_exposure_df has one row per exposed GEOID x storm
    # It also has information on max windspeed per GEOID and storm
    storm_event_exposure_df %>%
      mutate(GEOID = as.character(GEOID)),
    by = c("GEOID", "storm_name")
  ) %>%

  # Now we have a dataset of all GEOIDs but only for periods that at some point have some exposure

  mutate(
    treated_for_event = !is.na(local_event_period_id),
    local_event_period_id = as.integer(local_event_period_id)
  )

treated_event_panel_geo <- treated_event_panel %>%
  left_join(
    county_polygons_all %>% select(GEOID, geometry),
    by = "GEOID"
  ) %>%
  st_as_sf()

# Storm-stack event time: all counties for each storm, with untreated controls.
stacked_event_panel_nw <- event_units %>%
  # Expand to the event window
  # For every row we had before
  # We now have 29 versions of thid row, one for every period relative to when the storm first hit the dataset
  # I.e. rel_period is 0 in the period where the storm first hit the dataset
  expand_grid(rel_period = event_window) %>%
  mutate(
    rel_period = as.integer(rel_period),

    # build the event window around the period where the storm first hit the dataset
    # storm_event_period_id is precisely this period (where the storm first entered the data)
    # so for rel_period = 0, we have the period where the storm first entered the data
    period_id_int = storm_event_period_id + rel_period,

    # this says where each treated county is relative to its own first exposure
    # this may differ from the rel_period above that tracks the period relative to the storm's first overall exposure
    # Reminder local_event_period_id is the first period when a county is hit by a storm
    local_rel_period = if_else(
      treated_for_event,
      # This essentially shifts the period
      period_id_int - local_event_period_id,
      NA_integer_
    )
  ) %>%
  # here the outcome and control vars are added back
  # this join means for a GEOID and actual calendar period
  # what storms, if any, affected the county
  left_join(event_outcome_base, by = c("GEOID", "period_id_int")) %>%
  # With this filter quite a few rows are removed from the data
  # These removed rows are boundary rows that extend beyond the valid range
  # E.g. for data from 2016-2018 these are periods that are already in 2019
  # since we don't actually have control and outcome data for this the period_id is also NA
  filter(!is.na(period_id)) %>%
  mutate(
    period_storm_count = map_int(period_storm_name, ~ {
      if (is.na(.x) || !nzchar(.x)) {
        0L
      } else {
        storm_names <- str_split(.x, pattern = ";\\s*", simplify = FALSE)[[1]] %>%
          trimws()

        length(unique(storm_names[nzchar(storm_names)]))
      }
    }),
    # Check if the stack storm is present in period_storm_name in this actual period
    period_has_event_storm = map2_lgl(period_storm_name, storm_name, ~ {
      if (is.na(.x) || is.na(.y)) {
        FALSE
      } else {
        storm_names <- str_split(.x, pattern = ";\\s*", simplify = FALSE)[[1]] %>%
          trimws()

        .y %in% storm_names
      }
    }),
    period_has_other_storm = period_storm_count > as.integer(period_has_event_storm),
    # Construct FEs
    geoid_storm_id = interaction(GEOID, storm_name, drop = TRUE),
    storm_period_id = interaction(storm_name, period_id, drop = TRUE),
    # Replace NAs
    event_storm = replace_na(event_storm, 0),
    event_storm_days = replace_na(event_storm_days, 0),
    event_storm_weeks = replace_na(event_storm_weeks, 0),
    event_has_multi_storm_period = replace_na(event_has_multi_storm_period, FALSE),
    combined_local_rel_period = ifelse(
      treated_for_event,
      as.integer(local_rel_period),
      as.integer(rel_period)
    ),
    severity_group = case_when(
      !treated_for_event ~ "Control",
      event_storm <= quantile(event_storm[treated_for_event], 1/3, na.rm = TRUE) ~ "Low",
      event_storm <= quantile(event_storm[treated_for_event], 2/3, na.rm = TRUE) ~ "Medium",
      treated_for_event ~ "High"
    ),
    treated_low = severity_group == "Low",
    treated_medium = severity_group == "Medium",
    treated_high = severity_group == "High"
  ) %>%
  arrange(storm_name, period_id_int, GEOID)

# Compute weights function - source Coady Wing, Alex Hollingsworth, and Seth Freedman
compute_weights = function(dataset, treatedVar, eventTimeVar, subexpVar) {

  # Create a copy of the underlying dataset
  stack_dt_temp = as.data.table(copy(dataset))

  # Step 1: Compute stack - time counts for treated and control
  stack_dt_temp[, `:=` (stack_n = .N,
                     stack_treat_n = sum(get(treatedVar)),
                     stack_control_n = sum(1 - get(treatedVar))), 
             by = get(eventTimeVar)
             ]  
  # Step 2: Compute sub_exp-level counts
  stack_dt_temp[, `:=` (sub_n = .N,
                     sub_treat_n = sum(get(treatedVar)),
                     sub_control_n = sum(1 - get(treatedVar))
                     ), 
             by = list(get(subexpVar), get(eventTimeVar))
             ]
  
  # Step 3: Compute sub-experiment share of totals
  stack_dt_temp[, sub_share := sub_n / stack_n]
  
  stack_dt_temp[, `:=` (sub_treat_share = sub_treat_n / stack_treat_n,
                     sub_control_share = sub_control_n / stack_control_n
                     )
             ]
  
  # Step 4: Compute weights for treated and control groups
  stack_dt_temp[get(treatedVar) == 1, stack_weight := 1]
  stack_dt_temp[get(treatedVar) == 0, stack_weight := sub_treat_share/sub_control_share]
  
  return(stack_dt_temp)
}  

stacked_event_panel <- compute_weights(dataset = stacked_event_panel_nw, treatedVar = "treated_for_event", eventTimeVar = "rel_period", subexpVar = "storm_name")

#### SAVE FILES

print("Saving files")

# Cleanly separate the output panels
# Out panel carries extra event_study tables as attributes
# The main object remains the weekly county panel
# Its rows are still one row per GEOID x period_id
# storm_name can still be a combination of multiple storms
attr(out_panel, "storm_period_event_panel") <- storm_period_event_panel_clean
attr(out_panel, "storm_event_exposure_df") <- storm_event_exposure_df
attr(out_panel, "event_window") <- event_window
attr(out_panel, "treated_event_panel") <- treated_event_panel
attr(out_panel, "treated_event_panel_geo") <- treated_event_panel_geo
attr(out_panel, "stacked_event_panel") <- stacked_event_panel

saveRDS(out_panel, "Output/data_output/out_panel.rds")
saveRDS(out_panel_geo, "Output/data_output/out_panel_geo.rds")
saveRDS(storm_period_event_panel_clean, "Output/data_output/storm_period_event_panel.rds")
saveRDS(storm_event_exposure_df, "Output/data_output/storm_event_exposure_df.rds")
saveRDS(treated_event_panel, "Output/data_output/treated_event_panel.rds")
saveRDS(treated_event_panel_geo, "Output/data_output/treated_event_panel_geo.rds")
saveRDS(stacked_event_panel, "Output/data_output/stacked_event_panel.rds")
