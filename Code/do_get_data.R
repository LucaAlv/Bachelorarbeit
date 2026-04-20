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
# Set relevant states
states_list <- c(
  "12", "48", "45", "37", "22", "72"
  )
states_names_list <- c(
  "Florida", "Texas", "South Carolina", "North Carolina", "Louisiana", "Puerto Rico"
  )

# Here use the tigris package to pull USCensus county shapefiles

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

county_polygons_lu <- tigris::counties(state = "22", class = "sf") %>%
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
dir.create("Data/era5_files", recursive = TRUE, showWarnings = FALSE)
dir.create("Data/pep_files", recursive = TRUE, showWarnings = FALSE)
dir.create("Data/bm_files/weekly_panels", recursive = TRUE, showWarnings = FALSE)
# Output subfolders
dir.create("Output/era5_output", recursive = TRUE, showWarnings = FALSE)
dir.create("Output/ntl_output", recursive = TRUE, showWarnings = FALSE)
dir.create("Output/ibtracs_output", recursive = TRUE, showWarnings = FALSE)





#### Load functions ####

source("Code/get_blackmarble_data.R")
source("Code/get_ibtracs_data.R")
source("Code/get_era5_data.R")
source("Code/get_census_data.R")
source("Code/get_movement_data.R")





#### Define output panel ####

# Create panel dataframe that we will join to later
out_panel_raw <- county_polygons_pr %>%
  # drop geometry to make joining easier and faster
  st_drop_geometry() %>%
  crossing(
    year = seq(
      from = 2016,
      to = 2018
    ),
    week = seq(
      from = 1,
      to = 53
    )
  )









#### Get wind data ####

## Extract storm data 

if (!file.exists("Output/ibtracs_output/storm_measures_centroid.rds") | !file.exists("Output/ibtracs_output/storm_measures_whole_county.rds"))  {
  source("Code/do_get_ibtracs_data.R")
}

## Load data and create yearly panels

storm_measures_centroid_raw <- readRDS("Output/ibtracs_output/storm_measures_centroid.rds")

storm_measures_centroid_weekly_allus_2010_2025 <- storm_measures_centroid_raw %>%
  mutate(
    year = year(date),
    week_start = floor_date(date, unit = "week", week_start = 1),
    week_end = ceiling_date(date, unit = "week", week_start = 1),
    week = week(date)
  ) %>%
  # Add days since storm variable
  group_by(GEOID) %>%
  arrange(date, .by_group = TRUE) %>%
  mutate(
    days_since_last_storm = {
      storm_day <- !is.na(tc_day) & tc_day == 1
      last_storm_day <- cummax(if_else(storm_day, as.numeric(date), -Inf))
      days_since <- as.numeric(date) - last_storm_day
      days_since[!is.finite(last_storm_day)] <- NA_real_
      as.integer(days_since)
    }
  ) %>%
  ungroup() %>%
  group_by(GEOID, year, week) %>%
  summarize(
    n_days = n(),
    max_wind = max(tc_max_wind, na.rm = TRUE),
    day_count = sum(tc_day, na.rm = TRUE),
    weeks_since_last_storm = {
      days_at_week_end <- dplyr::last(days_since_last_storm, order_by = date)
      if (is.na(days_at_week_end)) NA_integer_ else as.integer(floor(days_at_week_end / 7))
    },
    .groups = "drop"
  ) %>%
  arrange(year, week, GEOID)

saveRDS(storm_measures_centroid_weekly_allus_2010_2025, "Output/ibtracs_output/storm_measures_centroid_weekly_allus_2010_2025.rds")



storm_measures_whole_county_raw <- readRDS("Output/ibtracs_output/storm_measures_whole_county.rds")

storm_measures_whole_county_weekly_allus_2010_2025 <- storm_measures_whole_county_raw %>%
  mutate(
    year = year(date),
    week_start = floor_date(date, unit = "week", week_start = 1),
    week_end = ceiling_date(date, unit = "week", week_start = 1),
    week = week(date)
  ) %>%
  # Add days since storm variable
  group_by(GEOID) %>%
  arrange(date, .by_group = TRUE) %>%
  mutate(
    days_since_last_storm = {
      storm_day <- !is.na(tc_poly_day) & tc_poly_day == 1
      last_storm_day <- cummax(if_else(storm_day, as.numeric(date), -Inf))
      days_since <- as.numeric(date) - last_storm_day
      days_since[!is.finite(last_storm_day)] <- NA_real_
      as.integer(days_since)
    }
  ) %>%
  ungroup() %>%
  group_by(GEOID, year, week) %>%
  summarize(
    n_days = n(),
    max_wind = max(tc_poly_max_wind, na.rm = TRUE),
    mean_wind = {
      storm_days <- !is.na(tc_poly_day) & tc_poly_day == 1
      if (any(storm_days)) mean(tc_poly_mean_wind[storm_days], na.rm = TRUE) else 0
    },
    day_count = sum(tc_poly_day, na.rm = TRUE),
    weeks_since_last_storm = {
      days_at_week_end <- dplyr::last(days_since_last_storm, order_by = date)
      if (is.na(days_at_week_end)) NA_integer_ else as.integer(floor(days_at_week_end / 7))
    },
    .groups = "drop"
  ) %>%
  arrange(year, week, GEOID)

saveRDS(storm_measures_whole_county_weekly_allus_2010_2025, "Output/ibtracs_output/storm_measures_whole_county_weekly_allus_2010_2025.rds")

storm_measures_whole_county_weekly_allus_2010_2025 <- readRDS("Output/ibtracs_output/storm_measures_whole_county_weekly_allus_2010_2025.rds")

#### Delete this later ####

storm_panel <- storm_measures_whole_county_weekly_allus_2010_2025 %>%
  filter(startsWith(GEOID, "72")) %>%
  filter(year == 2016 | year == 2017 | year == 2018)














#### Get blackmarble data ####

## Process data
source("Code/do_get_ntl_data.R")

## Read in previously processed data

# Puerto Rico
ntl_panel_pr_2016_raw <- readRDS("Data/bm_files/tract_day/pr_counties_gapfilled_2016/bm_panel.rds")
ntl_panel_pr_2017_raw <- readRDS("Data/bm_files/tract_day/pr_counties_gapfilled_2017/bm_panel.rds")
ntl_panel_pr_2018_raw <- readRDS("Data/bm_files/tract_day/pr_counties_gapfilled_2018/bm_panel.rds")

# Florida
ntl_panel_fl_2016_raw <- readRDS("Data/bm_files/tract_day/fl_gapfilled_2016/bm_panel.rds")
ntl_panel_fl_2017_raw <- readRDS("Data/bm_files/tract_day/fl_gapfilled_2017/bm_panel.rds")



## Turn raw data into nice panels and combine into larger dataframe

make_ntl_panel_weekly <- function(raw_data) {

  out <- raw_data %>%
    mutate(
      year = year(date),
      week_start = floor_date(date, unit = "week", week_start = 1),
      week_end = ceiling_date(date, unit = "week", week_start = 1),
      week = week(date)
    ) %>%
    group_by(GEOID, year, week) %>%
    summarise(
      ntl_mean = if (all(is.na(ntl_mean))) NA_real_ else mean(ntl_mean, na.rm = TRUE),
      valid_pixel_share_mean = if (all(is.na(valid_pixel_share))) NA_real_ else mean(valid_pixel_share, na.rm = TRUE),
      .groups = "drop"
    ) 

  return(out)
}

ntl_panel <- rbind(
  make_ntl_panel_weekly(ntl_panel_pr_2016_raw),
  make_ntl_panel_weekly(ntl_panel_pr_2017_raw),
  make_ntl_panel_weekly(ntl_panel_pr_2018_raw)
)


saveRDS(ntl_panel, "Data/bm_files/weekly_panels/ntl_panel")





















#### Get ERA5 data ####

variables_full = c(
  "10m_u_component_of_wind",
  "10m_v_component_of_wind",
  "10m_wind_gust_since_previous_post_processing",
  "total_precipitation"
)

## Process data

source("Code/do_get_era5_data.R")

## Read in previously processed data 

era5_pr_2016_tp_panel <- readRDS("Output/era5_output/era5_pr_2016_tp_panel.rds")
era5_pr_2017_tp_panel <- readRDS("Output/era5_output/era5_pr_2017_tp_panel.rds")
era5_pr_2018_tp_panel <- readRDS("Output/era5_output/era5_pr_2018_tp_panel.rds")

## Combine into larger data frame

make_era5_panel_weekly <- function(raw_data) {

  out <- raw_data %>%
    mutate(
      week_start = floor_date(date, unit = "week", week_start = 1),
      week_end = ceiling_date(date, unit = "week", week_start = 1),
      week = week(date)
    ) %>%
    group_by(GEOID, year, week) %>%
    summarise(
      ndays = n(),
      n_hours = sum(n_hours),
      precip_total_mm = sum(precip_total_mm, na.rm = TRUE),
      precip_max_hourly_mm = max(precip_max_hourly_mm, na.rm = TRUE),
      precip_mean_hourly_mm = mean(precip_mean_hourly_mm, na.rm = TRUE),
      .groups = "drop"
    ) 

  return(out)
}

era5_panel <- rbind(
  make_era5_panel_weekly(era5_pr_2016_tp_panel),
  make_era5_panel_weekly(era5_pr_2017_tp_panel),
  make_era5_panel_weekly(era5_pr_2018_tp_panel)
)

## Output panel

out_panel <- out_panel_raw %>%
  left_join(storm_panel, by = c("GEOID", "year", "week")) %>%
  left_join(ntl_panel, by = c("GEOID", "year", "week")) %>%
  left_join(era5_panel, by = c("GEOID", "year", "week")) %>%
  left_join(
    county_polygons_pr %>% dplyr::select(GEOID, geometry),
    by = "GEOID"
  ) %>%
  st_as_sf() %>%
  arrange(year, week, GEOID)

saveRDS(out_panel, "Output/out_panel.rds")
