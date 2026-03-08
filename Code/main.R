library(blackmarbler)
library(sf)
library(terra)
library(dplyr)
library(purrr)
library(tidyr)
library(lubridate)
library(exactextractr)  # for custom cloud-share extraction
library(tigris)
options(tigris_use_cache = TRUE)
library(sf)
library(tidync)
library(stringr)
library(tidyverse)
library(gganimate)
library(maps)
library(StormR)
library(terra)

######## Change only parameters in this section ########

## Set tracts to relevant geographic area - i.e. load shapefiles ##

# Here use the tigris package to pull USCensus tract shapefiles for a county
# FIPS code for puerto rico = 72
input_tracts <- tigris::tracts(state = "72", year = 2022, class = "sf") %>%
  st_transform(4326) %>%
  dplyr::select(GEOID, NAME, geometry)

## Set corresponding basin ##

input_basin <- "NA"

## Define NASA input_nasa_bearer token for blackmarble api ##
input_nasa_bearer <- get_nasa_token(
  username = Sys.getenv("NASA_USER"),
  password = Sys.getenv("NASA_PASS")
)

## Define relevant time frame ##
input_date_start <- as.Date("2023-01-01")
input_date_end <- as.Date("2023-01-31")

######## Begin script ########

#### Make sure the folders Code, Data, bm_files and ibtracs_files exist ####
dir.create("Code", recursive = TRUE, showWarnings = FALSE)
dir.create("Data", recursive = TRUE, showWarnings = FALSE)
dir.create("Data/bm_files", recursive = TRUE, showWarnings = FALSE)
dir.create("Data/ibtracs_files", recursive = TRUE, showWarnings = FALSE)

dest_dir <- file.path(getwd(), "Data", "ibtracs_files")
dest_file <- file.path(dest_dir, paste0("IBTrACS.NA.v04r01.nc"))

if (!file.exists(dest_file)) {
  source_url <- paste0("https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/v04r01/access/netcdf/IBTrACS.NA.v04r01.nc")
  curl::curl_download(source_url, destfile = dest_file, quiet = FALSE)
}

#### Load functions ####

source("Code/get_blackmarble_data.R")
source("Code/get_ibtracs_data.R")

#### Load geographic information on puerto rican counties ####

# Define region of interest (roi). The roi must be (1) an sf polygon and (2)
# in the WGS84 (epsg:4326) coordinate reference system.

# Create panel dataframe that we will join to later
panel_pr <- input_tracts %>%
  # drop geometry to make joining easier and faster
  st_drop_geometry() %>%
  crossing(date = seq.Date(
    from = input_date_start,
    to = input_date_end,
    by = "day"
  ))

#### Load blackmarble data ####

ntl_data <- build_bm_tract_panel(
  tracts_sf = input_tracts,
  dates = seq.Date(from = input_date_start, to = input_date_end, by = "day"),
  bearer = input_nasa_bearer,
  quiet = FALSE
)

panel_pr_ntl <- panel_pr %>%
  left_join(ntl_data)

#### Get wind data ####

get_seasons <- c(year(input_date_start), year(input_date_end))

# Make a list of all storms in north america between 2020 and 2025
storm_data_set <- defStormsDataset(
  filename = dest_file,
  basin = input_basin,
  seasons = get_seasons)

# Convert polygons from tracts data into one big polygon
loi_pr <- input_tracts %>%
  # fix potentially broken geometries (i.e. weird overlaps, broken boundaries, etc.)
  st_make_valid() %>%
  # filter out missing geometrc information
  filter(!st_is_empty(geometry)) %>%
  # merge geometries into one outline
  summarise(geometry = st_union(geometry)) %>%
  # convert to sf to be sure - result is a single polygon shape
  # this defines a total area that is relevant for our analysis and less granual modelling that we need for our anlysis later
  st_as_sf()

storms_list <- defStormsList(sds = storm_data_set, loi = loi_pr) %>%
  renameStorms()

storm_measures_centroid <- tc_daily_panel_centroids(
  storm_list = storms_list,
  tracts = input_tracts,
  start_date = input_date_start,
  end_date = input_date_end,
  verbose = 0,
  fill_zeros = TRUE
)

storm_measures_whole_tract <- tc_daily_panel_from_tracks(
  storm_list = storms_list,
  tracts = input_tracts,
  start_date = input_date_start,
  end_date = input_date_end,
  verbose = 0,
  fill_zeros = TRUE
)

panel_pr_ntl_storm <- panel_pr_ntl %>%
  left_join(storm_measures_centroid, by = c("GEOID", "date")) %>%
  left_join(storm_measures_whole_tract, by = c("GEOID", "date"))


#### Summary Statistics ####

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