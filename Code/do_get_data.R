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
library(ecmwfr)
library(arrow)
library(tidycensus)
library(httr2)
library(tibble)
library(jsonlite)
library(archive)
library(fs)
library(janitor)
library(modelsummary)

######## Change only parameters in this section ########

## Set tracts to relevant geographic area - i.e. load shapefiles ##
# Set relevant states
states_list <- c(
  "12", "48", "45", "37", "22", "72"
  )
states_names_list <- c(
  "Florida", "Texas", "South Carolina", "North Carolina", "Louisiana", "Puerto Rico"
  )

states_list_filled <- c(
  "12", "48", "45", "37", "22", "72",
  "28", "01", "13", "51"
  )
states_names_list_filled <- c(
  "Florida", "Texas", "South Carolina", "North Carolina", "Louisiana", "Puerto Rico",
  "Mississippi", "Alabama", "Georgia", "Virginia"
  )

# Here use the tigris package to pull USCensus county shapefiles

county_polygons_all <- tigris::counties(state = states_list, class = "sf") %>%
  st_transform(4326) %>%
  dplyr::select(GEOID, NAME, geometry)

county_polygons_pr <- tigris::counties(state = "72", class = "sf") %>%
  st_transform(4326) %>%
  dplyr::select(GEOID, NAME, geometry)

county_polygons_allus <- tigris::counties(class = "sf") %>%
  dplyr::select(GEOID, NAME, geometry)

## Set corresponding basin for ibtracs data ##

input_basin <- "NA"

## Define NASA input_nasa_bearer token for blackmarble api ##
input_nasa_bearer <- get_nasa_token(
  username = Sys.getenv("NASA_USER"),
  password = Sys.getenv("NASA_PASS")
)

######## Begin script ########

#### Make sure the folders Code, Data, bm_files and ibtracs_files exist ####
dir.create("Code", recursive = TRUE, showWarnings = FALSE)
dir.create("Data", recursive = TRUE, showWarnings = FALSE)
dir.create("Data/bm_files", recursive = TRUE, showWarnings = FALSE)
dir.create("Data/ibtracs_files", recursive = TRUE, showWarnings = FALSE)
dir.create("Data/era5_files", recursive = TRUE, showWarnings = FALSE)
dir.create("Data/pep_files", recursive = TRUE, showWarnings = FALSE)
dir.create("Output/era5_output", recursive = TRUE, showWarnings = FALSE)

ibtracs_dest_dir <- file.path(getwd(), "Data", "ibtracs_files")
ibtracs_dest_file <- file.path(ibtracs_dest_dir, paste0("IBTrACS.NA.v04r01.nc"))

if (!file.exists(ibtracs_dest_file)) {
  source_url <- paste0("https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/v04r01/access/netcdf/IBTrACS.NA.v04r01.nc")
  curl::curl_download(source_url, destfile = ibtracs_dest_file, quiet = FALSE)
}

#### Load functions ####

source("Code/get_blackmarble_data.R")
source("Code/get_ibtracs_data.R")
source("Code/get_era5_data.R")
source("Code/get_census_data.R")
source("Code/get_movement_data.R")

#### Load geographic information on puerto rican census tracts ####

# Define region of interest (roi). The roi must be (1) an sf polygon and (2)
# in the WGS84 (epsg:4326) coordinate reference system.

# Create panel dataframe that we will join to later
out_panel <- county_polygons_all %>%
  # drop geometry to make joining easier and faster
  crossing(year = seq(
    from = 2010,
    to = 2025
  ))








#### Get wind data ####

## Make a list of all storms in relevant area and specified time frame
get_seasons <- c(2010, 2025)

storm_data_set <- defStormsDataset(
  filename = ibtracs_dest_file,
  basin = input_basin,
  seasons = get_seasons)

# Convert polygons from tracts data into one big polygon
loi_all <- county_polygons_all %>%
  # fix potentially broken geometries (i.e. weird overlaps, broken boundaries, etc.)
  st_make_valid() %>%
  # filter out missing geometrc information
  filter(!st_is_empty(geometry)) %>%
  # merge geometries into one outline
  summarise(geometry = st_union(geometry)) %>%
  # convert to sf to be sure - result is a single polygon shape
  # this defines a total area that is relevant for our analysis and less granual modelling that we need for our anlysis later
  st_as_sf()

storms_list <- defStormsList(sds = storm_data_set, loi = loi_all) %>%
  renameStorms()

## Extract storm data - only do this once

# centroid based
storm_measures_centroid <- tc_daily_panel_centroids(
  storm_list = storms_list,
  tracts = county_polygons_all,
  start_date = "2010-01-01",
  end_date = "2025-12-31",
  verbose = 1,
  fill_zeros = TRUE
)

saveRDS(storm_measures_centroid, "Output/ibtracs_output/storm_measures_centroid.rds")

# polygon (so for whole tract) based
storm_measures_whole_tract <- tc_daily_panel_from_tracts(
  storm_list = storms_list,
  tracts = county_polygons_all,
  start_date = "2010-01-01",
  end_date = "2025-12-31",
  verbose = 1,
  fill_zeros = TRUE
)

saveRDS(storm_measures_whole_tract, "Output/ibtracs_output/storm_measures_whole_tract.rds")

## Load data and create yearly panels

storm_measures_centroid_raw <- readRDS("Output/ibtracs_output/storm_measures_centroid.rds")

storm_measures_whole_tract_raw <- readRDS("Output/ibtracs_output/storm_measures_whole_tract.rds")

storm_measures_centroid_yearly <- storm_measures_centroid_raw %>%
  mutate(
    year = year(date)
  ) %>%
  group_by(year, GEOID) %>%
  summarize(
    centroid_n_days = n(),
    centroid_max_tc_max_wind = max(tc_max_wind, na.rm = TRUE),
    centroid_mean_tc_max_wind = {
      storm_days <- !is.na(tc_day) & tc_day == 1
      if (any(storm_days)) mean(tc_max_wind[storm_days], na.rm = TRUE) else 0
    },
    centroid_sd_tc_max_wind = sd(tc_max_wind, na.rm = TRUE),
    centroid_tc_day_count = sum(tc_day, na.rm = TRUE),
    .groups = "drop"
  )

storm_measures_whole_tract_yearly <- storm_measures_whole_tract_raw %>%
  mutate(
    year = year(date)
  ) %>%
  group_by(year, GEOID) %>%
  summarize(
    poly_n_days = n(),
    poly_max_tc_poly_max_wind = max(tc_poly_max_wind, na.rm = TRUE),
    poly_mean_tc_poly_mean_wind = {
      storm_days <- !is.na(tc_poly_day) & tc_poly_day == 1
      if (any(storm_days)) mean(tc_poly_mean_wind[storm_days], na.rm = TRUE) else 0
    },
    poly_sd_tc_poly_max_wind = sd(tc_poly_max_wind, na.rm = TRUE),
    poly_tc_day_count = sum(tc_poly_day, na.rm = TRUE),
    .groups = "drop"
  )

# Merge panel
out_panel_ibtracs <- out_panel %>%
  left_join(storm_measures_centroid_yearly, by = c("GEOID", "year")) %>%
  left_join(storm_measures_whole_tract_yearly, by = c("GEOID", "year")) %>%
  st_as_sf()

ggplot(filter(out_panel_ibtracs, year == 2010)) +
  geom_sf(aes(fill = centroid_tc_day_count), color = "white", linewidth = 0.2) +
  scale_fill_viridis_c(na.value = "grey90") +
  labs(
    title = "County exposure to tropical cyclones, 2010",
    fill = "TC exposure days"
  ) +
  theme_minimal()







#### Get blackmarble data ####

# Add some date columns for the graphs
ntl_panel_pr_maria <- build_bm_tract_panel(
  tracts_sf = county_polygons_pr,
  dates = seq.Date(from = as.Date("2017-08-01"), to = as.Date("2017-11-30"), by = "day"),
  bearer = input_nasa_bearer,
  ntl_variable = "Gap_Filled_DNB_BRDF-Corrected_NTL",
  quality_flag_rm = 1,
  require_night = FALSE,
  quiet = FALSE,
  cache_key = "pr_counties_gapfilled_maria"
)

ntl_panel_pr_maria_raw <- readRDS("Data/bm_files/tract_day/pr_counties_gapfilled_maria/bm_panel.rds")

dir.create("Output/ntl_output", recursive = TRUE, showWarnings = FALSE)

ntl_panel_pr_maria_weekly <- ntl_panel_pr_maria_raw %>%
  mutate(
    week_start = floor_date(date, unit = "week", week_start = 1),
    week_label = format(week_start, "%Y-%m-%d")
  ) %>%
  group_by(GEOID, week_start, week_label) %>%
  summarise(
    ntl_mean = if (all(is.na(ntl_mean))) NA_real_ else mean(ntl_mean, na.rm = TRUE),
    valid_pixel_share_mean = if (all(is.na(valid_pixel_share))) NA_real_ else mean(valid_pixel_share, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  left_join(
    county_polygons_pr %>% dplyr::select(GEOID, NAME, geometry),
    by = "GEOID"
  ) %>%
  st_as_sf() %>%
  arrange(week_start, GEOID)

ntl_week_fill_limits <- range(ntl_panel_pr_maria_weekly$ntl_mean, na.rm = TRUE)
if (!all(is.finite(ntl_week_fill_limits))) {
  ntl_week_fill_limits <- NULL
}

ntl_panel_pr_maria_weekly_grid <- ggplot(ntl_panel_pr_maria_weekly) +
  geom_sf(aes(fill = ntl_mean), color = "white", linewidth = 0.2) +
  facet_wrap(~week_label, ncol = 4) +
  scale_fill_viridis_c(
    option = "magma",
    na.value = "grey90",
    limits = ntl_week_fill_limits,
    name = "Weekly mean\nNTL"
  ) +
  labs(
    title = "Puerto Rico weekly night lights during Hurricane Maria",
    subtitle = "County-level weekly mean of Gap-Filled DNB BRDF-Corrected NTL",
    caption = "Facet labels show the week start date"
  ) +
  coord_sf(datum = NA) +
  theme_void() +
  theme(
    strip.text = element_text(face = "bold", size = 8),
    legend.position = "bottom",
    plot.title.position = "plot"
  )

ggsave(
  filename = "Output/ntl_output/pr_maria_weekly_ntl_grid.png",
  plot = ntl_panel_pr_maria_weekly_grid,
  width = 14,
  height = 18,
  dpi = 300
)











#### Get Census Data ####

pep_county_panel <- get_pep_county_panel()

saveRDS(pep_county_panel, "Output/pep_output/county_panel_2010_2025.rds")

## Read in data 

census_panel_raw <- readRDS("Output/pep_output/county_panel_2010_2025.rds")

census_panel <- census_panel_raw %>%
  filter(state_fips %in% states_list) %>%
  rename("GEOID" = "fips")

out_panel_census <- out_panel %>%
  left_join(census_panel, by = c("GEOID", "year")) 


## Aggregate all out panels into one

out_panel_final <- out_panel %>%
  filter(startsWith(GEOID, "12")) %>%
  filter(year == 2010) %>%
  # left_join(out_panel_ntl) %>%
  left_join(out_panel_ibtracs, by = join_by(GEOID, NAME, year)) %>%
  left_join(out_panel_era5, by = join_by(GEOID, NAME, year)) %>%
  left_join(out_panel_census, by = join_by(GEOID, NAME, year))


#### Summary Statistics ####

## Table I

datasummary(population + netmig ~ centroid_max_tc_max_wind + centroid_mean_tc_max_wind, out_panel_final)


datasummary(
  population + netmig + centroid_max_tc_max_wind + centroid_mean_tc_max_wind ~ Mean + SD + Min + Max,
  data = out_panel_final
)

## Storm Data

panel_pr_ntl_storm[panel_pr_ntl_storm$tc_max_wind == max(panel_pr_ntl_storm$tc_max_wind), ]

mean(panel_pr_ntl$valid_pixel_share, na.rm = TRUE)

#### Exploratory Models ####

mod1 <- lm(netmig ~ centroid_tc_day_count, data = out_panel_final[out_panel_final$stname == "Puerto Rico", ])

# during storm days

panel_pr_ntl_storm_era5 %>%
  filter()

panel_pr_ntl_storm_era5[(panel_pr_ntl_storm_era5$tc_day == 1) | (panel_pr_ntl_storm_era5$tc_poly_day == 0), ]


#### Graphs ####




## Storm Data

storm_daily_isstorm_heatmap <- ggplot(panel_pr_ntl_storm_era5_daily, aes(x = day, y = factor(month_year), fill = tc_day)) +
  geom_tile() +
  scale_fill_viridis_c(na.value = "red") +
  labs(
    x = "Day of year",
    y = "Year",
    fill = "Is there a storm?",
    title = "Seasonal pattern in storm occurance"
  ) +
  theme_minimal()

ggsave("Output/storm_daily_isstorm_heatmap.png", storm_daily_isstorm_heatmap)

storm_daily_swind_heatmap <- ggplot(panel_pr_ntl_storm_era5_daily, aes(x = day, y = factor(month_year), fill = swind)) +
  geom_tile() +
  scale_fill_viridis_c(na.value = "red") +
  labs(
    x = "Day of year",
    y = "Year",
    fill = "Is there a storm?",
    title = "Seasonal pattern in storm occurance"
  ) +
  theme_minimal()

ggsave("Output/storm_daily_swind_heatmap.png", storm_daily_swind_heatmap)
