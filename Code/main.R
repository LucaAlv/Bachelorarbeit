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

#### Make sure the folders Code, Data, bm_files and ibtracs_files exist ####
dir.create("Code", recursive = TRUE, showWarnings = FALSE)
dir.create("Data", recursive = TRUE, showWarnings = FALSE)
dir.create("Data/bm_files", recursive = TRUE, showWarnings = FALSE)
dir.create("Data/ibtracs_files", recursive = TRUE, showWarnings = FALSE)

#### Load functions ####

source("Code/get_blackmarble_data.R")
source("Code/get_ibtracs_data.R")

#### Load geographic information on puerto rican counties ####

# Define region of interest (roi). The roi must be (1) an sf polygon and (2)
# in the WGS84 (epsg:4326) coordinate reference system.
# Here use the tigris package to pull USCensus tract shapefiles for a county
# FIPS code for puerto rico = 72
tracts_pr <- tigris::tracts(state = "72", year = 2022, class = "sf") %>%
  st_transform(4326) %>%
  dplyr::select(GEOID, NAME, geometry)

#### Load blackmarble data ####

# Define NASA bearer token
bearer <- get_nasa_token(
  username = Sys.getenv("NASA_USER"),
  password = Sys.getenv("NASA_PASS")
)

# Define relevant time frames
dates_total <- seq.Date(from = ymd("2023-01-01"), to = ymd("2026-01-01"), by = "day")  # General window
dates_ernesto <- seq.Date(from = ymd("2024-08-10"), to = ymd("2024-09-10"), by = "day")  # Ernesto window example
date_yearly <- c(2023, 2024, 2025)
date_test <- "2024-08-10"

build_bm_tract_panel(
  tracts_sf = tracts_pr,
  dates = date_test,
  bearer = bearer,
  quiet = FALSE
)

test <- readRDS("Data/bm_files/tract_day/pr_tract_day_panel.rds")

#### Load ibtracs data ####

base_df <- get_ibtracs_data(keep_meta = TRUE)
ernesto_vis_df <- get_ibtracs_data(output_type = "visualization", sid_input = "2024225N14313")

dest_dir <- file.path(getwd(), "Data", "ibtracs_files")
dest_file <- file.path(dest_dir, paste0("IBTrACS.NA.v04r01.nc"))

if (!file.exists(dest_file)) {
  source_url <- paste0("https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/v04r01/access/netcdf/IBTrACS.NA.v04r01.nc")
  curl::curl_download(source_url, destfile = dest_file, quiet = FALSE)
}

#### Reformat the shapefiles into the format required by the stormwindsmodel ####

tracts_pr_centroids <- tracts_pr %>% 
  st_make_valid() %>%
  st_centroid() 
  
tracts_pr_coords <- tracts_pr_centroids %>%
  st_coordinates()

pr_plot_with_centroids <- ggplot() +
  geom_sf(data = tracts_pr) +
  geom_sf(data = tracts_pr_centroids, color = "red", size = 0.6)

#### Some visualizations ####

## Visualize ernesto ##

make_storm_plot <- function(ibtracs_df, animate = FALSE) {
  # Plot focused on puerto rico
  pr_plot <- ggplot() +
    geom_sf(data = tracts_pr, 
            fill = NA, 
            color = "grey60", 
            linewidth = 0.2) +
    geom_point(data = ibtracs_df,
               aes(x = longitude, y = latitude, color = wind),
               size = 2) +
    scale_color_viridis_c() +
    coord_sf(xlim = c(-70, -62), ylim = c(17, 20)) +
    theme_minimal()

    print(pr_plot)

  # Animated plot focused on puerto rico
  if (animate == TRUE) {
    pr_plot_animated <- ggplot() +
      geom_sf(data = tracts_pr, 
              fill = NA, 
              color = "grey60", 
              linewidth = 0.2) +
      geom_point(data = ibtracs_df,
                 aes(x = longitude, y = latitude, color = wind),
                 size = 2) +
      scale_color_viridis_c() +
      coord_sf(xlim = c(-70, -62), ylim = c(17, 20)) +
      theme_minimal() +
      transition_time(iso_time)
    
    anim_save("Output/pr_storm.gif", animation = animate(pr_plot_animated))
  }

  # Plot showing americas
  sa_plot <- ggplot(ibtracs_df, aes(longitude, latitude, color = wind)) +
    annotation_borders("world", colour = "gray70", fill = "gray90") +
    geom_point(size = 2) +
    scale_color_viridis_c() +
    coord_sf(xlim = c(-140, -30), ylim = c(0, 75))

  print(sa_plot)

}

make_storm_plot(ernesto_vis_df)

#### Run stormwindmodel to get windspeeds for each county ####



sds <- defStormsDataset(
  filename = dest_file,
  basin = "NA",
  seasons = c(2020, 2025))

st <- defStormsList(sds = sds, loi = tracts_pr)

pf <- spatialBehaviour(
  st,
  product = "Profiles",
  method = "Willoughy",
  asymmetry = "Chen"
)
