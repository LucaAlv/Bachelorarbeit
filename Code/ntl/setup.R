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