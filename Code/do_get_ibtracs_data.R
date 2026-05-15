## This is a helper file that runs the get_ibtracs functions
## if the files don't already exist

## Load and set ibtracs file
ibtracs_dest_dir <- file.path(getwd(), "Data", "ibtracs_files")
ibtracs_dest_file <- file.path(ibtracs_dest_dir, paste0("IBTrACS.NA.v04r01.nc"))

if (!file.exists(ibtracs_dest_file)) {
  source_url <- paste0("https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/v04r01/access/netcdf/IBTrACS.NA.v04r01.nc")
  curl::curl_download(source_url, destfile = ibtracs_dest_file, quiet = FALSE)
}

## Set corresponding basin for ibtracs data ##

input_basin <- "NA"

## Make a list of all storms in relevant area and specified time frame
get_seasons <- c(2010, 2025)
get_seasons <- c(2010, 2010)

storm_data_set <- defStormsDataset(
  filename = ibtracs_dest_file,
  basin = input_basin,
  seasons = get_seasons)

# Convert polygons from county data into one big polygon
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

# Note: it is important that empirical RMW = TRUE
# Without this there is a weird "bug" that for weak storms with huge ibtracs rmw stormr's
# willoughby inner-core exponent can become negative
# this leads to the formula exploding near the storm center

storm_measures_centroid_twfe_file <- "Data/ibtracs_files/panels/storm_measures_centroid_twfe.rds"
storm_measures_centroid_event_file <- "Data/ibtracs_files/panels/storm_measures_centroid_event.rds"

storm_measures_whole_county_twfe_file <- "Data/ibtracs_files/panels/storm_measures_whole_county_twfe.rds"
storm_measures_whole_county_event_file <- "Data/ibtracs_files/panels/storm_measures_whole_county_event_file.rds"

start_date <- "2010-01-01"
end_date <- "2010-12-31"

# centroid based

if (!file.exists(storm_measures_centroid_twfe_file)) {

  storm_measures_centroid_twfe <- tc_daily_panel_centroids(
    storm_list = storms_list,
    tracts = county_polygons_all,
    start_date = start_date,
    end_date = end_date,
    tz = "local",
    empiricalRMW = TRUE,
    verbose = 2,
    fill_zeros = TRUE
  )

  saveRDS(storm_measures_centroid_twfe, storm_measures_centroid_twfe_file)

}

if (!file.exists(storm_measures_centroid_event_file)) {

  storm_measures_centroid_event <- tc_daily_panel_centroids(
    storm_list = storms_list,
    tracts = county_polygons_all,
    start_date = start_date,
    end_date = end_date,
    tz = "local",
    empiricalRMW = TRUE,
    verbose = 2,
    fill_zeros = FALSE
  )

  saveRDS(storm_measures_centroid_event, storm_measures_centroid_event_file)

}

# polygon (so for whole county) based

if (!file.exists(storm_measures_whole_county_twfe_file)) {

  storm_measures_whole_county_twfe <- tc_daily_panel_polygons(
    storm_list = storms_list,
    tracts = county_polygons_all,
    start_date = start_date,
    end_date = end_date,
    tz = "local",
    empiricalRMW = TRUE,
    verbose = 2,
    fill_zeros = TRUE
  )
  
  saveRDS(storm_measures_whole_county_twfe, storm_measures_whole_county_twfe_file)

}

if (!file.exists(storm_measures_whole_county_event_file)) {

  storm_measures_whole_county_event <- tc_daily_panel_polygons(
    storm_list = storms_list,
    tracts = county_polygons_all,
    start_date = start_date,
    end_date = end_date,
    tz = "local",
    empiricalRMW = TRUE,
    verbose = 2,
    fill_zeros = FALSE
  )
  
  saveRDS(storm_measures_whole_county_event, storm_measures_whole_county_event_file)

}