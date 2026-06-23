source("Code/get_blackmarble_data.R")

## Define NASA input_nasa_bearer token for blackmarble api ##
input_nasa_bearer <- get_nasa_token(
  username = Sys.getenv("NASA_USER"),
  password = Sys.getenv("NASA_PASS")
)

## Process data

bm_panel_path <- function(cache_key) {
  file.path("Data/bm_files/tract_day", cache_key, "bm_panel.rds")
}

date_seq_2015 <- seq.Date(from = as.Date("2015-01-01"), to = as.Date("2015-12-31"), by = "day")
date_seq_2016 <- seq.Date(from = as.Date("2016-01-01"), to = as.Date("2016-12-31"), by = "day")
date_seq_2017 <- seq.Date(from = as.Date("2017-01-01"), to = as.Date("2017-12-31"), by = "day")
date_seq_2018 <- seq.Date(from = as.Date("2018-01-01"), to = as.Date("2018-12-31"), by = "day")
date_seq_2019 <- seq.Date(from = as.Date("2019-01-01"), to = as.Date("2019-12-31"), by = "day")
date_seq_2020 <- seq.Date(from = as.Date("2020-01-01"), to = as.Date("2020-12-31"), by = "day")
date_seq_2024 <- seq.Date(from = as.Date("2024-01-01"), to = as.Date("2024-12-31"), by = "day")

ntl_variable <- "Gap_Filled_DNB_BRDF-Corrected_NTL"

# 2 removes poor-quality/outlier/potential cloud-contaminated pixels.
quality_flag_rm <- 2L

# South Carolina

if (!file.exists(bm_panel_path("sc_gapfilled_2015"))) {
  ntl_panel_sc_2015 <- build_bm_tract_panel(
    tracts_sf = county_polygons_sc,
    dates = date_seq_2015,
    bearer = input_nasa_bearer,
    ntl_variable = ntl_variable,
    quality_flag_rm = quality_flag_rm,
    quiet = FALSE,
    cache_key = "sc_gapfilled_2015",
    delete_downloads = TRUE
  )
}

if (!file.exists(bm_panel_path("sc_gapfilled_2016"))) {
  ntl_panel_sc_2016 <- build_bm_tract_panel(
    tracts_sf = county_polygons_sc,
    dates = date_seq_2016,
    bearer = input_nasa_bearer,
    ntl_variable = ntl_variable,
    quality_flag_rm = quality_flag_rm,
    quiet = FALSE,
    cache_key = "sc_gapfilled_2016",
    delete_downloads = TRUE
  )
}

if (!file.exists(bm_panel_path("sc_gapfilled_2017"))) {
  ntl_panel_sc_2017 <- build_bm_tract_panel(
    tracts_sf = county_polygons_sc,
    dates = date_seq_2017,
    bearer = input_nasa_bearer,
    ntl_variable = ntl_variable,
    quality_flag_rm = quality_flag_rm,
    quiet = FALSE,
    cache_key = "sc_gapfilled_2017",
    delete_downloads = TRUE
  )
}

if (!file.exists(bm_panel_path("sc_gapfilled_2018"))) {
  ntl_panel_sc_2018 <- build_bm_tract_panel(
    tracts_sf = county_polygons_sc,
    dates = date_seq_2018,
    bearer = input_nasa_bearer,
    ntl_variable = ntl_variable,
    quality_flag_rm = quality_flag_rm,
    quiet = FALSE,
    cache_key = "sc_gapfilled_2018",
    delete_downloads = TRUE
  )
}

if (!file.exists(bm_panel_path("sc_gapfilled_2019"))) {
  ntl_panel_sc_2019 <- build_bm_tract_panel(
    tracts_sf = county_polygons_sc,
    dates = date_seq_2019,
    bearer = input_nasa_bearer,
    ntl_variable = ntl_variable,
    quality_flag_rm = quality_flag_rm,
    quiet = FALSE,
    cache_key = "sc_gapfilled_2019",
    delete_downloads = TRUE
  )
}

if (!file.exists(bm_panel_path("sc_gapfilled_2020"))) {
  ntl_panel_sc_2020 <- build_bm_tract_panel(
    tracts_sf = county_polygons_sc,
    dates = date_seq_2020,
    bearer = input_nasa_bearer,
    ntl_variable = ntl_variable,
    quality_flag_rm = quality_flag_rm,
    quiet = FALSE,
    cache_key = "sc_gapfilled_2020",
    delete_downloads = TRUE
  )
}

if (!file.exists(bm_panel_path("sc_gapfilled_2024"))) {
  ntl_panel_sc_2024 <- build_bm_tract_panel(
    tracts_sf = county_polygons_sc,
    dates = date_seq_2024,
    bearer = input_nasa_bearer,
    ntl_variable = ntl_variable,
    quality_flag_rm = quality_flag_rm,
    quiet = FALSE,
    cache_key = "sc_gapfilled_2024",
    delete_downloads = TRUE
  )
}
