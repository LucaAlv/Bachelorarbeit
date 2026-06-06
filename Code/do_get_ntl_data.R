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

date_seq_2016 <- seq.Date(from = as.Date("2016-01-01"), to = as.Date("2016-12-31"), by = "day")
date_seq_2017 <- seq.Date(from = as.Date("2017-01-01"), to = as.Date("2017-12-31"), by = "day")
date_seq_2018 <- seq.Date(from = as.Date("2018-01-01"), to = as.Date("2018-12-31"), by = "day")

ntl_variable <- "Gap_Filled_DNB_BRDF-Corrected_NTL"

# 2 removes poor-quality/outlier/potential cloud-contaminated pixels.
quality_flag_rm <- 2L


# Puerto Rico

if (!file.exists(bm_panel_path("pr_gapfilled_2016"))) {
  ntl_panel_pr_2016 <- build_bm_tract_panel(
    tracts_sf = county_polygons_pr,
    dates = date_seq_2016,
    bearer = input_nasa_bearer,
    ntl_variable = ntl_variable,
    quality_flag_rm = quality_flag_rm,
    quiet = FALSE,
    cache_key = "pr_gapfilled_2016",
    delete_downloads = TRUE
  )
}

if (!file.exists(bm_panel_path("pr_gapfilled_2017"))) {
  ntl_panel_pr_2017 <- build_bm_tract_panel(
    tracts_sf = county_polygons_pr,
    dates = date_seq_2017,
    bearer = input_nasa_bearer,
    ntl_variable = ntl_variable,
    quality_flag_rm = quality_flag_rm,
    quiet = FALSE,
    cache_key = "pr_gapfilled_2017",
    delete_downloads = TRUE
  )
}

if (!file.exists(bm_panel_path("pr_gapfilled_2018"))) {
  ntl_panel_pr_2018 <- build_bm_tract_panel(
    tracts_sf = county_polygons_pr,
    dates = date_seq_2018,
    bearer = input_nasa_bearer,
    ntl_variable = ntl_variable,
    quality_flag_rm = quality_flag_rm,
    quiet = FALSE,
    cache_key = "pr_gapfilled_2018",
    delete_downloads = TRUE
  )
}


# Florida

if (!file.exists(bm_panel_path("fl_gapfilled_2016"))) {
  ntl_panel_fl_2016 <- build_bm_tract_panel(
    tracts_sf = county_polygons_fl,
    dates = date_seq_2016,
    bearer = input_nasa_bearer,
    ntl_variable = ntl_variable,
    quality_flag_rm = quality_flag_rm,
    quiet = FALSE,
    cache_key = "fl_gapfilled_2016",
    delete_downloads = TRUE
  )
}

if (!file.exists(bm_panel_path("fl_gapfilled_2017"))) {
  ntl_panel_fl_2017 <- build_bm_tract_panel(
    tracts_sf = county_polygons_fl,
    dates = date_seq_2017,
    bearer = input_nasa_bearer,
    ntl_variable = ntl_variable,
    quality_flag_rm = quality_flag_rm,
    quiet = FALSE,
    cache_key = "fl_gapfilled_2017",
    delete_downloads = TRUE
  )
}

if (!file.exists(bm_panel_path("fl_gapfilled_2018"))) {
  ntl_panel_fl_2018 <- build_bm_tract_panel(
    tracts_sf = county_polygons_fl,
    dates = date_seq_2018,
    bearer = input_nasa_bearer,
    ntl_variable = ntl_variable,
    quality_flag_rm = quality_flag_rm,
    quiet = FALSE,
    cache_key = "fl_gapfilled_2018",
    delete_downloads = TRUE
  )
}

# Texas

if (!file.exists(bm_panel_path("tx_gapfilled_2016"))) {
  ntl_panel_tx_2016 <- build_bm_tract_panel(
    tracts_sf = county_polygons_tx,
    dates = date_seq_2016,
    bearer = input_nasa_bearer,
    ntl_variable = ntl_variable,
    quality_flag_rm = quality_flag_rm,
    quiet = FALSE,
    cache_key = "tx_gapfilled_2016",
    delete_downloads = TRUE
  )
}

if (!file.exists(bm_panel_path("tx_gapfilled_2017"))) {
  ntl_panel_tx_2017 <- build_bm_tract_panel(
    tracts_sf = county_polygons_tx,
    dates = date_seq_2017,
    bearer = input_nasa_bearer,
    ntl_variable = ntl_variable,
    quality_flag_rm = quality_flag_rm,
    quiet = FALSE,
    cache_key = "tx_gapfilled_2017",
    delete_downloads = TRUE
  )
}

if (!file.exists(bm_panel_path("tx_gapfilled_2018"))) {
  ntl_panel_tx_2018 <- build_bm_tract_panel(
    tracts_sf = county_polygons_tx,
    dates = date_seq_2018,
    bearer = input_nasa_bearer,
    ntl_variable = ntl_variable,
    quality_flag_rm = quality_flag_rm,
    quiet = FALSE,
    cache_key = "tx_gapfilled_2018",
    delete_downloads = TRUE
  )
}


# North Carolina

if (!file.exists(bm_panel_path("nc_gapfilled_2016"))) {
  ntl_panel_nc_2016 <- build_bm_tract_panel(
    tracts_sf = county_polygons_nc,
    dates = date_seq_2016,
    bearer = input_nasa_bearer,
    ntl_variable = ntl_variable,
    quality_flag_rm = quality_flag_rm,
    quiet = FALSE,
    cache_key = "nc_gapfilled_2016",
    delete_downloads = TRUE
  )
}

if (!file.exists(bm_panel_path("nc_gapfilled_2017"))) {
  ntl_panel_nc_2017 <- build_bm_tract_panel(
    tracts_sf = county_polygons_nc,
    dates = date_seq_2017,
    bearer = input_nasa_bearer,
    ntl_variable = ntl_variable,
    quality_flag_rm = quality_flag_rm,
    quiet = FALSE,
    cache_key = "nc_gapfilled_2017",
    delete_downloads = TRUE
  )
}

if (!file.exists(bm_panel_path("nc_gapfilled_2018"))) {
  ntl_panel_nc_2018 <- build_bm_tract_panel(
    tracts_sf = county_polygons_nc,
    dates = date_seq_2018,
    bearer = input_nasa_bearer,
    ntl_variable = ntl_variable,
    quality_flag_rm = quality_flag_rm,
    quiet = FALSE,
    cache_key = "nc_gapfilled_2018",
    delete_downloads = TRUE
  )
}


# South Carolina

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


# Louisiana

if (!file.exists(bm_panel_path("la_gapfilled_2016"))) {
  ntl_panel_la_2016 <- build_bm_tract_panel(
    tracts_sf = county_polygons_la,
    dates = date_seq_2016,
    bearer = input_nasa_bearer,
    ntl_variable = ntl_variable,
    quality_flag_rm = quality_flag_rm,
    quiet = FALSE,
    cache_key = "la_gapfilled_2016",
    delete_downloads = TRUE
  )
}

if (!file.exists(bm_panel_path("la_gapfilled_2017"))) {
  ntl_panel_la_2017 <- build_bm_tract_panel(
    tracts_sf = county_polygons_la,
    dates = date_seq_2017,
    bearer = input_nasa_bearer,
    ntl_variable = ntl_variable,
    quality_flag_rm = quality_flag_rm,
    quiet = FALSE,
    cache_key = "la_gapfilled_2017",
    delete_downloads = TRUE
  )
}

if (!file.exists(bm_panel_path("la_gapfilled_2018"))) {
  ntl_panel_la_2018 <- build_bm_tract_panel(
    tracts_sf = county_polygons_la,
    dates = date_seq_2018,
    bearer = input_nasa_bearer,
    ntl_variable = ntl_variable,
    quality_flag_rm = quality_flag_rm,
    quiet = FALSE,
    cache_key = "la_gapfilled_2018",
    delete_downloads = TRUE
  )
}
