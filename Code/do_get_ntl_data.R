## Define NASA input_nasa_bearer token for blackmarble api ##
input_nasa_bearer <- get_nasa_token(
  username = Sys.getenv("NASA_USER"),
  password = Sys.getenv("NASA_PASS")
)

## Process data


# Puerto Rico

if (!file.exists("Data/bm_files/tract_day/pr_gapfilled_2016/bm_panel.rds")) {
  ntl_panel_pr_2016 <- build_bm_tract_panel(
    tracts_sf = county_polygons_pr,
    dates = seq.Date(from = as.Date("2016-01-01"), to = as.Date("2016-12-31"), by = "day"),
    bearer = input_nasa_bearer,
    ntl_variable = "Gap_Filled_DNB_BRDF-Corrected_NTL",
    quality_flag_rm = 1,
    quiet = FALSE,
    cache_key = "pr_gapfilled_2016",
    delete_downloads = TRUE 
  )
}

if (!file.exists("Data/bm_files/tract_day/pr_gapfilled_2017/bm_panel.rds")) {
  ntl_panel_pr_2017 <- build_bm_tract_panel(
    tracts_sf = county_polygons_pr,
    dates = seq.Date(from = as.Date("2017-01-01"), to = as.Date("2017-12-31"), by = "day"),
    bearer = input_nasa_bearer,
    ntl_variable = "Gap_Filled_DNB_BRDF-Corrected_NTL",
    quality_flag_rm = 1,
    quiet = FALSE,
    cache_key = "pr_gapfilled_2017",
    delete_downloads = TRUE 
  )
}

if (!file.exists("Data/bm_files/tract_day/pr_gapfilled_2018/bm_panel.rds")) {
  ntl_panel_pr_2018 <- build_bm_tract_panel(
    tracts_sf = county_polygons_pr,
    dates = seq.Date(from = as.Date("2018-01-01"), to = as.Date("2018-12-31"), by = "day"),
    bearer = input_nasa_bearer,
    ntl_variable = "Gap_Filled_DNB_BRDF-Corrected_NTL",
    quality_flag_rm = 1,
    quiet = FALSE,
    cache_key = "pr_gapfilled_2018",
    delete_downloads = TRUE 
  )
}


# Florida

if (!file.exists("Data/bm_files/tract_day/fl_gapfilled_2016/bm_panel.rds")) {
  ntl_panel_fl_2016 <- build_bm_tract_panel(
    tracts_sf = county_polygons_fl,
    dates = seq.Date(from = as.Date("2016-01-01"), to = as.Date("2016-12-31"), by = "day"),
    bearer = input_nasa_bearer,
    ntl_variable = "Gap_Filled_DNB_BRDF-Corrected_NTL",
    quality_flag_rm = 1,
    quiet = FALSE,
    cache_key = "fl_gapfilled_2016",
    delete_downloads = TRUE 
  )
}

if (!file.exists("Data/bm_files/tract_day/fl_gapfilled_2017/bm_panel.rds")) {
  ntl_panel_fl_2017 <- build_bm_tract_panel(
    tracts_sf = county_polygons_fl,
    dates = seq.Date(from = as.Date("2017-01-01"), to = as.Date("2017-12-31"), by = "day"),
    bearer = input_nasa_bearer,
    ntl_variable = "Gap_Filled_DNB_BRDF-Corrected_NTL",
    quality_flag_rm = 1,
    quiet = FALSE,
    cache_key = "fl_gapfilled_2017",
    delete_downloads = TRUE 
  )
}


# Texas

if (!file.exists("Data/bm_files/tract_day/tx_gapfilled_2016/bm_panel.rds")) {
  ntl_panel_tx_2016 <- build_bm_tract_panel(
    tracts_sf = county_polygons_tx,
    dates = seq.Date(from = as.Date("2016-01-01"), to = as.Date("2016-12-31"), by = "day"),
    bearer = input_nasa_bearer,
    ntl_variable = "Gap_Filled_DNB_BRDF-Corrected_NTL",
    quality_flag_rm = 1,
    quiet = FALSE,
    cache_key = "tx_gapfilled_2016",
    delete_downloads = TRUE 
  )
}

if (!file.exists("Data/bm_files/tract_day/tx_gapfilled_2017/bm_panel.rds")) {
  ntl_panel_tx_2017 <- build_bm_tract_panel(
    tracts_sf = county_polygons_tx,
    dates = seq.Date(from = as.Date("2017-01-01"), to = as.Date("2017-12-31"), by = "day"),
    bearer = input_nasa_bearer,
    ntl_variable = "Gap_Filled_DNB_BRDF-Corrected_NTL",
    quality_flag_rm = 1,
    quiet = FALSE,
    cache_key = "tx_gapfilled_2017",
    delete_downloads = TRUE 
  )
}


# North Carolina

if (!file.exists("Data/bm_files/tract_day/nc_gapfilled_2016/bm_panel.rds")) {
  ntl_panel_nc_2016 <- build_bm_tract_panel(
    tracts_sf = county_polygons_nc,
    dates = seq.Date(from = as.Date("2016-01-01"), to = as.Date("2016-12-31"), by = "day"),
    bearer = input_nasa_bearer,
    ntl_variable = "Gap_Filled_DNB_BRDF-Corrected_NTL",
    quality_flag_rm = 1,
    quiet = FALSE,
    cache_key = "nc_gapfilled_2016",
    delete_downloads = TRUE 
  )
}

if (!file.exists("Data/bm_files/tract_day/nc_gapfilled_2017/bm_panel.rds")) {
  ntl_panel_nc_2017 <- build_bm_tract_panel(
    tracts_sf = county_polygons_nc,
    dates = seq.Date(from = as.Date("2017-01-01"), to = as.Date("2017-12-31"), by = "day"),
    bearer = input_nasa_bearer,
    ntl_variable = "Gap_Filled_DNB_BRDF-Corrected_NTL",
    quality_flag_rm = 1,
    quiet = FALSE,
    cache_key = "nc_gapfilled_2017",
    delete_downloads = TRUE 
  )
}


# South Carolina

if (!file.exists("Data/bm_files/tract_day/sc_gapfilled_2016/bm_panel.rds")) {
  ntl_panel_sc_2016 <- build_bm_tract_panel(
    tracts_sf = county_polygons_sc,
    dates = seq.Date(from = as.Date("2016-01-01"), to = as.Date("2016-12-31"), by = "day"),
    bearer = input_nasa_bearer,
    ntl_variable = "Gap_Filled_DNB_BRDF-Corrected_NTL",
    quality_flag_rm = 1,
    quiet = FALSE,
    cache_key = "sc_gapfilled_2016",
    delete_downloads = TRUE 
  )
}

if (!file.exists("Data/bm_files/tract_day/sc_gapfilled_2017/bm_panel.rds")) {
  ntl_panel_sc_2017 <- build_bm_tract_panel(
    tracts_sf = county_polygons_sc,
    dates = seq.Date(from = as.Date("2017-01-01"), to = as.Date("2017-12-31"), by = "day"),
    bearer = input_nasa_bearer,
    ntl_variable = "Gap_Filled_DNB_BRDF-Corrected_NTL",
    quality_flag_rm = 1,
    quiet = FALSE,
    cache_key = "sc_gapfilled_2017",
    delete_downloads = TRUE 
  )
}


# Louisiana

if (!file.exists("Data/bm_files/tract_day/la_gapfilled_2016/bm_panel.rds")) {
  ntl_panel_la_2016 <- build_bm_tract_panel(
    tracts_sf = county_polygons_la,
    dates = seq.Date(from = as.Date("2016-01-01"), to = as.Date("2016-12-31"), by = "day"),
    bearer = input_nasa_bearer,
    ntl_variable = "Gap_Filled_DNB_BRDF-Corrected_NTL",
    quality_flag_rm = 1,
    quiet = FALSE,
    cache_key = "la_gapfilled_2016",
    delete_downloads = TRUE 
  )
}

if (!file.exists("Data/bm_files/tract_day/la_gapfilled_2017/bm_panel.rds")) {
  ntl_panel_la_2017 <- build_bm_tract_panel(
    tracts_sf = county_polygons_la,
    dates = seq.Date(from = as.Date("2017-01-01"), to = as.Date("2017-12-31"), by = "day"),
    bearer = input_nasa_bearer,
    ntl_variable = "Gap_Filled_DNB_BRDF-Corrected_NTL",
    quality_flag_rm = 1,
    quiet = FALSE,
    cache_key = "la_gapfilled_2017",
    delete_downloads = TRUE 
  )
}

