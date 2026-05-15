## Define NASA input_nasa_bearer token for blackmarble api ##
input_nasa_bearer <- get_nasa_token(
  username = Sys.getenv("NASA_USER"),
  password = Sys.getenv("NASA_PASS")
)

## Process data

# 2 removes poor-quality/outlier/potential cloud-contaminated pixels.
black_marble_quality_flag_rm <- 2L

bm_panel_path <- function(cache_key) {
  file.path("Data/bm_files/tract_day", cache_key, "bm_panel.rds")
}

bm_panel_has_quality_flag <- function(cache_key, quality_flag_rm) {
  manifest_path <- file.path("Data/bm_files/tract_day", cache_key, "run_manifest.rds")
  if (!file.exists(manifest_path)) {
    return(FALSE)
  }

  manifest <- tryCatch(readRDS(manifest_path), error = function(e) NULL)
  isTRUE(manifest$completed) &&
    isTRUE(identical(
      as.integer(manifest$quality_flag_rm),
      as.integer(quality_flag_rm)
    ))
}

bm_panel_needs_rebuild <- function(cache_key, quality_flag_rm) {
  !file.exists(bm_panel_path(cache_key)) ||
    !bm_panel_has_quality_flag(cache_key, quality_flag_rm)
}


# Puerto Rico

if (bm_panel_needs_rebuild("pr_gapfilled_2016", black_marble_quality_flag_rm)) {
  ntl_panel_pr_2016 <- build_bm_tract_panel(
    tracts_sf = county_polygons_pr,
    dates = seq.Date(from = as.Date("2016-01-01"), to = as.Date("2016-12-31"), by = "day"),
    bearer = input_nasa_bearer,
    ntl_variable = "Gap_Filled_DNB_BRDF-Corrected_NTL",
    quality_flag_rm = black_marble_quality_flag_rm,
    overwrite = !bm_panel_has_quality_flag("pr_gapfilled_2016", black_marble_quality_flag_rm),
    quiet = FALSE,
    cache_key = "pr_gapfilled_2016",
    delete_downloads = TRUE
  )
}

if (bm_panel_needs_rebuild("pr_gapfilled_2017", black_marble_quality_flag_rm)) {
  ntl_panel_pr_2017 <- build_bm_tract_panel(
    tracts_sf = county_polygons_pr,
    dates = seq.Date(from = as.Date("2017-01-01"), to = as.Date("2017-12-31"), by = "day"),
    bearer = input_nasa_bearer,
    ntl_variable = "Gap_Filled_DNB_BRDF-Corrected_NTL",
    quality_flag_rm = black_marble_quality_flag_rm,
    overwrite = !bm_panel_has_quality_flag("pr_gapfilled_2017", black_marble_quality_flag_rm),
    quiet = FALSE,
    cache_key = "pr_gapfilled_2017",
    delete_downloads = TRUE
  )
}

if (bm_panel_needs_rebuild("pr_gapfilled_2018", black_marble_quality_flag_rm)) {
  ntl_panel_pr_2018 <- build_bm_tract_panel(
    tracts_sf = county_polygons_pr,
    dates = seq.Date(from = as.Date("2018-01-01"), to = as.Date("2018-12-31"), by = "day"),
    bearer = input_nasa_bearer,
    ntl_variable = "Gap_Filled_DNB_BRDF-Corrected_NTL",
    quality_flag_rm = black_marble_quality_flag_rm,
    overwrite = !bm_panel_has_quality_flag("pr_gapfilled_2018", black_marble_quality_flag_rm),
    quiet = FALSE,
    cache_key = "pr_gapfilled_2018",
    delete_downloads = TRUE
  )
}


# Florida

if (bm_panel_needs_rebuild("fl_gapfilled_2016", black_marble_quality_flag_rm)) {
  ntl_panel_fl_2016 <- build_bm_tract_panel(
    tracts_sf = county_polygons_fl,
    dates = seq.Date(from = as.Date("2016-01-01"), to = as.Date("2016-12-31"), by = "day"),
    bearer = input_nasa_bearer,
    ntl_variable = "Gap_Filled_DNB_BRDF-Corrected_NTL",
    quality_flag_rm = black_marble_quality_flag_rm,
    overwrite = !bm_panel_has_quality_flag("fl_gapfilled_2016", black_marble_quality_flag_rm),
    quiet = FALSE,
    cache_key = "fl_gapfilled_2016",
    delete_downloads = TRUE
  )
}

if (bm_panel_needs_rebuild("fl_gapfilled_2017", black_marble_quality_flag_rm)) {
  ntl_panel_fl_2017 <- build_bm_tract_panel(
    tracts_sf = county_polygons_fl,
    dates = seq.Date(from = as.Date("2017-01-01"), to = as.Date("2017-12-31"), by = "day"),
    bearer = input_nasa_bearer,
    ntl_variable = "Gap_Filled_DNB_BRDF-Corrected_NTL",
    quality_flag_rm = black_marble_quality_flag_rm,
    overwrite = !bm_panel_has_quality_flag("fl_gapfilled_2017", black_marble_quality_flag_rm),
    quiet = FALSE,
    cache_key = "fl_gapfilled_2017",
    delete_downloads = TRUE
  )
}

if (bm_panel_needs_rebuild("fl_gapfilled_2018", black_marble_quality_flag_rm)) {
  ntl_panel_fl_2018 <- build_bm_tract_panel(
    tracts_sf = county_polygons_fl,
    dates = seq.Date(from = as.Date("2018-01-01"), to = as.Date("2018-12-31"), by = "day"),
    bearer = input_nasa_bearer,
    ntl_variable = "Gap_Filled_DNB_BRDF-Corrected_NTL",
    quality_flag_rm = black_marble_quality_flag_rm,
    overwrite = !bm_panel_has_quality_flag("fl_gapfilled_2018", black_marble_quality_flag_rm),
    quiet = FALSE,
    cache_key = "fl_gapfilled_2018",
    delete_downloads = TRUE
  )
}

# Texas

if (bm_panel_needs_rebuild("tx_gapfilled_2016", black_marble_quality_flag_rm)) {
  ntl_panel_tx_2016 <- build_bm_tract_panel(
    tracts_sf = county_polygons_tx,
    dates = seq.Date(from = as.Date("2016-01-01"), to = as.Date("2016-12-31"), by = "day"),
    bearer = input_nasa_bearer,
    ntl_variable = "Gap_Filled_DNB_BRDF-Corrected_NTL",
    quality_flag_rm = black_marble_quality_flag_rm,
    overwrite = !bm_panel_has_quality_flag("tx_gapfilled_2016", black_marble_quality_flag_rm),
    quiet = FALSE,
    cache_key = "tx_gapfilled_2016",
    delete_downloads = TRUE
  )
}

if (bm_panel_needs_rebuild("tx_gapfilled_2017", black_marble_quality_flag_rm)) {
  ntl_panel_tx_2017 <- build_bm_tract_panel(
    tracts_sf = county_polygons_tx,
    dates = seq.Date(from = as.Date("2017-01-01"), to = as.Date("2017-12-31"), by = "day"),
    bearer = input_nasa_bearer,
    ntl_variable = "Gap_Filled_DNB_BRDF-Corrected_NTL",
    quality_flag_rm = black_marble_quality_flag_rm,
    overwrite = !bm_panel_has_quality_flag("tx_gapfilled_2017", black_marble_quality_flag_rm),
    quiet = FALSE,
    cache_key = "tx_gapfilled_2017",
    delete_downloads = TRUE
  )
}

if (bm_panel_needs_rebuild("tx_gapfilled_2018", black_marble_quality_flag_rm)) {
  ntl_panel_tx_2018 <- build_bm_tract_panel(
    tracts_sf = county_polygons_tx,
    dates = seq.Date(from = as.Date("2018-01-01"), to = as.Date("2018-12-31"), by = "day"),
    bearer = input_nasa_bearer,
    ntl_variable = "Gap_Filled_DNB_BRDF-Corrected_NTL",
    quality_flag_rm = black_marble_quality_flag_rm,
    overwrite = !bm_panel_has_quality_flag("tx_gapfilled_2018", black_marble_quality_flag_rm),
    quiet = FALSE,
    cache_key = "tx_gapfilled_2018",
    delete_downloads = TRUE
  )
}


# North Carolina

if (bm_panel_needs_rebuild("nc_gapfilled_2016", black_marble_quality_flag_rm)) {
  ntl_panel_nc_2016 <- build_bm_tract_panel(
    tracts_sf = county_polygons_nc,
    dates = seq.Date(from = as.Date("2016-01-01"), to = as.Date("2016-12-31"), by = "day"),
    bearer = input_nasa_bearer,
    ntl_variable = "Gap_Filled_DNB_BRDF-Corrected_NTL",
    quality_flag_rm = black_marble_quality_flag_rm,
    overwrite = !bm_panel_has_quality_flag("nc_gapfilled_2016", black_marble_quality_flag_rm),
    quiet = FALSE,
    cache_key = "nc_gapfilled_2016",
    delete_downloads = TRUE
  )
}

if (bm_panel_needs_rebuild("nc_gapfilled_2017", black_marble_quality_flag_rm)) {
  ntl_panel_nc_2017 <- build_bm_tract_panel(
    tracts_sf = county_polygons_nc,
    dates = seq.Date(from = as.Date("2017-01-01"), to = as.Date("2017-12-31"), by = "day"),
    bearer = input_nasa_bearer,
    ntl_variable = "Gap_Filled_DNB_BRDF-Corrected_NTL",
    quality_flag_rm = black_marble_quality_flag_rm,
    overwrite = !bm_panel_has_quality_flag("nc_gapfilled_2017", black_marble_quality_flag_rm),
    quiet = FALSE,
    cache_key = "nc_gapfilled_2017",
    delete_downloads = TRUE
  )
}

if (bm_panel_needs_rebuild("nc_gapfilled_2018", black_marble_quality_flag_rm)) {
  ntl_panel_nc_2018 <- build_bm_tract_panel(
    tracts_sf = county_polygons_nc,
    dates = seq.Date(from = as.Date("2018-01-01"), to = as.Date("2018-12-31"), by = "day"),
    bearer = input_nasa_bearer,
    ntl_variable = "Gap_Filled_DNB_BRDF-Corrected_NTL",
    quality_flag_rm = black_marble_quality_flag_rm,
    overwrite = !bm_panel_has_quality_flag("nc_gapfilled_2018", black_marble_quality_flag_rm),
    quiet = FALSE,
    cache_key = "nc_gapfilled_2018",
    delete_downloads = TRUE
  )
}


# South Carolina

if (bm_panel_needs_rebuild("sc_gapfilled_2016", black_marble_quality_flag_rm)) {
  ntl_panel_sc_2016 <- build_bm_tract_panel(
    tracts_sf = county_polygons_sc,
    dates = seq.Date(from = as.Date("2016-01-01"), to = as.Date("2016-12-31"), by = "day"),
    bearer = input_nasa_bearer,
    ntl_variable = "Gap_Filled_DNB_BRDF-Corrected_NTL",
    quality_flag_rm = black_marble_quality_flag_rm,
    overwrite = !bm_panel_has_quality_flag("sc_gapfilled_2016", black_marble_quality_flag_rm),
    quiet = FALSE,
    cache_key = "sc_gapfilled_2016",
    delete_downloads = TRUE
  )
}

if (bm_panel_needs_rebuild("sc_gapfilled_2017", black_marble_quality_flag_rm)) {
  ntl_panel_sc_2017 <- build_bm_tract_panel(
    tracts_sf = county_polygons_sc,
    dates = seq.Date(from = as.Date("2017-01-01"), to = as.Date("2017-12-31"), by = "day"),
    bearer = input_nasa_bearer,
    ntl_variable = "Gap_Filled_DNB_BRDF-Corrected_NTL",
    quality_flag_rm = black_marble_quality_flag_rm,
    overwrite = !bm_panel_has_quality_flag("sc_gapfilled_2017", black_marble_quality_flag_rm),
    quiet = FALSE,
    cache_key = "sc_gapfilled_2017",
    delete_downloads = TRUE
  )
}

if (bm_panel_needs_rebuild("sc_gapfilled_2018", black_marble_quality_flag_rm)) {
  ntl_panel_sc_2018 <- build_bm_tract_panel(
    tracts_sf = county_polygons_sc,
    dates = seq.Date(from = as.Date("2018-01-01"), to = as.Date("2018-12-31"), by = "day"),
    bearer = input_nasa_bearer,
    ntl_variable = "Gap_Filled_DNB_BRDF-Corrected_NTL",
    quality_flag_rm = black_marble_quality_flag_rm,
    overwrite = !bm_panel_has_quality_flag("sc_gapfilled_2018", black_marble_quality_flag_rm),
    quiet = FALSE,
    cache_key = "sc_gapfilled_2018",
    delete_downloads = TRUE
  )
}


# Louisiana

if (bm_panel_needs_rebuild("la_gapfilled_2016", black_marble_quality_flag_rm)) {
  ntl_panel_la_2016 <- build_bm_tract_panel(
    tracts_sf = county_polygons_la,
    dates = seq.Date(from = as.Date("2016-01-01"), to = as.Date("2016-12-31"), by = "day"),
    bearer = input_nasa_bearer,
    ntl_variable = "Gap_Filled_DNB_BRDF-Corrected_NTL",
    quality_flag_rm = black_marble_quality_flag_rm,
    overwrite = !bm_panel_has_quality_flag("la_gapfilled_2016", black_marble_quality_flag_rm),
    quiet = FALSE,
    cache_key = "la_gapfilled_2016",
    delete_downloads = TRUE
  )
}

if (bm_panel_needs_rebuild("la_gapfilled_2017", black_marble_quality_flag_rm)) {
  ntl_panel_la_2017 <- build_bm_tract_panel(
    tracts_sf = county_polygons_la,
    dates = seq.Date(from = as.Date("2017-01-01"), to = as.Date("2017-12-31"), by = "day"),
    bearer = input_nasa_bearer,
    ntl_variable = "Gap_Filled_DNB_BRDF-Corrected_NTL",
    quality_flag_rm = black_marble_quality_flag_rm,
    overwrite = !bm_panel_has_quality_flag("la_gapfilled_2017", black_marble_quality_flag_rm),
    quiet = FALSE,
    cache_key = "la_gapfilled_2017",
    delete_downloads = TRUE
  )
}

if (bm_panel_needs_rebuild("la_gapfilled_2018", black_marble_quality_flag_rm)) {
  ntl_panel_la_2018 <- build_bm_tract_panel(
    tracts_sf = county_polygons_la,
    dates = seq.Date(from = as.Date("2018-01-01"), to = as.Date("2018-12-31"), by = "day"),
    bearer = input_nasa_bearer,
    ntl_variable = "Gap_Filled_DNB_BRDF-Corrected_NTL",
    quality_flag_rm = black_marble_quality_flag_rm,
    overwrite = !bm_panel_has_quality_flag("la_gapfilled_2018", black_marble_quality_flag_rm),
    quiet = FALSE,
    cache_key = "la_gapfilled_2018",
    delete_downloads = TRUE
  )
}
