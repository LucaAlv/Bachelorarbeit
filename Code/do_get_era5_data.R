make_era5_panel <- function(
  shapefile,
  start_date,
  end_date,
  variables,
  file_prefix,
  daily_output_file,
  use_batch = TRUE,
  workers = 3,
  retry = 10,
  overwrite_downloads = FALSE,
  overwrite_extracts = FALSE
) {
  # 1) Download one ERA5 file per month. Existing files are skipped by default.
  download_manifest <- download_era5_files(
    shapefile = shapefile,
    start_date = start_date,
    end_date = end_date,
    variables = variables,
    file_prefix = file_prefix,
    use_batch = use_batch,
    workers = workers,
    retry = retry,
    overwrite = overwrite_downloads
  )

  # 2) Extract hourly county-level values from every downloaded NetCDF file.
  extract_manifest <- extract_era5_nc_files(
    nc_files = download_manifest,
    shapefile = shapefile,
    overwrite = overwrite_extracts
  )

  # 3) Convert hourly precipitation to a clean county-day precipitation panel.
  out <- summarise_era5_daily_precip(
    extracted_files = extract_manifest,
    output_file = daily_output_file,
  )
}

# Precipitation - Puerto Rico - 2016
if (!file.exists("Output/era5_output/era5_pr_2016_tp_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_pr,
    start_date = "2016-01-01",
    end_date = "2016-12-31",
    variables = c("total_precipitation"),
    file_prefix = "era5_pr",
    daily_output_file = "Output/era5_output/era5_pr_2016_tp_panel.rds",
  )
}

# Precipitation - Puerto Rico - 2017
if (!file.exists("Output/era5_output/era5_pr_2016_tp_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_pr,
    start_date = "2017-01-01",
    end_date = "2017-12-31",
    variables = c("total_precipitation"),
    file_prefix = "era5_pr",
    daily_output_file = "Output/era5_output/era5_pr_2017_tp_panel.rds",
  )
}

# Precipitation - Puerto Rico - 2018
if (!file.exists("Output/era5_output/era5_pr_2016_tp_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_pr,
    start_date = "2018-01-01",
    end_date = "2018-12-31",
    variables = c("total_precipitation"),
    file_prefix = "era5_pr",
    daily_output_file = "Output/era5_output/era5_pr_2018_tp_panel.rds",
  )
}
