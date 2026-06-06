
source("Code/get_era5_data.R")

make_era5_panel <- function(
  shapefile,
  start_date,
  end_date,
  variables,
  file_prefix,
  daily_output_file,
  daily_summary = c("auto", "precip", "windgust"),
  use_batch = TRUE,
  workers = 3,
  retry = 10,
  overwrite_downloads = FALSE,
  overwrite_extracts = FALSE
) {

  daily_summary <- match.arg(daily_summary, daily_summary)
  panel_start_date <- as.Date(start_date)
  panel_end_date <- as.Date(end_date)

  # 1. Download one ERA5 file per month. Existing files are skipped by default.
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

  # 2. Extract hourly county-level values from every downloaded NetCDF file.
  extract_manifest <- extract_era5_nc_files(
    nc_files = download_manifest,
    shapefile = shapefile,
    overwrite = overwrite_extracts
  )

  # 3. Convert hourly ERA5 values to a clean county-day panel.
  summarise_fun <- switch(
    daily_summary,
    auto = {
      if (length(variables) != 1) {
        stop("`daily_summary = \"auto\"` requires exactly one ERA5 variable.")
      }

      if (identical(variables[[1]], "total_precipitation")) {
        summarise_era5_daily_precip
      } else if (identical(variables[[1]], "10m_wind_gust_since_previous_post_processing")) {
        summarise_era5_daily_windgust
      } else {
        stop("Could not infer a daily summary function for ERA5 variable: ", variables[[1]])
      }
    },
    precip = summarise_era5_daily_precip,
    windgust = summarise_era5_daily_windgust
  )

  out <- summarise_fun(
    extracted_files = extract_manifest
  )
  out <- out[out$date >= panel_start_date & out$date <= panel_end_date, , drop = FALSE]
  rownames(out) <- NULL

  dir.create(dirname(daily_output_file), recursive = TRUE, showWarnings = FALSE)
  if (tolower(tools::file_ext(daily_output_file)) == "csv") {
    utils::write.csv(out, daily_output_file, row.names = FALSE)
  } else {
    saveRDS(out, daily_output_file)
  }

  out
}








#### Load Data ####

### Precipitation

## Puerto Rico

# Precipitation - Puerto Rico - 2016
if (!file.exists("Data/era5_files/panels/era5_pr_2016_tp_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_pr,
    start_date = "2016-01-01",
    end_date = "2016-12-31",
    variables = c("total_precipitation"),
    file_prefix = "era5_pr",
    daily_output_file = "Data/era5_files/panels/era5_pr_2016_tp_panel.rds",
  )
}

# Precipitation - Puerto Rico - 2017
if (!file.exists("Data/era5_files/panels/era5_pr_2017_tp_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_pr,
    start_date = "2017-01-01",
    end_date = "2017-12-31",
    variables = c("total_precipitation"),
    file_prefix = "era5_pr",
    daily_output_file = "Data/era5_files/panels/era5_pr_2017_tp_panel.rds",
  )
}

# Precipitation - Puerto Rico - 2018
if (!file.exists("Data/era5_files/panels/era5_pr_2018_tp_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_pr,
    start_date = "2018-01-01",
    end_date = "2018-12-31",
    variables = c("total_precipitation"),
    file_prefix = "era5_pr",
    daily_output_file = "Data/era5_files/panels/era5_pr_2018_tp_panel.rds",
  )
}

## Florida

# Precipitation - Florida - 2016
if (!file.exists("Data/era5_files/panels/era5_fl_2016_tp_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_fl,
    start_date = "2016-01-01",
    end_date = "2016-12-31",
    variables = c("total_precipitation"),
    file_prefix = "era5_fl",
    daily_output_file = "Data/era5_files/panels/era5_fl_2016_tp_panel.rds",
  )
}

# Precipitation - Florida - 2017
if (!file.exists("Data/era5_files/panels/era5_fl_2017_tp_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_fl,
    start_date = "2017-01-01",
    end_date = "2017-12-31",
    variables = c("total_precipitation"),
    file_prefix = "era5_fl",
    daily_output_file = "Data/era5_files/panels/era5_fl_2017_tp_panel.rds",
  )
}

# Precipitation - Florida - 2018
if (!file.exists("Data/era5_files/panels/era5_fl_2018_tp_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_fl,
    start_date = "2018-01-01",
    end_date = "2018-12-31",
    variables = c("total_precipitation"),
    file_prefix = "era5_fl",
    daily_output_file = "Data/era5_files/panels/era5_fl_2018_tp_panel.rds",
  )
}

## Texas

# Precipitation - Texas - 2016
if (!file.exists("Data/era5_files/panels/era5_tx_2016_tp_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_tx,
    start_date = "2016-01-01",
    end_date = "2016-12-31",
    variables = c("total_precipitation"),
    file_prefix = "era5_tx",
    daily_output_file = "Data/era5_files/panels/era5_tx_2016_tp_panel.rds",
  )
}

# Precipitation - Texas - 2017
if (!file.exists("Data/era5_files/panels/era5_tx_2017_tp_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_tx,
    start_date = "2017-01-01",
    end_date = "2017-12-31",
    variables = c("total_precipitation"),
    file_prefix = "era5_tx",
    daily_output_file = "Data/era5_files/panels/era5_tx_2017_tp_panel.rds",
  )
}

# Precipitation - Texas - 2018
if (!file.exists("Data/era5_files/panels/era5_tx_2018_tp_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_tx,
    start_date = "2018-01-01",
    end_date = "2018-12-31",
    variables = c("total_precipitation"),
    file_prefix = "era5_tx",
    daily_output_file = "Data/era5_files/panels/era5_tx_2018_tp_panel.rds",
  )
}

## North Carolina

# Precipitation - North Carolina - 2016
if (!file.exists("Data/era5_files/panels/era5_nc_2016_tp_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_nc,
    start_date = "2016-01-01",
    end_date = "2016-12-31",
    variables = c("total_precipitation"),
    file_prefix = "era5_nc",
    daily_output_file = "Data/era5_files/panels/era5_nc_2016_tp_panel.rds",
  )
}

# Precipitation - North Carolina - 2017
if (!file.exists("Data/era5_files/panels/era5_nc_2017_tp_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_nc,
    start_date = "2017-01-01",
    end_date = "2017-12-31",
    variables = c("total_precipitation"),
    file_prefix = "era5_nc",
    daily_output_file = "Data/era5_files/panels/era5_nc_2017_tp_panel.rds",
  )
}

# Precipitation - North Carolina - 2018
if (!file.exists("Data/era5_files/panels/era5_nc_2018_tp_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_nc,
    start_date = "2018-01-01",
    end_date = "2018-12-31",
    variables = c("total_precipitation"),
    file_prefix = "era5_nc",
    daily_output_file = "Data/era5_files/panels/era5_nc_2018_tp_panel.rds",
  )
}

## South Carolina

# Precipitation - South Carolina - 2016
if (!file.exists("Data/era5_files/panels/era5_sc_2016_tp_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_sc,
    start_date = "2016-01-01",
    end_date = "2016-12-31",
    variables = c("total_precipitation"),
    file_prefix = "era5_sc",
    daily_output_file = "Data/era5_files/panels/era5_sc_2016_tp_panel.rds",
  )
}

# Precipitation - South Carolina - 2017
if (!file.exists("Data/era5_files/panels/era5_sc_2017_tp_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_sc,
    start_date = "2017-01-01",
    end_date = "2017-12-31",
    variables = c("total_precipitation"),
    file_prefix = "era5_sc",
    daily_output_file = "Data/era5_files/panels/era5_sc_2017_tp_panel.rds",
  )
}

# Precipitation - South Carolina - 2018
if (!file.exists("Data/era5_files/panels/era5_sc_2018_tp_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_sc,
    start_date = "2018-01-01",
    end_date = "2018-12-31",
    variables = c("total_precipitation"),
    file_prefix = "era5_sc",
    daily_output_file = "Data/era5_files/panels/era5_sc_2018_tp_panel.rds",
  )
}

## Louisiana

# Precipitation - Louisiana - 2016
if (!file.exists("Data/era5_files/panels/era5_la_2016_tp_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_la,
    start_date = "2016-01-01",
    end_date = "2016-12-31",
    variables = c("total_precipitation"),
    file_prefix = "era5_la",
    daily_output_file = "Data/era5_files/panels/era5_la_2016_tp_panel.rds",
  )
}

# Precipitation - Louisiana - 2017
if (!file.exists("Data/era5_files/panels/era5_la_2017_tp_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_la,
    start_date = "2017-01-01",
    end_date = "2017-12-31",
    variables = c("total_precipitation"),
    file_prefix = "era5_la",
    daily_output_file = "Data/era5_files/panels/era5_la_2017_tp_panel.rds",
  )
}

# Precipitation - Louisiana - 2018
if (!file.exists("Data/era5_files/panels/era5_la_2018_tp_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_la,
    start_date = "2018-01-01",
    end_date = "2018-12-31",
    variables = c("total_precipitation"),
    file_prefix = "era5_la",
    daily_output_file = "Data/era5_files/panels/era5_la_2018_tp_panel.rds",
  )
}

### Wind

## Puerto Rico

# Wind Gust - Puerto Rico - 2016
if (!file.exists("Data/era5_files/panels/era5_pr_2016_wg_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_pr,
    start_date = "2016-01-01",
    end_date = "2016-12-31",
    variables = c("10m_wind_gust_since_previous_post_processing"),
    file_prefix = "era5_pr",
    daily_output_file = "Data/era5_files/panels/era5_pr_2016_wg_panel.rds",
  )
}

# Wind Gust - Puerto Rico - 2017
if (!file.exists("Data/era5_files/panels/era5_pr_2017_wg_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_pr,
    start_date = "2017-01-01",
    end_date = "2017-12-31",
    variables = c("10m_wind_gust_since_previous_post_processing"),
    file_prefix = "era5_pr",
    daily_output_file = "Data/era5_files/panels/era5_pr_2017_wg_panel.rds",
  )
}

# Wind Gust - Puerto Rico - 2018
if (!file.exists("Data/era5_files/panels/era5_pr_2018_wg_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_pr,
    start_date = "2018-01-01",
    end_date = "2018-12-31",
    variables = c("10m_wind_gust_since_previous_post_processing"),
    file_prefix = "era5_pr",
    daily_output_file = "Data/era5_files/panels/era5_pr_2018_wg_panel.rds",
  )
}

## Florida

# Wind Gust - Florida - 2016
if (!file.exists("Data/era5_files/panels/era5_fl_2016_wg_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_fl,
    start_date = "2016-01-01",
    end_date = "2016-12-31",
    variables = c("10m_wind_gust_since_previous_post_processing"),
    file_prefix = "era5_fl",
    daily_output_file = "Data/era5_files/panels/era5_fl_2016_wg_panel.rds",
  )
}

# Wind Gust - Florida - 2017
if (!file.exists("Data/era5_files/panels/era5_fl_2017_wg_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_fl,
    start_date = "2017-01-01",
    end_date = "2017-12-31",
    variables = c("10m_wind_gust_since_previous_post_processing"),
    file_prefix = "era5_fl",
    daily_output_file = "Data/era5_files/panels/era5_fl_2017_wg_panel.rds",
  )
}

# Wind Gust - Florida - 2018
if (!file.exists("Data/era5_files/panels/era5_fl_2018_wg_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_fl,
    start_date = "2018-01-01",
    end_date = "2018-12-31",
    variables = c("10m_wind_gust_since_previous_post_processing"),
    file_prefix = "era5_fl",
    daily_output_file = "Data/era5_files/panels/era5_fl_2018_wg_panel.rds",
  )
}

## Texas

# Wind Gust - Texas - 2016
if (!file.exists("Data/era5_files/panels/era5_tx_2016_wg_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_tx,
    start_date = "2016-01-01",
    end_date = "2016-12-31",
    variables = c("10m_wind_gust_since_previous_post_processing"),
    file_prefix = "era5_tx",
    daily_output_file = "Data/era5_files/panels/era5_tx_2016_wg_panel.rds",
  )
}

# Wind Gust - Texas - 2017
if (!file.exists("Data/era5_files/panels/era5_tx_2017_wg_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_tx,
    start_date = "2017-01-01",
    end_date = "2017-12-31",
    variables = c("10m_wind_gust_since_previous_post_processing"),
    file_prefix = "era5_tx",
    daily_output_file = "Data/era5_files/panels/era5_tx_2017_wg_panel.rds",
  )
}

# Wind Gust - Texas - 2018
if (!file.exists("Data/era5_files/panels/era5_tx_2018_wg_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_tx,
    start_date = "2018-01-01",
    end_date = "2018-12-31",
    variables = c("10m_wind_gust_since_previous_post_processing"),
    file_prefix = "era5_tx",
    daily_output_file = "Data/era5_files/panels/era5_tx_2018_wg_panel.rds",
  )
}

## North Carolina

# Wind Gust - North Carolina - 2016
if (!file.exists("Data/era5_files/panels/era5_nc_2016_wg_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_nc,
    start_date = "2016-01-01",
    end_date = "2016-12-31",
    variables = c("10m_wind_gust_since_previous_post_processing"),
    file_prefix = "era5_nc",
    daily_output_file = "Data/era5_files/panels/era5_nc_2016_wg_panel.rds",
  )
}

# Wind Gust - North Carolina - 2017
if (!file.exists("Data/era5_files/panels/era5_nc_2017_wg_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_nc,
    start_date = "2017-01-01",
    end_date = "2017-12-31",
    variables = c("10m_wind_gust_since_previous_post_processing"),
    file_prefix = "era5_nc",
    daily_output_file = "Data/era5_files/panels/era5_nc_2017_wg_panel.rds",
  )
}

# Wind Gust - North Carolina - 2018
if (!file.exists("Data/era5_files/panels/era5_nc_2018_wg_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_nc,
    start_date = "2018-01-01",
    end_date = "2018-12-31",
    variables = c("10m_wind_gust_since_previous_post_processing"),
    file_prefix = "era5_nc",
    daily_output_file = "Data/era5_files/panels/era5_nc_2018_wg_panel.rds",
  )
}

## South Carolina

# Wind Gust - South Carolina - 2016
if (!file.exists("Data/era5_files/panels/era5_sc_2016_wg_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_sc,
    start_date = "2016-01-01",
    end_date = "2016-12-31",
    variables = c("10m_wind_gust_since_previous_post_processing"),
    file_prefix = "era5_sc",
    daily_output_file = "Data/era5_files/panels/era5_sc_2016_wg_panel.rds",
  )
}

# Wind Gust - South Carolina - 2017
if (!file.exists("Data/era5_files/panels/era5_sc_2017_wg_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_sc,
    start_date = "2017-01-01",
    end_date = "2017-12-31",
    variables = c("10m_wind_gust_since_previous_post_processing"),
    file_prefix = "era5_sc",
    daily_output_file = "Data/era5_files/panels/era5_sc_2017_wg_panel.rds",
  )
}

# Wind Gust - South Carolina - 2018
if (!file.exists("Data/era5_files/panels/era5_sc_2018_wg_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_sc,
    start_date = "2018-01-01",
    end_date = "2018-12-31",
    variables = c("10m_wind_gust_since_previous_post_processing"),
    file_prefix = "era5_sc",
    daily_output_file = "Data/era5_files/panels/era5_sc_2018_wg_panel.rds",
  )
}

## Louisiana

# Wind Gust - Louisiana - 2016
if (!file.exists("Data/era5_files/panels/era5_la_2016_wg_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_la,
    start_date = "2016-01-01",
    end_date = "2016-12-31",
    variables = c("10m_wind_gust_since_previous_post_processing"),
    file_prefix = "era5_la",
    daily_output_file = "Data/era5_files/panels/era5_la_2016_wg_panel.rds",
  )
}

# Wind Gust - Louisiana - 2017
if (!file.exists("Data/era5_files/panels/era5_la_2017_wg_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_la,
    start_date = "2017-01-01",
    end_date = "2017-12-31",
    variables = c("10m_wind_gust_since_previous_post_processing"),
    file_prefix = "era5_la",
    daily_output_file = "Data/era5_files/panels/era5_la_2017_wg_panel.rds",
  )
}

# Wind Gust - Louisiana - 2018
if (!file.exists("Data/era5_files/panels/era5_la_2018_wg_panel.rds")) {
  make_era5_panel(
    shapefile = county_polygons_la,
    start_date = "2018-01-01",
    end_date = "2018-12-31",
    variables = c("10m_wind_gust_since_previous_post_processing"),
    file_prefix = "era5_la",
    daily_output_file = "Data/era5_files/panels/era5_la_2018_wg_panel.rds",
  )
}
