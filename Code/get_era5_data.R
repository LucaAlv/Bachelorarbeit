# Build a tract-level ERA5 panel from the hourly single-levels dataset.
#
# Main design choices:
# - Downloads hourly ERA5 single-level data and aggregates to tract level.
# - Uses area-weighted polygon extraction via exactextractr.
# - Aggregates to daily values in a user-specified local time zone by default.
# - Splits large CDS requests into monthly variable batches to avoid oversized files.
# - Works for any region supplied as an sf polygon object, not just Puerto Rico.
#
# Before first use:
# 1) accept the ERA5 single-level dataset terms in CDS
# 2) store your CDS token once via ecmwfr::wf_set_key()
#
# Example:
# pr_panel <- build_era5_tract_panel(
#   polygons = pr_tracts,
#   start_date = "2023-01-01",
#   end_date   = "2026-01-31",
#   region_name = "puerto_rico",
#   local_tz = "America/Puerto_Rico"
# )
#
# fl_panel <- build_era5_tract_panel(
#   polygons = florida_tracts,
#   start_date = "2023-01-01",
#   end_date   = "2026-01-31",
#   region_name = "florida",
#   local_tz = "America/New_York"
# )

required_era5_packages <- function() {
  c(
    "ecmwfr",
    "sf",
    "terra",
    "exactextractr",
    "dplyr",
    "tidyr",
    "lubridate",
    "purrr",
    "arrow",
    "stringr",
    "ncdf4"
  )
}

check_era5_packages <- function() {
  pkgs <- required_era5_packages()
  missing_pkgs <- pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly = TRUE)]
  if (length(missing_pkgs) > 0) {
    stop(
      "Missing packages: ",
      paste(missing_pkgs, collapse = ", "),
      call. = FALSE
    )
  }
}

era5_variable_catalog <- function() {
  data.frame(
    alias = c(
      "u10", "v10", "gust10", "mslp", "sp",
      "tp", "runoff", "t2m",
      "swvl1", "swvl2", "swvl3", "swvl4"
    ),
    cds_name = c(
      "10m_u_component_of_wind",
      "10m_v_component_of_wind",
      "10m_wind_gust_since_previous_post_processing",
      "mean_sea_level_pressure",
      "surface_pressure",
      "total_precipitation",
      "runoff",
      "2m_temperature",
      "volumetric_soil_water_layer_1",
      "volumetric_soil_water_layer_2",
      "volumetric_soil_water_layer_3",
      "volumetric_soil_water_layer_4"
    ),
    short_name = c(
      "u10", "v10", "fg10", "msl", "sp",
      "tp", "ro", "t2m",
      "swvl1", "swvl2", "swvl3", "swvl4"
    ),
    output_name = c(
      "u10_ms", "v10_ms", "gust10_ms", "mslp_hpa", "sp_hpa",
      "tp_mm", "runoff_mm", "t2m_c",
      "swvl1_m3_m3", "swvl2_m3_m3", "swvl3_m3_m3", "swvl4_m3_m3"
    ),
    step_type = c(
      rep("instant", 2), "max", "instant", "instant",
      rep("accum", 2),
      rep("instant", 5)
    ),
    scale = c(
      1, 1, 1, 0.01, 0.01,
      1000, 1000, 1,
      1, 1, 1, 1
    ),
    offset = c(
      0, 0, 0, 0, 0,
      0, 0, -273.15,
      0, 0, 0, 0
    ),
    daily_agg = c(
      "mean", "mean", "max", "mean", "mean",
      "sum", "sum", "mean",
      "mean", "mean", "mean", "mean"
    ),
    stringsAsFactors = FALSE
  )
}

default_era5_variables <- function() {
  era5_variable_catalog()$alias
}

slugify_region_name <- function(x) {
  x <- tolower(x)
  x <- gsub("[^a-z0-9]+", "_", x)
  x <- gsub("(^_+|_+$)", "", x)
  if (identical(x, "")) {
    x <- "region"
  }
  x
}

parse_nc_time <- function(ncfile) {
  nc <- ncdf4::nc_open(ncfile)
  on.exit(ncdf4::nc_close(nc), add = TRUE)

  var_names <- names(nc$var)
  dim_names <- names(nc$dim)

  time_name <- if ("valid_time" %in% var_names) {
    "valid_time"
  } else if ("time" %in% var_names) {
    "time"
  } else if ("valid_time" %in% dim_names) {
    "valid_time"
  } else if ("time" %in% dim_names) {
    "time"
  } else {
    stop("Could not find a time dimension in ", ncfile, call. = FALSE)
  }

  time_values <- ncdf4::ncvar_get(nc, time_name)
  time_units <- ncdf4::ncatt_get(nc, time_name, "units")$value

  if (is.null(time_units) || is.na(time_units)) {
    stop("Could not read time units from ", ncfile, call. = FALSE)
  }

  unit_name <- sub(" since.*$", "", time_units)
  origin_txt <- sub("^[^ ]+ since ", "", time_units)
  origin_txt <- sub(" UTC$", "", origin_txt)
  origin_txt <- sub("\\.0$", "", origin_txt)

  origin_time <- as.POSIXct(origin_txt, tz = "UTC")
  if (is.na(origin_time)) {
    stop("Could not parse NetCDF time origin in ", ncfile, call. = FALSE)
  }

  seconds_per_unit <- switch(
    unit_name,
    "seconds" = 1,
    "second" = 1,
    "minutes" = 60,
    "minute" = 60,
    "hours" = 3600,
    "hour" = 3600,
    "days" = 86400,
    "day" = 86400,
    stop("Unsupported NetCDF time unit '", unit_name, "' in ", ncfile, call. = FALSE)
  )

  origin_time + as.numeric(time_values) * seconds_per_unit
}

safe_mean <- function(x) {
  if (all(is.na(x))) NA_real_ else mean(x, na.rm = TRUE)
}

safe_sum <- function(x) {
  if (all(is.na(x))) NA_real_ else sum(x, na.rm = TRUE)
}

safe_max <- function(x) {
  if (all(is.na(x))) NA_real_ else max(x, na.rm = TRUE)
}

safe_first_non_na <- function(x) {
  idx <- which(!is.na(x))
  if (length(idx) == 0) NA_real_ else x[[idx[[1]]]]
}

chunk_indices <- function(n, chunk_size) {
  split(seq_len(n), ceiling(seq_len(n) / chunk_size))
}

request_days_for_month <- function(month_date, query_start, query_end) {
  month_start <- lubridate::floor_date(month_date, unit = "month")
  month_end <- lubridate::ceiling_date(month_date, unit = "month") - 1L
  days_seq <- seq.Date(
    from = max(month_start, query_start),
    to = min(month_end, query_end),
    by = "day"
  )

  sprintf("%02d", as.integer(format(days_seq, "%d")))
}

resolve_download_artifact <- function(raw_dir, target_stub) {
  candidates <- c(
    file.path(raw_dir, paste0(target_stub, ".nc")),
    file.path(raw_dir, paste0(target_stub, ".zip")),
    file.path(raw_dir, paste0(target_stub, ".grib"))
  )
  existing <- candidates[file.exists(candidates)]
  if (length(existing) == 0) {
    return(NA_character_)
  }
  existing[[1]]
}

list_nc_sources <- function(artifact_path) {
  if (is.na(artifact_path) || !file.exists(artifact_path)) {
    stop("Missing download artifact: ", artifact_path, call. = FALSE)
  }

  ext <- tolower(tools::file_ext(artifact_path))

  if (ext == "nc") {
    return(normalizePath(artifact_path, winslash = "/", mustWork = TRUE))
  }

  if (ext == "zip") {
    unzip_dir <- file.path(
      dirname(artifact_path),
      tools::file_path_sans_ext(basename(artifact_path))
    )
    dir.create(unzip_dir, recursive = TRUE, showWarnings = FALSE)

    nc_existing <- list.files(unzip_dir, pattern = "\\.nc$", full.names = TRUE)
    if (length(nc_existing) == 0) {
      utils::unzip(artifact_path, exdir = unzip_dir)
      nc_existing <- list.files(unzip_dir, pattern = "\\.nc$", full.names = TRUE)
    }

    if (length(nc_existing) == 0) {
      stop("ZIP archive contained no NetCDF files: ", artifact_path, call. = FALSE)
    }

    return(sort(normalizePath(nc_existing, winslash = "/", mustWork = TRUE)))
  }

  stop(
    "Unsupported download artifact type '.", ext, "' for ", artifact_path,
    call. = FALSE
  )
}

request_has_usable_artifacts <- function(raw_dir, target_stub, legacy_target_stubs = character(0)) {
  primary_artifact <- resolve_download_artifact(raw_dir, target_stub)
  if (!is.na(primary_artifact)) {
    return(TRUE)
  }

  if (length(legacy_target_stubs) == 0) {
    return(FALSE)
  }

  legacy_artifacts <- vapply(
    legacy_target_stubs,
    function(stub) resolve_download_artifact(raw_dir, stub),
    FUN.VALUE = character(1)
  )

  all(!is.na(legacy_artifacts))
}

resolve_request_artifacts <- function(raw_dir, target_stub, legacy_target_stubs = character(0)) {
  stubs <- unique(c(target_stub, legacy_target_stubs))
  artifacts <- vapply(
    stubs,
    function(stub) resolve_download_artifact(raw_dir, stub),
    FUN.VALUE = character(1)
  )

  unique(unname(stats::na.omit(artifacts)))
}

build_request_manifest <- function(
  selected_specs,
  month_seq,
  variables_per_request,
  region_slug,
  query_start,
  query_end,
  split_by_step_type = FALSE
) {
  purrr::map_dfr(
    seq_along(month_seq),
    function(i) {
      month_date <- month_seq[[i]]
      request_days <- request_days_for_month(month_date, query_start, query_end)

      if (split_by_step_type) {
        purrr::map_dfr(
          unique(selected_specs$step_type),
          function(step_type_i) {
            specs_step <- selected_specs[selected_specs$step_type == step_type_i, , drop = FALSE]
            idx_chunks <- chunk_indices(nrow(specs_step), variables_per_request)

            purrr::map_dfr(
              seq_along(idx_chunks),
              function(batch_id) {
                idx <- idx_chunks[[batch_id]]
                batch_specs <- specs_step[idx, , drop = FALSE]
                data.frame(
                  month = month_date,
                  batch_id = batch_id,
                  step_type = step_type_i,
                  target_stub = sprintf(
                    "era5_%s_%s_%s_batch%02d",
                    region_slug,
                    format(month_date, "%Y_%m"),
                    step_type_i,
                    batch_id
                  ),
                  cds_vars = I(list(batch_specs$cds_name)),
                  aliases = I(list(batch_specs$alias)),
                  request_days = I(list(request_days)),
                  stringsAsFactors = FALSE
                )
              }
            )
          }
        )
      } else {
        idx_chunks <- chunk_indices(nrow(selected_specs), variables_per_request)

        purrr::map_dfr(
          seq_along(idx_chunks),
          function(batch_id) {
            idx <- idx_chunks[[batch_id]]
            batch_specs <- selected_specs[idx, , drop = FALSE]
            data.frame(
              month = month_date,
              batch_id = batch_id,
              target_stub = sprintf(
                "era5_%s_%s_batch%02d",
                region_slug,
                format(month_date, "%Y_%m"),
                batch_id
              ),
              cds_vars = I(list(batch_specs$cds_name)),
              aliases = I(list(batch_specs$alias)),
              request_days = I(list(request_days)),
              stringsAsFactors = FALSE
            )
          }
        )
      }
    }
  )
}

normalize_era5_token <- function(x) {
  x <- tolower(x)
  x <- gsub("[^a-z0-9]+", "", x)
  x
}

list_nc_variable_metadata <- function(ncfile) {
  nc <- ncdf4::nc_open(ncfile)
  on.exit(ncdf4::nc_close(nc), add = TRUE)

  var_names <- names(nc$var)
  if (length(var_names) == 0) {
    return(data.frame(
      var_name = character(0),
      long_name = character(0),
      standard_name = character(0),
      stringsAsFactors = FALSE
    ))
  }

  long_names <- vapply(
    var_names,
    function(v) {
      att <- ncdf4::ncatt_get(nc, v, "long_name")$value
      if (is.null(att) || length(att) == 0 || is.na(att)) "" else as.character(att)
    },
    FUN.VALUE = character(1)
  )

  standard_names <- vapply(
    var_names,
    function(v) {
      att <- ncdf4::ncatt_get(nc, v, "standard_name")$value
      if (is.null(att) || length(att) == 0 || is.na(att)) "" else as.character(att)
    },
    FUN.VALUE = character(1)
  )

  data.frame(
    var_name = var_names,
    long_name = long_names,
    standard_name = standard_names,
    stringsAsFactors = FALSE
  )
}

resolve_nc_var_name <- function(ncfile, spec_row) {
  meta <- list_nc_variable_metadata(ncfile)
  if (nrow(meta) == 0) {
    return(NA_character_)
  }

  sds_names <- tryCatch(names(terra::sds(ncfile)), error = function(e) character(0))
  available_names <- unique(c(meta$var_name, sds_names))

  exact_candidates <- unique(stats::na.omit(c(
    spec_row$cds_name,
    spec_row$short_name,
    spec_row$alias,
    sub("_.*$", "", spec_row$output_name)
  )))

  exact_match <- available_names[available_names %in% exact_candidates]
  if (length(exact_match) > 0) {
    return(exact_match[[1]])
  }

  meta$var_name_norm <- normalize_era5_token(meta$var_name)
  meta$long_name_norm <- normalize_era5_token(meta$long_name)
  meta$standard_name_norm <- normalize_era5_token(meta$standard_name)

  target_norms <- unique(stats::na.omit(c(
    normalize_era5_token(spec_row$cds_name),
    normalize_era5_token(spec_row$short_name),
    normalize_era5_token(spec_row$alias),
    normalize_era5_token(sub("_.*$", "", spec_row$output_name))
  )))

  exact_norm_idx <- which(
    meta$var_name_norm %in% target_norms |
      meta$long_name_norm %in% target_norms |
      meta$standard_name_norm %in% target_norms
  )
  if (length(exact_norm_idx) > 0) {
    return(meta$var_name[[exact_norm_idx[[1]]]])
  }

  contains_hits <- lapply(
    target_norms,
    function(target) {
      which(
        grepl(target, meta$var_name_norm, fixed = TRUE) |
          grepl(target, meta$long_name_norm, fixed = TRUE) |
          grepl(target, meta$standard_name_norm, fixed = TRUE)
      )
    }
  )
  contains_hits <- unique(unlist(contains_hits, use.names = FALSE))

  if (length(contains_hits) > 0) {
    return(meta$var_name[[contains_hits[[1]]]])
  }

  NA_character_
}

find_ncfile_for_variable <- function(ncfiles, spec_row) {
  for (ncfile in ncfiles) {
    nc_var_name <- tryCatch(resolve_nc_var_name(ncfile, spec_row), error = function(e) NA_character_)
    if (!is.na(nc_var_name)) {
      return(list(ncfile = ncfile, nc_var_name = nc_var_name))
    }
  }

  stop(
    "Could not find a matching NetCDF variable for CDS variable '", spec_row$cds_name,
    "' in artifact set: ",
    paste(basename(ncfiles), collapse = ", "),
    call. = FALSE
  )
}

read_era5_subdataset <- function(ncfile, nc_var_name, datetimes_utc) {
  r <- tryCatch(
    terra::rast(ncfile, subds = nc_var_name),
    error = function(e) NULL
  )

  if (is.null(r)) {
    sds <- terra::sds(ncfile)
    sds_names <- names(sds)
    idx <- match(nc_var_name, sds_names)
    if (is.na(idx)) {
      stop(
        "Could not find NetCDF variable '", nc_var_name,
        "' in file ", basename(ncfile),
        call. = FALSE
      )
    }
    r <- sds[[idx]]
  }

  if (terra::nlyr(r) != length(datetimes_utc)) {
    stop(
      "Time dimension mismatch for NetCDF variable '", nc_var_name,
      "' in file ", basename(ncfile),
      call. = FALSE
    )
  }

  terra::time(r) <- datetimes_utc
  names(r) <- format(datetimes_utc, "%Y-%m-%dT%H:%M:%SZ")
  r
}

extract_variable_panel <- function(
  ncfiles,
  spec_row,
  polygons,
  id_data,
  id_cols,
  temporal_resolution = c("daily", "hourly"),
  local_tz = "UTC"
) {
  temporal_resolution <- match.arg(temporal_resolution)

  nc_match <- find_ncfile_for_variable(ncfiles, spec_row)
  ncfile <- nc_match$ncfile
  datetimes_utc <- parse_nc_time(ncfile)
  r <- read_era5_subdataset(ncfile, nc_match$nc_var_name, datetimes_utc)
  cell_area_raster <- terra::cellSize(r[[1]], unit = "m")

  values <- exactextractr::exact_extract(
    x = r,
    y = polygons,
    fun = "weighted_mean",
    weights = cell_area_raster,
    progress = FALSE
  )

  values_mat <- as.matrix(values)
  rm(values)

  n_units <- nrow(id_data)
  n_times <- length(datetimes_utc)

  out <- id_data[rep(seq_len(n_units), each = n_times), , drop = FALSE]
  out$datetime_utc <- rep(datetimes_utc, times = n_units)
  out[[spec_row$output_name]] <- as.vector(t(values_mat)) * spec_row$scale + spec_row$offset

  rm(values_mat, r, cell_area_raster)
  gc(verbose = FALSE)

  if (temporal_resolution == "daily") {
    out <- out |>
      dplyr::mutate(
        datetime_local = lubridate::with_tz(datetime_utc, tzone = local_tz),
        date = as.Date(datetime_local)
      )

    agg_fun <- switch(
      spec_row$daily_agg,
      mean = safe_mean,
      sum = safe_sum,
      max = safe_max,
      stop("Unsupported daily_agg: ", spec_row$daily_agg, call. = FALSE)
    )

    out <- out |>
      dplyr::group_by(dplyr::across(dplyr::all_of(c(id_cols, "date")))) |>
      dplyr::summarise(
        !!spec_row$output_name := agg_fun(.data[[spec_row$output_name]]),
        .groups = "drop"
      ) |>
      dplyr::arrange(dplyr::across(dplyr::all_of(id_cols)), date)
  } else {
    out <- out |>
      dplyr::mutate(datetime_local = lubridate::with_tz(datetime_utc, tzone = local_tz)) |>
      dplyr::arrange(dplyr::across(dplyr::all_of(id_cols)), datetime_utc)
  }

  out
}

aggregate_hourly_to_daily <- function(hourly_panel, specs, id_cols, local_tz) {
  hourly_panel <- hourly_panel |>
    dplyr::mutate(
      datetime_local = lubridate::with_tz(datetime_utc, tzone = local_tz),
      date = as.Date(datetime_local)
    )

  mean_vars <- specs$output_name[specs$daily_agg == "mean"]
  sum_vars  <- specs$output_name[specs$daily_agg == "sum"]
  max_vars  <- specs$output_name[specs$daily_agg == "max"]

  daily_panel <- hourly_panel |>
    dplyr::group_by(dplyr::across(dplyr::all_of(c(id_cols, "date")))) |>
    dplyr::summarise(
      dplyr::across(dplyr::all_of(mean_vars), safe_mean),
      dplyr::across(dplyr::all_of(sum_vars), safe_sum),
      dplyr::across(dplyr::all_of(max_vars), safe_max),
      .groups = "drop"
    ) |>
    dplyr::arrange(dplyr::across(dplyr::all_of(id_cols)), date)

  daily_panel
}

download_with_retry <- function(
  request,
  path,
  max_tries = 7,
  wait_seconds = 2,
  max_wait_seconds = 600
) {
  for (i in seq_len(max_tries)) {
    res <- tryCatch(
      {
        ecmwfr::wf_request(
          request = request,
          path = path,
          transfer = TRUE,
          verbose = TRUE
        )
      },
      error = function(e) e
    )

    if (!inherits(res, "error")) {
      return(invisible(TRUE))
    }

    msg <- conditionMessage(res)

    if (grepl(
      "429|Rate limit exceeded|Too Many Requests|timed out|temporar|try again|connection reset|proxy error",
      msg,
      ignore.case = TRUE
    )) {
      wait_for <- min(wait_seconds * (2^(i - 1)), max_wait_seconds)
      message(
        sprintf(
          "Temporary CDS error on try %s/%s. Waiting %s seconds...",
          i,
          max_tries,
          wait_for
        )
      )
      Sys.sleep(wait_for)
    } else {
      stop(msg, call. = FALSE)
    }
  }

  stop("Download failed after repeated temporary-error retries.", call. = FALSE)
}

build_era5_tract_panel <- function(
  polygons,
  start_date,
  end_date,
  variables = default_era5_variables(),
  id_cols = NULL,
  region_name = NULL,
  local_tz = "UTC",
  temporal_resolution = c("daily", "hourly"),
  bbox_padding = 0.25,
  variables_per_request = 6,
  request_pause_seconds = 1,
  data_dir = file.path(getwd(), "Data", "era5_single_levels"),
  overwrite_downloads = FALSE,
  overwrite_output = FALSE,
  write_parquet = TRUE
) {
  check_era5_packages()

  temporal_resolution <- match.arg(temporal_resolution)

  start_date <- as.Date(start_date)
  end_date <- as.Date(end_date)

  if (is.na(start_date) || is.na(end_date)) {
    stop("start_date and end_date must be coercible to Date.", call. = FALSE)
  }
  if (start_date > end_date) {
    stop("start_date must be on or before end_date.", call. = FALSE)
  }
  if (!inherits(polygons, "sf")) {
    stop("polygons must be an sf object.", call. = FALSE)
  }
  if (!is.numeric(bbox_padding) || length(bbox_padding) != 1 || bbox_padding < 0) {
    stop("bbox_padding must be a single non-negative number.", call. = FALSE)
  }
  if (!is.numeric(variables_per_request) || length(variables_per_request) != 1 || variables_per_request < 1) {
    stop("variables_per_request must be a positive integer.", call. = FALSE)
  }
  if (!is.numeric(request_pause_seconds) || length(request_pause_seconds) != 1 || request_pause_seconds < 0) {
    stop("request_pause_seconds must be a single non-negative number.", call. = FALSE)
  }

  # Ensure the timezone is valid early.
  test_time <- tryCatch(
    lubridate::with_tz(as.POSIXct("2024-01-01 00:00:00", tz = "UTC"), tzone = local_tz),
    error = function(e) e
  )
  if (inherits(test_time, "error")) {
    stop("local_tz is not a valid timezone string.", call. = FALSE)
  }

  polygons <- sf::st_make_valid(polygons)
  polygons <- polygons[!sf::st_is_empty(polygons), , drop = FALSE]
  if (nrow(polygons) == 0) {
    stop("polygons has no valid non-empty geometries after cleaning.", call. = FALSE)
  }

  polygons <- sf::st_transform(polygons, 4326)

  if (is.null(id_cols)) {
    candidate_cols <- intersect(c("GEOID", "NAME"), names(polygons))
    if (length(candidate_cols) == 0) {
      polygons$unit_id <- sprintf("unit_%05d", seq_len(nrow(polygons)))
      id_cols <- "unit_id"
    } else {
      id_cols <- candidate_cols
    }
  }

  missing_id_cols <- setdiff(id_cols, names(polygons))
  if (length(missing_id_cols) > 0) {
    stop(
      "The following id_cols are missing from polygons: ",
      paste(missing_id_cols, collapse = ", "),
      call. = FALSE
    )
  }

  region_slug <- if (is.null(region_name)) {
    slugify_region_name(paste0("bbox_", paste(round(sf::st_bbox(polygons), 2), collapse = "_")))
  } else {
    slugify_region_name(region_name)
  }

  dir.create(data_dir, recursive = TRUE, showWarnings = FALSE)
  raw_dir <- file.path(data_dir, "raw", region_slug)
  processed_dir <- file.path(data_dir, "processed", region_slug)
  dir.create(raw_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(processed_dir, recursive = TRUE, showWarnings = FALSE)

  catalog <- era5_variable_catalog()

  # Allow either aliases or native CDS names in `variables`.
  selected_specs <- dplyr::bind_rows(
    catalog[catalog$alias %in% variables, , drop = FALSE],
    catalog[catalog$cds_name %in% variables, , drop = FALSE]
  ) |>
    dplyr::distinct(alias, .keep_all = TRUE)

  if (nrow(selected_specs) == 0) {
    stop("No valid ERA5 variables were selected.", call. = FALSE)
  }

  unresolved_variables <- setdiff(variables, c(selected_specs$alias, selected_specs$cds_name))
  if (length(unresolved_variables) > 0) {
    stop(
      "Unknown ERA5 variables: ",
      paste(unresolved_variables, collapse = ", "),
      call. = FALSE
    )
  }

  # One extra day on each side avoids timezone-edge truncation when aggregating to local days.
  query_start <- start_date - 1L
  query_end <- end_date + 1L

  month_seq <- seq.Date(
    from = lubridate::floor_date(query_start, unit = "month"),
    to   = lubridate::floor_date(query_end, unit = "month"),
    by   = "month"
  )

  hours_full_day <- sprintf("%02d:00", 0:23)

  tract_bb <- sf::st_bbox(polygons)
  area_bbox <- c(
    min(90,  unname(tract_bb["ymax"]) + bbox_padding),
    max(-180, unname(tract_bb["xmin"]) - bbox_padding),
    max(-90,  unname(tract_bb["ymin"]) - bbox_padding),
    min(180,  unname(tract_bb["xmax"]) + bbox_padding)
  )

  request_manifest <- build_request_manifest(
    selected_specs = selected_specs,
    month_seq = month_seq,
    variables_per_request = variables_per_request,
    region_slug = region_slug,
    query_start = query_start,
    query_end = query_end,
    split_by_step_type = FALSE
  )

  legacy_request_manifest <- build_request_manifest(
    selected_specs = selected_specs,
    month_seq = month_seq,
    variables_per_request = variables_per_request,
    region_slug = region_slug,
    query_start = query_start,
    query_end = query_end,
    split_by_step_type = TRUE
  )

  request_manifest$legacy_target_stubs <- lapply(
    seq_len(nrow(request_manifest)),
    function(i) {
      entry <- request_manifest[i, , drop = FALSE]
      same_month <- legacy_request_manifest$month == entry$month
      overlaps_alias <- vapply(
        legacy_request_manifest$aliases,
        function(aliases) any(aliases %in% unlist(entry$aliases)),
        FUN.VALUE = logical(1)
      )
      unique(legacy_request_manifest$target_stub[same_month & overlaps_alias])
    }
  )

  needs_download <- vapply(
    seq_len(nrow(request_manifest)),
    function(i) {
      if (overwrite_downloads) {
        return(TRUE)
      }

      !request_has_usable_artifacts(
        raw_dir = raw_dir,
        target_stub = request_manifest$target_stub[[i]],
        legacy_target_stubs = unlist(request_manifest$legacy_target_stubs[[i]])
      )
    },
    FUN.VALUE = logical(1)
  )

  if (any(needs_download)) {
    stored_key <- tryCatch(ecmwfr::wf_get_key(), error = function(e) e)
    if (inherits(stored_key, "error") || length(stored_key) == 0) {
      stop(
        "No CDS token found. First accept the ERA5 terms in CDS, then store your token with ecmwfr::wf_set_key().",
        call. = FALSE
      )
    }

    remaining_downloads <- sum(needs_download)

    for (i in seq_len(nrow(request_manifest))) {
      if (!needs_download[[i]]) {
        next
      }

      entry <- request_manifest[i, , drop = FALSE]
      cds_request <- list(
        dataset_short_name = "reanalysis-era5-single-levels",
        product_type = "reanalysis",
        variable = unlist(entry$cds_vars),
        year = format(entry$month, "%Y"),
        month = format(entry$month, "%m"),
        day = unlist(entry$request_days),
        time = hours_full_day,
        area = area_bbox,
        data_format = "netcdf",
        download_format = "unarchived",
        target = paste0(entry$target_stub, ".nc")
      )

      message(
        sprintf(
          "Downloading %s | batch %02d | days %s-%s | vars: %s",
          format(entry$month, "%Y-%m"),
          entry$batch_id,
          min(unlist(entry$request_days)),
          max(unlist(entry$request_days)),
          paste(unlist(entry$cds_vars), collapse = ", ")
        )
      )

      tryCatch(
        download_with_retry(
          request = cds_request,
          path = raw_dir
        ),
        error = function(e) {
          stop(
            "Failed to download ", entry$target_stub, ": ",
            conditionMessage(e),
            call. = FALSE
          )
        }
      )

      downloaded_artifact <- resolve_download_artifact(raw_dir, entry$target_stub)
      if (is.na(downloaded_artifact)) {
        stop(
          "Download finished, but neither .nc nor .zip was found for request ",
          entry$target_stub,
          call. = FALSE
        )
      }

      remaining_downloads <- remaining_downloads - 1L
      if (request_pause_seconds > 0 && remaining_downloads > 0) {
        Sys.sleep(request_pause_seconds)
      }
    }
  } else {
    message("All requested ERA5 files already exist in ", raw_dir)
  }

  output_stub <- sprintf(
    "era5_%s_%s_%s_%s",
    region_slug,
    temporal_resolution,
    format(start_date, "%Y%m%d"),
    format(end_date, "%Y%m%d")
  )

  parquet_path <- file.path(processed_dir, paste0(output_stub, ".parquet"))
  metadata_path <- file.path(processed_dir, paste0(output_stub, "_variables.csv"))

  if (file.exists(parquet_path) && !overwrite_output) {
    message("Reading existing processed panel from ", parquet_path)
    out <- arrow::read_parquet(parquet_path)
    attr(out, "era5_variable_catalog") <- selected_specs
    attr(out, "era5_output_path") <- parquet_path
    return(out)
  }

  id_data <- sf::st_drop_geometry(polygons)[, id_cols, drop = FALSE]
  extraction_resolution <- if (temporal_resolution == "daily") "hourly" else temporal_resolution
  extraction_join_keys <- if (extraction_resolution == "daily") c(id_cols, "date") else c(id_cols, "datetime_utc", "datetime_local")
  variable_parts <- stats::setNames(vector("list", nrow(selected_specs)), selected_specs$output_name)

  for (i in seq_len(nrow(request_manifest))) {
    entry <- request_manifest[i, , drop = FALSE]
    artifact_paths <- resolve_request_artifacts(
      raw_dir = raw_dir,
      target_stub = entry$target_stub,
      legacy_target_stubs = unlist(entry$legacy_target_stubs)
    )
    if (length(artifact_paths) == 0) {
      stop("Missing download artifact(s) for request ", entry$target_stub, call. = FALSE)
    }

    ncfiles <- unique(unlist(lapply(artifact_paths, list_nc_sources), use.names = FALSE))
    batch_specs <- selected_specs[selected_specs$alias %in% unlist(entry$aliases), , drop = FALSE]

    batch_parts <- purrr::map(
      seq_len(nrow(batch_specs)),
      function(j) {
        extract_variable_panel(
          ncfiles = ncfiles,
          spec_row = batch_specs[j, , drop = FALSE],
          polygons = polygons,
          id_data = id_data,
          id_cols = id_cols,
          temporal_resolution = extraction_resolution,
          local_tz = local_tz
        )
      }
    )

    for (j in seq_len(nrow(batch_specs))) {
      output_name <- batch_specs$output_name[[j]]
      variable_parts[[output_name]][[length(variable_parts[[output_name]]) + 1L]] <- batch_parts[[j]]
    }

    rm(batch_parts)
    gc(verbose = FALSE)
  }

  variable_panels <- purrr::map(
    seq_len(nrow(selected_specs)),
    function(i) {
      spec_row <- selected_specs[i, , drop = FALSE]
      parts_i <- variable_parts[[spec_row$output_name]]

      if (length(parts_i) == 0) {
        return(NULL)
      }

      dplyr::bind_rows(parts_i) |>
        dplyr::group_by(dplyr::across(dplyr::all_of(extraction_join_keys))) |>
        dplyr::summarise(
          !!spec_row$output_name := safe_first_non_na(.data[[spec_row$output_name]]),
          .groups = "drop"
        ) |>
        dplyr::arrange(dplyr::across(dplyr::all_of(extraction_join_keys)))
    }
  )
  variable_panels <- variable_panels[!vapply(variable_panels, is.null, logical(1))]

  if (length(variable_panels) == 0) {
    stop("No ERA5 panels were extracted for the requested variables.", call. = FALSE)
  }

  panel <- purrr::reduce(variable_panels, dplyr::full_join, by = extraction_join_keys)

  if (temporal_resolution == "daily") {
    panel <- aggregate_hourly_to_daily(
      hourly_panel = panel,
      specs = selected_specs,
      id_cols = id_cols,
      local_tz = local_tz
    ) |>
      dplyr::filter(date >= start_date, date <= end_date) |>
      dplyr::arrange(dplyr::across(dplyr::all_of(id_cols)), date)
  } else {
    panel <- panel |>
      dplyr::filter(
        datetime_local >= as.POSIXct(start_date, tz = local_tz),
        datetime_local < as.POSIXct(end_date + 1L, tz = local_tz)
      ) |>
      dplyr::arrange(dplyr::across(dplyr::all_of(id_cols)), datetime_utc)
  }

  if (all(c("u10_ms", "v10_ms") %in% names(panel)) && !("ws10_ms" %in% names(panel))) {
    panel$ws10_ms <- sqrt(panel$u10_ms^2 + panel$v10_ms^2)
  }

  # Save output and a machine-readable variable dictionary.
  if (write_parquet) {
    arrow::write_parquet(panel, parquet_path)
    utils::write.csv(selected_specs, metadata_path, row.names = FALSE)
  }

  attr(panel, "era5_variable_catalog") <- selected_specs
  attr(panel, "era5_output_path") <- parquet_path
  panel
}

run_era5_state_with_retry <- function(
  state_polygons,
  state_name,
  local_tz,
  max_tries = Inf,
  wait_seconds = 10
) {
  attempt <- 0

  repeat {
    attempt <- attempt + 1
    message("Starting ", state_name, " | attempt ", attempt)

    result <- tryCatch(
      {
        build_era5_tract_panel(
          polygons = state_polygons,
          start_date = "2010-01-01",
          end_date = "2025-12-31",
          region_name = state_name,
          local_tz = local_tz,
          # Note only use hourly if you have a lot of RAM
          temporal_resolution = "daily",
          overwrite_output = TRUE,
          overwrite_downloads = FALSE
        )
      },
      error = function(e) e
    )

    if (!inherits(result, "error")) {
      return(result)
    }

    message("Failed for ", state_name, ": ", conditionMessage(result))

    if (is.finite(max_tries) && attempt >= max_tries) {
      stop("Giving up on ", state_name, " after ", attempt, " attempts.")
    }

    Sys.sleep(wait_seconds)
  }
}

download_era5_state_files <- function(
  states = c(
    "Florida",
    "Texas",
    "South Carolina",
    "North Carolina",
    "Louisiana",
    "Puerto Rico"
  ),
  years = 2010:2025,
  variables = c(
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "10m_wind_gust_since_previous_post_processing",
    "total_precipitation"
  ),
  data_dir = file.path(getwd(), "Data", "era5_files"),
  variables_per_request = 6,
  bbox_padding = 0.25,
  request_pause_seconds = 2,
  overwrite = FALSE,
  max_tries = 5,
  wait_seconds = 10
) {
  needed_pkgs <- c("ecmwfr", "sf", "dplyr", "purrr", "lubridate", "tigris")
  missing_pkgs <- needed_pkgs[
    !vapply(needed_pkgs, requireNamespace, logical(1), quietly = TRUE)
  ]

  if (length(missing_pkgs) > 0) {
    stop(
      "Missing packages: ",
      paste(missing_pkgs, collapse = ", "),
      call. = FALSE
    )
  }

  catalog <- era5_variable_catalog()
  selected_specs <- dplyr::bind_rows(
    catalog[catalog$alias %in% variables, , drop = FALSE],
    catalog[catalog$cds_name %in% variables, , drop = FALSE]
  ) |>
    dplyr::distinct(alias, .keep_all = TRUE)

  if (nrow(selected_specs) == 0) {
    stop("No valid ERA5 variables were selected.", call. = FALSE)
  }

  unresolved_variables <- setdiff(variables, c(selected_specs$alias, selected_specs$cds_name))
  if (length(unresolved_variables) > 0) {
    stop(
      "Unknown ERA5 variables: ",
      paste(unresolved_variables, collapse = ", "),
      call. = FALSE
    )
  }

  stored_key <- tryCatch(ecmwfr::wf_get_key(), error = function(e) e)
  if (inherits(stored_key, "error") || length(stored_key) == 0) {
    stop(
      "No CDS token found. First accept the ERA5 terms in CDS, then store your token with ecmwfr::wf_set_key().",
      call. = FALSE
    )
  }

  all_states <- tigris::states(cb = TRUE, class = "sf", year = 2024)
  all_states <- sf::st_transform(all_states, 4326)
  state_lookup <- stats::setNames(seq_len(nrow(all_states)), tolower(all_states$NAME))
  matched_idx <- unname(state_lookup[tolower(states)])

  if (any(is.na(matched_idx))) {
    stop(
      "Could not match these state names: ",
      paste(states[is.na(matched_idx)], collapse = ", "),
      call. = FALSE
    )
  }

  dir.create(data_dir, recursive = TRUE, showWarnings = FALSE)

  start_date <- as.Date(sprintf("%s-01-01", min(years)))
  end_date <- as.Date(sprintf("%s-12-31", max(years)))
  month_seq <- seq.Date(
    from = lubridate::floor_date(start_date, unit = "month"),
    to = lubridate::floor_date(end_date, unit = "month"),
    by = "month"
  )
  hours_full_day <- sprintf("%02d:00", 0:23)

  out <- vector("list", length(states))

  for (state_i in seq_along(states)) {
    state_name <- states[[state_i]]
    state_geom <- all_states[matched_idx[[state_i]], , drop = FALSE]
    region_slug <- slugify_region_name(state_name)
    state_dir <- file.path(data_dir, region_slug)
    dir.create(state_dir, recursive = TRUE, showWarnings = FALSE)

    state_bb <- sf::st_bbox(state_geom)
    area_bbox <- c(
      min(90, unname(state_bb["ymax"]) + bbox_padding),
      max(-180, unname(state_bb["xmin"]) - bbox_padding),
      max(-90, unname(state_bb["ymin"]) - bbox_padding),
      min(180, unname(state_bb["xmax"]) + bbox_padding)
    )

    request_manifest <- build_request_manifest(
      selected_specs = selected_specs,
      month_seq = month_seq,
      # Monthly batches keep NetCDF requests comfortably smaller.
      variables_per_request = variables_per_request,
      region_slug = region_slug,
      query_start = start_date,
      query_end = end_date,
      split_by_step_type = FALSE
    )

    state_results <- purrr::map_dfr(
      seq_len(nrow(request_manifest)),
      function(i) {
        entry <- request_manifest[i, , drop = FALSE]
        target_file <- file.path(state_dir, paste0(entry$target_stub, ".nc"))
        existing_artifact <- resolve_download_artifact(state_dir, entry$target_stub)

        if (!overwrite && !is.na(existing_artifact)) {
          return(data.frame(
            state = state_name,
            year = as.integer(format(entry$month, "%Y")),
            month = as.integer(format(entry$month, "%m")),
            batch_id = entry$batch_id,
            target_stub = entry$target_stub,
            file_path = existing_artifact,
            status = "skipped_existing",
            stringsAsFactors = FALSE
          ))
        }

        cds_request <- list(
          dataset_short_name = "reanalysis-era5-single-levels",
          product_type = "reanalysis",
          variable = unlist(entry$cds_vars),
          year = format(entry$month, "%Y"),
          month = format(entry$month, "%m"),
          day = unlist(entry$request_days),
          time = hours_full_day,
          area = area_bbox,
          # Current CDS requests need both keys for direct NetCDF output.
          data_format = "netcdf",
          download_format = "unarchived",
          target = basename(target_file)
        )

        message(
          sprintf(
            "Downloading %s | %s | batch %02d | vars: %s",
            state_name,
            format(entry$month, "%Y-%m"),
            entry$batch_id,
            paste(unlist(entry$cds_vars), collapse = ", ")
          )
        )

        result <- tryCatch(
          {
            download_with_retry(
              request = cds_request,
              path = state_dir,
              max_tries = max_tries,
              wait_seconds = wait_seconds
            )
            NULL
          },
          error = function(e) e
        )

        downloaded_artifact <- resolve_download_artifact(state_dir, entry$target_stub)

        if (inherits(result, "error") || is.na(downloaded_artifact)) {
          warning(
            sprintf(
              "Failed to download %s (%s): %s",
              state_name,
              entry$target_stub,
              if (inherits(result, "error")) conditionMessage(result) else "no file was written"
            ),
            call. = FALSE
          )

          return(data.frame(
            state = state_name,
            year = as.integer(format(entry$month, "%Y")),
            month = as.integer(format(entry$month, "%m")),
            batch_id = entry$batch_id,
            target_stub = entry$target_stub,
            file_path = NA_character_,
            status = "failed",
            stringsAsFactors = FALSE
          ))
        }

        if (request_pause_seconds > 0) {
          Sys.sleep(request_pause_seconds)
        }

        data.frame(
          state = state_name,
          year = as.integer(format(entry$month, "%Y")),
          month = as.integer(format(entry$month, "%m")),
          batch_id = entry$batch_id,
          target_stub = entry$target_stub,
          file_path = downloaded_artifact,
          status = "downloaded",
          stringsAsFactors = FALSE
        )
      }
    )

    out[[state_i]] <- state_results
  }

  dplyr::bind_rows(out)
}

############# Saved from do_get_data.R #############

#### Get ERA5 Weather Data ####

state_specs <- tibble(
  state_fips = states_list,
  state_name = states_names_list,
  local_tz = states_tz_list
)

## Loop for data extraction and systematic panel creation for all states and years

era5_panel <- purrr::pmap_dfr(
  state_specs,
  function(state_fips, state_name, local_tz) {
    state_polygons <- county_polygons_all %>%
      filter(stringr::str_sub(GEOID, 1, 2) == state_fips)

    if (nrow(state_polygons) == 0) {
      stop("No polygons found in county_polygons_all for state FIPS ", state_fips)
    }

    message(
      "Building ERA5 panel for ",
      state_name,
      " (",
      nrow(state_polygons),
      " polygons)"
    )

    run_era5_state_with_retry(
      state_polygons = state_polygons,
      state_name = state_name,
      local_tz = local_tz
    )
  }
)

# panel for florida for 2010 - temporary - delete later

run_era5_state_with_rdd <- function(
  max_tries = Inf,
  wait_seconds = 10
) {
  attempt <- 0

  repeat {
    attempt <- attempt + 1

    result <- tryCatch(
      {
        download_era5_state_files()
      },
      error = function(e) e
    )

    if (!inherits(result, "error")) {
      return(result)
    }


    if (is.finite(max_tries) && attempt >= max_tries) {
      stop()
    }

    Sys.sleep(wait_seconds)
  }
}

run_era5_state_with_rdd()


era5_panel <- build_era5_tract_panel(
          polygons = county_polygons_fl,
          start_date = "2010-01-01",
          end_date = "2010-12-31",
          region_name = "Florida",
          # local_tz = local_tz,
          # Note only use hourly if you have a lot of RAM
          temporal_resolution = "daily",
          overwrite_output = TRUE,
          overwrite_downloads = FALSE
        )

saveRDS(era5_panel, "Output/era5_panel.rds")

era5_panel_raw <- readRDS("Output/era5_panel.rds")

era5_panel_yearly <- era5_panel_raw %>%
  mutate(
    year = year(date),
    # Helper variable to count days
    row_of_ones = 1
  ) %>%
  group_by(GEOID, year) %>%
  summarize(
    amount_days_era5           = n(),
    u10_ms_min                 = min(u10_ms),
    u10_ms_med                 = median(u10_ms),
    u10_ms_mean                = mean(u10_ms),
    u10_ms_max                 = max(u10_ms),
    u10_ms_sd                  = sd(u10_ms),
    u10_ms_days_above_10       = sum(row_of_ones[u10_ms > 10]),
    u10_ms_days_above_20       = sum(row_of_ones[u10_ms > 20]),
    u10_ms_days_above_30       = sum(row_of_ones[u10_ms > 30]),
    v10_ms_min                 = min(v10_ms),
    v10_ms_med                 = median(v10_ms),
    v10_ms_mean                = mean(v10_ms),
    v10_ms_max                 = max(v10_ms),
    v10_ms_sd                  = sd(v10_ms),
    v10_ms_days_above_10       = sum(row_of_ones[v10_ms > 10]),
    v10_ms_days_above_20       = sum(row_of_ones[v10_ms > 20]),
    v10_ms_days_above_30       = sum(row_of_ones[v10_ms > 30]),
    mslp_hpa_min               = min(mslp_hpa),
    mslp_hpa_med               = median(mslp_hpa),
    mslp_hpa_mean              = mean(mslp_hpa),
    mslp_hpa_max               = max(mslp_hpa),
    mslp_hpa_sd                = sd(mslp_hpa),
    mslp_hpa_days_above_10     = sum(row_of_ones[mslp_hpa > 10]),
    mslp_hpa_days_above_20     = sum(row_of_ones[mslp_hpa > 20]),
    mslp_hpa_days_above_30     = sum(row_of_ones[mslp_hpa > 30]),
    sp_hpa_min                 = min(sp_hpa),
    sp_hpa_med                 = median(sp_hpa),
    sp_hpa_mean                = mean(sp_hpa),
    sp_hpa_max                 = max(sp_hpa),
    sp_hpa_sd                  = sd(sp_hpa),
    sp_hpa_days_above_10       = sum(row_of_ones[sp_hpa > 10]),
    sp_hpa_days_above_20       = sum(row_of_ones[sp_hpa > 20]),
    sp_hpa_days_above_30       = sum(row_of_ones[sp_hpa > 30]),
    t2m_c_min                  = min(t2m_c),
    t2m_c_med                  = median(t2m_c),
    t2m_c_mean                 = mean(t2m_c),
    t2m_c_max                  = max(t2m_c),
    t2m_c_sd                   = sd(t2m_c),
    t2m_c_days_above_10        = sum(row_of_ones[t2m_c > 10]),
    t2m_c_days_above_20        = sum(row_of_ones[t2m_c > 20]),
    t2m_c_days_above_30        = sum(row_of_ones[t2m_c > 30]),
    swvl1_m3_m3_min            = min(swvl1_m3_m3),
    swvl1_m3_m3_med            = median(swvl1_m3_m3),
    swvl1_m3_m3_mean           = mean(swvl1_m3_m3),
    swvl1_m3_m3_max            = max(swvl1_m3_m3),
    swvl1_m3_m3_sd             = sd(swvl1_m3_m3),
    swvl1_m3_m3_days_above_10  = sum(row_of_ones[swvl1_m3_m3 > 10]),
    swvl1_m3_m3_days_above_20  = sum(row_of_ones[swvl1_m3_m3 > 20]),
    swvl1_m3_m3_days_above_30  = sum(row_of_ones[swvl1_m3_m3 > 30]),
    swvl2_m3_m3_min            = min(swvl2_m3_m3),
    swvl2_m3_m3_med            = median(swvl2_m3_m3),
    swvl2_m3_m3_mean           = mean(swvl2_m3_m3),
    swvl2_m3_m3_max            = max(swvl2_m3_m3),
    swvl2_m3_m3_sd             = sd(swvl2_m3_m3),
    swvl2_m3_m3_days_above_10  = sum(row_of_ones[swvl2_m3_m3 > 10]),
    swvl2_m3_m3_days_above_20  = sum(row_of_ones[swvl2_m3_m3 > 20]),
    swvl2_m3_m3_days_above_30  = sum(row_of_ones[swvl2_m3_m3 > 30]),
    swvl3_m3_m3_min            = min(swvl3_m3_m3),
    swvl3_m3_m3_med            = median(swvl3_m3_m3),
    swvl3_m3_m3_mean           = mean(swvl3_m3_m3),
    swvl3_m3_m3_max            = max(swvl3_m3_m3),
    swvl3_m3_m3_sd             = sd(swvl3_m3_m3),
    swvl3_m3_m3_days_above_10  = sum(row_of_ones[swvl3_m3_m3 > 10]),
    swvl3_m3_m3_days_above_20  = sum(row_of_ones[swvl3_m3_m3 > 20]),
    swvl3_m3_m3_days_above_30  = sum(row_of_ones[swvl3_m3_m3 > 30]),
    swvl4_m3_m3_min            = min(swvl4_m3_m3),
    swvl4_m3_m3_med            = median(swvl4_m3_m3),
    swvl4_m3_m3_mean           = mean(swvl4_m3_m3),
    swvl4_m3_m3_max            = max(swvl4_m3_m3),
    swvl4_m3_m3_sd             = sd(swvl4_m3_m3),
    swvl4_m3_m3_days_above_10  = sum(row_of_ones[swvl4_m3_m3 > 10]),
    swvl4_m3_m3_days_above_20  = sum(row_of_ones[swvl4_m3_m3 > 20]),
    swvl4_m3_m3_days_above_30  = sum(row_of_ones[swvl4_m3_m3 > 30]),
    tp_mm_min                  = min(tp_mm),
    tp_mm_med                  = median(tp_mm),
    tp_mm_mean                 = mean(tp_mm),
    tp_mm_max                  = max(tp_mm),
    tp_mm_sd                   = sd(tp_mm),
    tp_mm_days_above_10        = sum(row_of_ones[tp_mm > 10]),
    tp_mm_days_above_20        = sum(row_of_ones[tp_mm > 20]),
    tp_mm_days_above_30        = sum(row_of_ones[tp_mm > 30]),
    runoff_mm_min              = min(runoff_mm),
    runoff_mm_med              = median(runoff_mm),
    runoff_mm_mean             = mean(runoff_mm),
    runoff_mm_max              = max(runoff_mm),
    runoff_mm_sd               = sd(runoff_mm),
    runoff_mm_days_above_10    = sum(row_of_ones[runoff_mm > 10]),
    runoff_mm_days_above_20    = sum(row_of_ones[runoff_mm > 20]),
    runoff_mm_days_above_30    = sum(row_of_ones[runoff_mm > 30]),
    gust10_ms_min              = min(gust10_ms),
    gust10_ms_med              = median(gust10_ms),
    gust10_ms_mean             = mean(gust10_ms),
    gust10_ms_max              = max(gust10_ms),
    gust10_ms_sd               = sd(gust10_ms),
    gust10_ms_days_above_10    = sum(row_of_ones[gust10_ms > 10]),
    gust10_ms_days_above_20    = sum(row_of_ones[gust10_ms > 20]),
    gust10_ms_days_above_30    = sum(row_of_ones[gust10_ms > 30]),
    ws10_ms_min                = min(ws10_ms),
    ws10_ms_med                = median(ws10_ms),
    ws10_ms_mean               = mean(ws10_ms),
    ws10_ms_max                = max(ws10_ms),
    ws10_ms_sd                 = sd(ws10_ms),
    ws10_ms_days_above_10      = sum(row_of_ones[ws10_ms > 10]),
    ws10_ms_days_above_20      = sum(row_of_ones[ws10_ms > 20]),
    ws10_ms_days_above_30      = sum(row_of_ones[ws10_ms > 30]),
    .groups = "drop"
  )

  # Merge panel
out_panel_era5 <- out_panel %>%
  left_join(era5_panel_yearly, by = c("GEOID", "year")) %>%
  filter(startsWith(GEOID, "12")) %>%
  filter(year == 2010)
