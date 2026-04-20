# Download ERA5 hourly single-level files for the bounding box of a shapefile.
#
# Before first use:
# 1) Accept the ERA5 single-levels licence in the Climate Data Store.
# 2) Store your CDS token with ecmwfr::wf_set_key().

download_era5_files <- function(
  shapefile,
  start_date,
  end_date,
  variables,
  output_dir = file.path(getwd(), "Data", "era5_files", "raw"),
  file_prefix = "era5",
  data_format = "netcdf",
  bbox_padding = 0.25,
  overwrite = FALSE,
  use_batch = FALSE,
  workers = 2,
  user = "ecmwfr",
  time_out = 3600,
  retry = 30,
  total_timeout = NULL
) {
  # Read the shapefile, or use it directly if an sf object was supplied.
  shape <- if (inherits(shapefile, "sf")) {
    shapefile
  } else {
    sf::st_read(shapefile, quiet = TRUE)
  }

  # ERA5 area requests use latitude/longitude coordinates.
  shape <- sf::st_transform(shape, 4326)
  bbox <- sf::st_bbox(shape)

  # CDS expects area as North, West, South, East.
  area <- c(
    min(90, unname(bbox["ymax"]) + bbox_padding),
    max(-180, unname(bbox["xmin"]) - bbox_padding),
    max(-90, unname(bbox["ymin"]) - bbox_padding),
    min(180, unname(bbox["xmax"]) + bbox_padding)
  )

  # Build one request per calendar month to keep files and CDS jobs manageable.
  start_date <- as.Date(start_date)
  end_date <- as.Date(end_date)
  if (is.na(start_date) || is.na(end_date) || end_date < start_date) {
    stop("`start_date` and `end_date` must define a valid date range.")
  }

  first_month <- as.Date(format(start_date, "%Y-%m-01"))
  last_month <- as.Date(format(end_date, "%Y-%m-01"))
  months <- seq.Date(first_month, last_month, by = "month")
  hours <- sprintf("%02d:00", 0:23)

  # Save files in the requested folder.
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

  # Use compact file names that still identify the requested variables.
  data_format <- match.arg(tolower(data_format), choices = c("netcdf", "grib"))
  file_extension <- if (data_format == "netcdf") "nc" else "grib"
  variable_slug <- tolower(gsub("[^a-zA-Z0-9]+", "_", paste(variables, collapse = "_")))
  variable_slug <- substr(gsub("(^_+|_+$)", "", variable_slug), 1, 80)

  downloads <- vector("list", length(months))
  batch_requests <- list()
  batch_download_indexes <- integer()

  for (i in seq_along(months)) {
    month_start <- months[[i]]
    month_end <- seq.Date(month_start, by = "month", length.out = 2)[[2]] - 1
    request_days <- seq.Date(
      max(start_date, month_start),
      min(end_date, month_end),
      by = "day"
    )

    target <- sprintf(
      "%s_%s_%s.%s",
      file_prefix,
      format(month_start, "%Y_%m"),
      variable_slug,
      file_extension
    )
    target_path <- file.path(output_dir, target)

    # Skip files that have already been downloaded unless overwrite = TRUE.
    if (file.exists(target_path) && !overwrite) {
      downloads[[i]] <- data.frame(
        year = as.integer(format(month_start, "%Y")),
        month = as.integer(format(month_start, "%m")),
        file_path = target_path,
        status = "skipped_existing",
        stringsAsFactors = FALSE
      )
      next
    }

    # Build the request once; it can then be submitted either singly or as part
    # of a batch, depending on `use_batch`.
    request <- list(
      dataset_short_name = "reanalysis-era5-single-levels",
      product_type = "reanalysis",
      variable = variables,
      year = format(month_start, "%Y"),
      month = format(month_start, "%m"),
      day = sprintf("%02d", as.integer(format(request_days, "%d"))),
      time = hours,
      area = area,
      data_format = data_format,
      download_format = "unarchived",
      target = target
    )

    downloads[[i]] <- data.frame(
      year = as.integer(format(month_start, "%Y")),
      month = as.integer(format(month_start, "%m")),
      file_path = target_path,
      status = "pending",
      stringsAsFactors = FALSE
    )

    if (use_batch) {
      # In batch mode, collect all pending monthly requests and submit them
      # together after the loop with ecmwfr::wf_request_batch().
      batch_requests[[length(batch_requests) + 1]] <- request
      batch_download_indexes <- c(batch_download_indexes, i)
      next
    }

    # Default mode keeps the previous behaviour: submit each monthly request
    # immediately and wait for that file before moving on.
    message("Downloading ERA5 ", format(month_start, "%Y-%m"), " to ", target)
    ecmwfr::wf_request(
      request = request,
      user = user,
      path = output_dir,
      transfer = TRUE,
      time_out = time_out,
      retry = retry,
      verbose = TRUE
    )

    downloads[[i]]$status <- "downloaded"
  }

  if (use_batch && length(batch_requests) > 0) {
    if (workers < 1) {
      stop("`workers` must be at least 1 when `use_batch = TRUE`.")
    }

    if (is.null(total_timeout)) {
      active_workers <- min(workers, length(batch_requests))
      total_timeout <- length(batch_requests) * time_out / active_workers
    }

    # Submit all missing monthly requests at once. Existing files were already
    # removed from `batch_requests` above unless overwrite = TRUE.
    message(
      "Downloading ",
      length(batch_requests),
      " ERA5 monthly file(s) with wf_request_batch()."
    )
    ecmwfr::wf_request_batch(
      request_list = batch_requests,
      workers = workers,
      user = user,
      path = output_dir,
      time_out = time_out,
      retry = retry,
      total_timeout = total_timeout
    )

    for (i in batch_download_indexes) {
      downloads[[i]]$status <- "downloaded"
    }
  }

  # Return a small manifest of the files that were downloaded or skipped.
  do.call(rbind, downloads)
}





extract_era5_nc_files <- function(
  nc_files,
  shapefile,
  output_dir = file.path(getwd(), "Data", "era5_files", "processed"),
  id_cols = c("GEOID", "NAME"),
  overwrite = FALSE
) {
  # Use the download manifest directly, or use a vector of NetCDF file paths.
  if (is.data.frame(nc_files) && "file_path" %in% names(nc_files)) {
    nc_files <- nc_files$file_path
  }
  nc_files <- stats::na.omit(nc_files)

  # Read the shapefile, or use it directly if an sf object was supplied.
  shape <- if (inherits(shapefile, "sf")) {
    shapefile
  } else {
    sf::st_read(shapefile, quiet = TRUE)
  }

  # Keep only the ID columns that are present in the shapefile.
  id_cols <- intersect(id_cols, names(shape))
  ids <- sf::st_drop_geometry(shape)[, id_cols, drop = FALSE]

  # Save extracted RDS files in the requested folder.
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

  outputs <- vector("list", length(nc_files))

  for (i in seq_along(nc_files)) {
    nc_file <- nc_files[[i]]
    output_file <- file.path(
      output_dir,
      paste0(tools::file_path_sans_ext(basename(nc_file)), ".rds")
    )

    # Skip files that have already been extracted unless overwrite = TRUE.
    if (file.exists(output_file) && !overwrite) {
      outputs[[i]] <- data.frame(
        nc_file = nc_file,
        rds_file = output_file,
        status = "skipped_existing",
        stringsAsFactors = FALSE
      )
      next
    }

    # Read all NetCDF subdatasets and extract polygon means for each layer.
    datasets <- terra::sds(nc_file)
    extracted_parts <- vector("list", length(datasets))

    for (j in seq_along(datasets)) {
      raster_data <- datasets[[j]]
      variable_name <- names(datasets)[[j]]
      if (is.na(variable_name) || variable_name == "") {
        variable_name <- paste0("variable_", j)
      }

      # Use readable layer names when the NetCDF contains time metadata.
      raster_time <- terra::time(raster_data)
      if (length(raster_time) == terra::nlyr(raster_data) && !all(is.na(raster_time))) {
        names(raster_data) <- paste0(
          variable_name,
          "_",
          format(as.POSIXct(raster_time, tz = "UTC"), "%Y%m%d_%H%M")
        )
      } else if (terra::nlyr(raster_data) == 1) {
        names(raster_data) <- variable_name
      } else {
        names(raster_data) <- paste0(variable_name, "_", seq_len(terra::nlyr(raster_data)))
      }

      # Match the shapefile CRS to the raster before extraction.
      shape_for_raster <- sf::st_transform(shape, terra::crs(raster_data))
      cell_area <- terra::cellSize(raster_data[[1]], unit = "m")

      extracted_values <- exactextractr::exact_extract(
        x = raster_data,
        y = shape_for_raster,
        fun = "weighted_mean",
        weights = cell_area,
        progress = FALSE
      )
      extracted_values <- as.data.frame(extracted_values)
      names(extracted_values) <- names(raster_data)
      extracted_parts[[j]] <- extracted_values
    }

    # Combine polygon IDs with extracted values and save the result.
    extracted_data <- cbind(ids, do.call(cbind, extracted_parts))
    saveRDS(extracted_data, output_file)

    outputs[[i]] <- data.frame(
      nc_file = nc_file,
      rds_file = output_file,
      status = "extracted",
      stringsAsFactors = FALSE
    )
  }

  # Return a small manifest of the files that were extracted or skipped.
  do.call(rbind, outputs)
}

summarise_era5_daily_precip <- function(
  extracted_files,
  start_date = NULL,
  output_file = NULL,
  id_cols = c("GEOID", "NAME"),
  precip_prefix = "tp",
  convert_to_mm = TRUE
) {
  # Accept the extraction manifest directly, or a vector of extracted RDS files.
  if (is.data.frame(extracted_files) && "rds_file" %in% names(extracted_files)) {
    extracted_files <- extracted_files$rds_file
  }

  # A single already-loaded data frame is also allowed, but then we need a date.
  input_is_data <- is.data.frame(extracted_files)
  if (input_is_data && is.null(start_date)) {
    stop("Please provide `start_date` when `extracted_files` is already a data frame.")
  }

  if (!input_is_data) {
    extracted_files <- stats::na.omit(extracted_files)
  }

  start_dates <- NULL
  if (!is.null(start_date)) {
    start_dates <- as.Date(start_date)
    if (any(is.na(start_dates))) {
      stop("`start_date` must be coercible to Date.")
    }
    if (!input_is_data && !(length(start_dates) %in% c(1, length(extracted_files)))) {
      stop("For multiple files, provide either one `start_date` or one per file.")
    }
  }

  infer_start_date <- function(file_path) {
    # Files produced by download_era5_files() contain YYYY_MM in their name.
    file_month <- regmatches(
      basename(file_path),
      regexpr("[0-9]{4}_[0-9]{2}", basename(file_path))
    )

    if (length(file_month) == 0 || file_month == "") {
      stop("Could not infer the month from file name: ", file_path)
    }

    as.Date(paste0(gsub("_", "-", file_month), "-01"))
  }

  summarise_one_file <- function(data, file_path = NULL, file_start_date = NULL) {
    precip_cols <- grep(paste0("^", precip_prefix, "_"), names(data), value = TRUE)
    if (length(precip_cols) == 0) {
      stop("No precipitation columns starting with `", precip_prefix, "_` were found.")
    }

    # Keep tp_1, tp_2, ..., tp_24 in numeric order if that naming scheme is used.
    hour_index <- suppressWarnings(as.integer(sub(paste0("^", precip_prefix, "_"), "", precip_cols)))
    if (!all(is.na(hour_index))) {
      precip_cols <- precip_cols[order(hour_index)]
    }

    if (is.null(file_start_date)) {
      file_start_date <- infer_start_date(file_path)
    }

    # The extracted ERA5 total_precipitation values are hourly metres of water.
    # Multiplying by 1000 gives millimetres, which is easier to interpret.
    precip_values <- as.matrix(data[, precip_cols, drop = FALSE])
    if (convert_to_mm) {
      precip_values <- precip_values * 1000
    }

    hourly_times <- seq.POSIXt(
      from = as.POSIXct(file_start_date, tz = "UTC"),
      by = "hour",
      length.out = length(precip_cols)
    )
    hourly_dates <- as.Date(hourly_times)
    dates <- sort(unique(hourly_dates))

    id_cols_present <- intersect(id_cols, names(data))
    ids <- data[, id_cols_present, drop = FALSE]

    daily_parts <- vector("list", length(dates))
    unit_suffix <- if (convert_to_mm) "mm" else "m"

    for (i in seq_along(dates)) {
      day_cols <- hourly_dates == dates[[i]]
      day_values <- precip_values[, day_cols, drop = FALSE]
      valid_hours <- rowSums(!is.na(day_values))

      daily_total <- rowSums(day_values, na.rm = TRUE)
      daily_mean <- rowMeans(day_values, na.rm = TRUE)
      daily_max <- apply(day_values, 1, function(x) {
        if (all(is.na(x))) NA_real_ else max(x, na.rm = TRUE)
      })

      # If a county has no valid hourly values that day, keep all summaries NA.
      daily_total[valid_hours == 0] <- NA_real_
      daily_mean[valid_hours == 0] <- NA_real_

      daily_parts[[i]] <- data.frame(
        ids,
        date = dates[[i]],
        year = as.integer(format(dates[[i]], "%Y")),
        month = as.integer(format(dates[[i]], "%m")),
        day = as.integer(format(dates[[i]], "%d")),
        n_hours = valid_hours,
        stringsAsFactors = FALSE
      )
      daily_parts[[i]][[paste0("precip_total_", unit_suffix)]] <- daily_total
      daily_parts[[i]][[paste0("precip_max_hourly_", unit_suffix)]] <- daily_max
      daily_parts[[i]][[paste0("precip_mean_hourly_", unit_suffix)]] <- daily_mean
    }

    do.call(rbind, daily_parts)
  }

  if (input_is_data) {
    daily_precip <- summarise_one_file(
      data = extracted_files,
      file_start_date = start_dates[[1]]
    )
  } else {
    daily_precip <- do.call(
      rbind,
      lapply(seq_along(extracted_files), function(i) {
        file_start_date <- if (is.null(start_dates)) {
          NULL
        } else if (length(start_dates) == 1) {
          start_dates[[1]]
        } else {
          start_dates[[i]]
        }

        summarise_one_file(
          data = readRDS(extracted_files[[i]]),
          file_path = extracted_files[[i]],
          file_start_date = file_start_date
        )
      })
    )
  }

  if ("GEOID" %in% names(daily_precip)) {
    daily_precip <- daily_precip[order(daily_precip$GEOID, daily_precip$date), ]
  } else {
    daily_precip <- daily_precip[order(daily_precip$date), ]
  }
  rownames(daily_precip) <- NULL

  # Optionally save the final county-day panel for reuse.
  if (!is.null(output_file)) {
    dir.create(dirname(output_file), recursive = TRUE, showWarnings = FALSE)
    if (tolower(tools::file_ext(output_file)) == "csv") {
      utils::write.csv(daily_precip, output_file, row.names = FALSE)
    } else {
      saveRDS(daily_precip, output_file)
    }
  }

  daily_precip
}
