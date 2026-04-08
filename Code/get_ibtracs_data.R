####### Download and extrac IBTRACS data ########

get_ibtracs_data <- function(output_type = "base", sid_input = NULL, area = "NA", wind_var = "usa_wind", main_only = TRUE, keep_meta = FALSE) {
    
  dest_dir <- file.path(getwd(), "Data", "ibtracs_files")
  dest_file <- file.path(dest_dir, paste0("IBTrACS.", area, ".v04r01.nc"))

  if (!file.exists(dest_file)) {
    source_url <- paste0("https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/v04r01/access/netcdf/IBTrACS.", area, ".v04r01.nc")
    curl::curl_download(source_url, destfile = dest_file, quiet = FALSE)
  }

  ## Some error handling ##
  # In case downloading didn't work
  stopifnot(file.exists(dest_file))

  ## Dependencies ##
  pkgs <- c("tidync", "dplyr", "lubridate", "rlang")
  # checks for missing packages and returns the ones that are missing
  miss <- pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly = FALSE)]
  # if there are missing packages execution is stopped and the missing packages are returned
  if (length(miss) > 0) {
    stop(
      "Missing packages: ", 
      paste(miss, collapse = ", "))
  }

  #### Extract data from nc ####
  core_df <- tidync(dest_file) %>%
  activate("D2,D0") %>%
  hyper_tibble(select_var = c("time","lat","lon", wind_var))

  sid_df <- tidync(dest_file) %>%
  activate("D1,D0") %>%
  hyper_tibble(select_var = c("sid"))

  track_type_df <- tidync(dest_file) %>%
  activate("D5,D0") %>%
  hyper_tibble(select_var = c("track_type"))

  iso_df <- tidync(dest_file) %>%
  activate("D5,D2,D0") %>%
  hyper_tibble(select_var = c("iso_time"))

  name_df <- tidync(dest_file) %>%
  activate("D4,D0") %>%
  hyper_tibble(select_var = c("name"))

  dat <- core_df %>%
    left_join(sid_df, by = "storm") %>%
    left_join(track_type_df, by = "storm") %>%
    left_join(iso_df, by = c("storm", "date_time")) %>%
    left_join(name_df, by = "storm")

  #### Filter for specified sid and MAIN and do some sanity checks ####
  
  if (!is.null(sid_input) && length(sid_input) > 0) {
    sid_input <- as.character(sid_input)
    sid_input <- sid_input[nzchar(trimws(sid_input))]   # drop "" / "   "

    if (length(sid_input) > 0) {
      dat <- dplyr::filter(dat, sid %in% sid_input)

      if (nrow(dat) == 0) {
        stop("No rows found for the provided SID(s): ", paste(sid_input, collapse = ", "), 
             ". Check if the storm with the specified SID falls in the specified area. Available areas are EP for East-Pacific, NA for North-Atlantic, NI for Northern Indian Ocean, SA for South-Atlantic, SI for Southern Indian Ocean, SP for South-Pacific and WP for West-Pacific")
      }
    }
  }

  if (isTRUE(main_only)) {
    dat <- filter(dat, track_type == "main")
  }

  #### If desired prepare data for input into graph functions or stormwindmodel ####
  if (output_type == "visualization") {
    dat_vis <- dat %>%
      filter(!is.na(.data$lat), !is.na(.data$lon), !is.na(.data[[wind_var]])) %>%
      mutate(
        iso_time = as.POSIXct(.data$iso_time, tz = "UTC"),
        wind = .data[[wind_var]]
      ) %>%
      arrange(.data$iso_time) %>%
      rename(latitude = lat, longitude = lon)

    if (!isTRUE(keep_meta)) {
      dat_vis <- dat_vis %>% select(iso_time, latitude, longitude, wind)
    }
    
    return(dat_vis)
  } else if (output_type == "stormwindmodel") {
    dat_swm <- dat %>%
      # Filter out NAs
      filter(!is.na(.data$lat), !is.na(.data$lon), !is.na(.data[[wind_var]])) %>%
      mutate(
        iso_time = ymd_hms(.data$iso_time, tz = "UTC", quiet = TRUE),
        date = format(.data$iso_time, "%Y%m%d%H%M"),
        wind = .data[[wind_var]]
      ) %>%
      arrange(.data$iso_time) %>%
      rename(latitude = lat, longitude = lon) 

    if (!isTRUE(keep_meta)) {
      dat_swm <- dat_swm %>% select(date, latitude, longitude, wind)
    }

    return(dat_swm)

  } else if (output_type == "base") { 
      return(dat) 
  } else {
     stop("Please provide a viable output type. Choose between base, visualization and stormwindmodel")
  }

}

######## Some visualizations ########

## Visualize ernesto ##

make_storm_plot <- function(ibtracs_df, animate = FALSE) {
  # Plot focused on puerto rico
  pr_plot <- ggplot() +
    geom_sf(data = tracts_pr, 
            fill = NA, 
            color = "grey60", 
            linewidth = 0.2) +
    geom_point(data = ibtracs_df,
               aes(x = longitude, y = latitude, color = wind),
               size = 2) +
    scale_color_viridis_c() +
    coord_sf(xlim = c(-70, -62), ylim = c(17, 20)) +
    theme_minimal()

    print(pr_plot)

  # Animated plot focused on puerto rico
  if (animate == TRUE) {
    pr_plot_animated <- ggplot() +
      geom_sf(data = tracts_pr, 
              fill = NA, 
              color = "grey60", 
              linewidth = 0.2) +
      geom_point(data = ibtracs_df,
                 aes(x = longitude, y = latitude, color = wind),
                 size = 2) +
      scale_color_viridis_c() +
      coord_sf(xlim = c(-70, -62), ylim = c(17, 20)) +
      theme_minimal() +
      transition_time(iso_time)
    
    anim_save("Output/pr_storm.gif", animation = animate(pr_plot_animated))
  }

  # Plot showing americas
  sa_plot <- ggplot(ibtracs_df, aes(longitude, latitude, color = wind)) +
    annotation_borders("world", colour = "gray70", fill = "gray90") +
    geom_point(size = 2) +
    scale_color_viridis_c() +
    coord_sf(xlim = c(-140, -30), ylim = c(0, 75))

  print(sa_plot)

}

######## Get storm measurements ########

#### Function that extracts max windspeeds for exactly one point per tract - the centroid

tc_daily_panel_centroids <- function(
  storm_list,
  tracts,
  tract_id = "GEOID",
  start_date = NULL,
  end_date = NULL,
  tz = "America/Puerto_Rico",
  tempRes = 60,
  method = "Willoughby",
  asymmetry = "Chen",
  verbose = 0,
  fill_zeros = TRUE
  ){

  ## Do some input checks

  # Check storms List 
  if (!inherits(storm_list, "stormsList")) {
    stop(
      "`storm_list` must be a StormR stormsList object (output of defStormsList()). ",
      "Current class: ", paste(class(storm_list), collapse = ", ")
    )
  }

  # Check tracts data
  tracts <- tracts %>%
    st_make_valid() %>%
    filter(!st_is_empty(geometry))
  if (nrow(tracts) == 0) {
    stop("`tracts` has no valid non-empty geometries after cleaning.")
  }

  # Fix weird bug 
  # StormR temporal interpolation fails for storms with <2 observations
  # inside the area of interest (e.g., lenData = 1 in stormDisplacement).
  keep_storm <- vapply(storm_list@data, function(st) {
    length(st@obs) >= 2
  }, logical(1))
  if (!all(keep_storm)) {
    dropped <- names(storm_list@data)[!keep_storm]
    warning(
      "Dropping storms with fewer than 2 observations in the AOI: ",
      paste(dropped, collapse = ", ")
    )
    storm_list@data <- storm_list@data[keep_storm]
  }
  if (length(storm_list@data) == 0) {
    stop("No storms with at least 2 observations are available for temporal modelling.")
  }

  ## Prepare the centroids from the tracts data ##
  tracts_v <- terra::vect(tracts)

  if(!(tract_id %in% names(tracts_v))) {
    stop(sprintf( "Column '%s' not found in the provided tracts data.", tract_id))
  }

  # Prepare the points we are interested in 
  # In this case these are the centroids of each tract
  # Option 1:
  # centroids <- terra::centroids(tracts_v)
  # coords <- terra::crds(centroids)

  # Option 2:
  tracts_sf <- st_as_sf(tracts)

  pts_sf <- tracts_sf %>%
    # projected CRS for PR
    st_transform(32161) %>%  
    # safer than terra::centroids for polygons    
    st_point_on_surface() %>%    
    st_transform(4326)

  coords <- st_coordinates(pts_sf)

  tracts_points <- data.frame (
    x = coords[, 1],
    y = coords[, 2]
  )

  # Attach GEOID to point coordinates (centroids)
  rownames(tracts_points) <- as.character(terra::values(tracts_v)[[tract_id]])

  # Function to safely extract temporal data for each point
  # This function will return a wind speed / direction time series and summary statistics 
  do_storm_temp_chunked <- function(
    storm_list,
    points,
    chunk_size = 1,
    empiricalRMW = FALSE,
    tempRes = 60,
    method = "Willoughby",
    asymmetry = "Chen",
    verbose = 0
  ) {
    # Need to split the points we are interested in into chunks because of some weird bug with temoporalBehaviour
    # Split the indices into groups of 25 indices (or whatever we set the chunk_size at)
    idx <- split(
      # Basically simulate the indices of points (is a sequence anyway so don't need to extract)
      seq_len(nrow(points)),
      # "Assigns" each index a group number
      ceiling(seq_len(nrow(points)) / chunk_size))

    # Run temporalBehaviour for each chunk
    chunk_results <- lapply(idx, function(ii) {
      # Extract chunk of points from whole points dataset
      pts_chunk <- points[ii, , drop = FALSE]

      temporalBehaviour(
        sts = storm_list,
        points = pts_chunk,
        product = "TS",
        empiricalRMW = empiricalRMW,
        tempRes = tempRes,
        method = method,
        asymmetry = asymmetry,
        verbose = verbose
      )
    })

    # Merge chunks into a final list
    merged <- list()

    # For each chunk
    for (res in chunk_results) {
      # For each storm in each chunk
      for (storm_name in names(res)) {
        # check if there is actually data on the storm
        if (is.null(merged[[storm_name]])) {
          # If not leave empty
          merged[[storm_name]] <- list()
        }
        merged[[storm_name]] <- c(merged[[storm_name]], res[[storm_name]])
      }
    }

  return(merged)

  }

  temporalBehaviour_result <- do_storm_temp_chunked(
    storm_list = storm_list,
    points = tracts_points,
    tempRes = tempRes,
    method = method,
    asymmetry = asymmetry,
    verbose = verbose
  )

  # build quick tract ID lookup
  id_lookup <- data.frame(
    ID = seq_len(nrow(tracts_v)),
    tract_value = tracts_v[[tract_id]]
  )
  names(id_lookup)[2] <- tract_id

  ts_long <- imap_dfr(temporalBehaviour_result, function(storm_obj, storm_name) {
    imap_dfr(storm_obj, function(df_point, point_name) {
      tibble(
        storm = storm_name,
        GEOID = point_name,
        time_utc = as.POSIXct(df_point$isoTimes, tz = "UTC"),
        tc_wind = as.numeric(df_point$speed)
      )
    })
  })

  out <- ts_long %>%
    mutate(
      date = as.Date(format(time_utc, tz = "America/Puerto_Rico", usetz = FALSE))
    ) %>%
    group_by(GEOID, date) %>%
    summarise(

    # insert mean calculation here
      tc_max_wind = if (all(is.na(tc_wind))) 0 else max(tc_wind, na.rm = TRUE),
      .groups = "drop"
    )

  if (fill_zeros) {
    if (is.null(start_date)) start_date <- min(out$date, na.rm = TRUE)
    if (is.null(end_date)) end_date <- max(out$date, na.rm = TRUE)

    full_dates <- seq.Date(as.Date(start_date), as.Date(end_date), by = "day")

    full_panel <- tidyr::expand_grid(
      !!tract_id := unique(id_lookup[[tract_id]]),
      date = full_dates
    )

    out <- full_panel %>%
      left_join(out, by = c(tract_id, "date")) %>%
      mutate(
        tc_max_wind = dplyr::coalesce(tc_max_wind, 0),
        tc_day = as.integer(tc_max_wind > 0)
      ) %>%
      arrange(.data[[tract_id]], date)
  }

  return(out)

}

#### Function that calculates max and mean wind speeds across tract polygons

tc_daily_panel_from_tracts <- function(
  storm_list,
  tracts,
  tract_id = "GEOID",
  start_date = NULL,
  end_date   = NULL,
  tz = "America/Puerto_Rico",
  tempRes = 60,
  tract_stat = c("max", "mean"),
  method = "Willoughby",
  asymmetry = "Chen",
  verbose = 0,
  fill_zeros = TRUE
  ) {

  ## Do some input checks

  # Check storms List 
  if (!inherits(storm_list, "stormsList")) {
    stop(
      "storm_list must be a StormR stormsList object (output of defStormsList()). ",
      "Current class: ", paste(class(storm_list), collapse = ", ")
    )
  }

  # Check tracts data
  tracts <- tracts %>%
    st_make_valid() %>%
    filter(!st_is_empty(geometry))
  if (nrow(tracts) == 0) {
    stop("tracts has no valid non-empty geometries after cleaning.")
  }

  # Fix weird bug 
  # StormR temporal interpolation fails for storms with <2 observations
  # inside the area of interest (e.g., lenData = 1 in stormDisplacement).
  keep_storm <- vapply(storm_list@data, function(st) {
    length(st@obs) >= 2
  }, logical(1))
  if (!all(keep_storm)) {
    dropped <- names(storm_list@data)[!keep_storm]
    warning(
      "Dropping storms with fewer than 2 observations in the AOI: ",
      paste(dropped, collapse = ", ")
    )
    storm_list@data <- storm_list@data[keep_storm]
  }
  if (length(storm_list@data) == 0) {
    stop("No storms with at least 2 observations are available for temporal modelling.")
  }

  # Kept for backward compatibility with older calls; both stats are computed.
  tract_stat <- match.arg(tract_stat)

  # convert polygons to terra vector if needed
  tracts_v <- terra::vect(tracts)

  if(!(tract_id %in% names(tracts_v))) {
    stop(sprintf( "Column '%s' not found in the provided tracts data.", tract_id))
  }

  # build quick tract ID lookup
  id_lookup <- data.frame(
    ID = seq_len(nrow(tracts_v)),
    tract_value = tracts_v[[tract_id]]
  )
  names(id_lookup)[2] <- tract_id

  start_date_in <- if (is.null(start_date)) NULL else as.Date(start_date)
  end_date_in <- if (is.null(end_date)) NULL else as.Date(end_date)
  storm_verbose <- if (verbose > 1) verbose - 1 else 0
  # Small batches avoid building one massive StormR raster object for the
  # entire AOI/time span, which is what caused the crash on the full panel run.
  storm_chunk_size <- 5L

  build_daily_raster <- function(storm_subset) {
    pf <- spatialBehaviour(
      storm_subset,
      product = "Profiles",
      method = method,
      asymmetry = asymmetry,
      tempRes = tempRes,
      verbose = storm_verbose
    )

    speed_idx <- grep("_Speed_", names(pf))
    if (length(speed_idx) == 0) {
      stop("No speed layers found in the StormR output.")
    }
    wind_speed <- pf[[speed_idx]]

    times_utc <- terra::time(wind_speed)
    if (length(times_utc) == 0 || all(is.na(times_utc))) {
      stop("No timestamps found on the speed layers. Check `terra::time(wind_speed)`.")
    }

    day_local <- as.Date(
      format(as.POSIXct(times_utc, tz = "UTC"), tz = tz, usetz = FALSE)
    )

    daily_dates <- sort(unique(day_local))
    keep_dates <- rep(TRUE, length(daily_dates))
    if (!is.null(start_date_in)) keep_dates <- keep_dates & daily_dates >= start_date_in
    if (!is.null(end_date_in)) keep_dates <- keep_dates & daily_dates <= end_date_in
    if (!any(keep_dates)) {
      return(NULL)
    }

    daily_rast <- terra::tapp(
      wind_speed,
      index = day_local,
      fun = max,
      na.rm = TRUE
    )

    ext_r <- terra::ext(daily_rast)
    ext_v <- terra::ext(tracts_v)
    if (ext_r$xmin >= 0 && ext_v$xmin < 0) {
      daily_rast <- terra::rotate(daily_rast)
    }

    daily_rast <- daily_rast[[keep_dates]]
    daily_dates <- daily_dates[keep_dates]
    names(daily_rast) <- paste0("d_", format(daily_dates, "%Y_%m_%d"))

    daily_rast
  }

  merge_daily_layers <- function(existing, incoming) {
    if (is.null(existing)) {
      return(incoming)
    }
    merged <- terra::mosaic(existing, incoming, fun = "max")
    names(merged) <- names(existing)
    merged
  }

  daily_layers <- list()
  storm_idx <- seq_along(storm_list@data)
  storm_chunks <- split(
    storm_idx,
    ceiling(storm_idx / storm_chunk_size)
  )

  for (i in seq_along(storm_chunks)) {
    idx <- storm_chunks[[i]]
    storm_subset <- storm_list
    storm_subset@data <- storm_list@data[idx]

    if (verbose > 0) {
      message(
        sprintf(
          "tc_daily_panel_from_tracts: processing chunk %d/%d (storms %d-%d)",
          i,
          length(storm_chunks),
          idx[[1]],
          idx[[length(idx)]]
        )
      )
    }

    storm_daily <- build_daily_raster(storm_subset)
    if (is.null(storm_daily)) {
      next
    }

    for (j in seq_len(terra::nlyr(storm_daily))) {
      layer_name <- names(storm_daily)[j]
      daily_layers[[layer_name]] <- merge_daily_layers(
        daily_layers[[layer_name]],
        storm_daily[[j]]
      )
    }

    gc(verbose = FALSE)
  }

  day_names <- sort(names(daily_layers))
  out_list <- vector("list", length(day_names))

  for (i in seq_along(day_names)) {
    layer_name <- day_names[[i]]
    date_value <- as.Date(sub("^d_", "", layer_name), format = "%Y_%m_%d")
    day_rast <- daily_layers[[layer_name]]

    tract_vals_max <- terra::extract(
      day_rast,
      tracts_v,
      fun = max,
      na.rm = TRUE,
      ID = TRUE
    )
    tract_vals_mean <- terra::extract(
      day_rast,
      tracts_v,
      fun = mean,
      na.rm = TRUE,
      ID = TRUE
    )

    names(tract_vals_max)[2] <- "tc_poly_max_wind"
    names(tract_vals_mean)[2] <- "tc_poly_mean_wind"

    out_list[[i]] <- tract_vals_max %>%
      left_join(tract_vals_mean, by = "ID") %>%
      left_join(id_lookup, by = "ID") %>%
      mutate(date = date_value) %>%
      select(all_of(tract_id), date, tc_poly_max_wind, tc_poly_mean_wind)

    if (verbose > 0 && (i %% 25 == 0 || i == length(day_names))) {
      message(
        sprintf(
          "tc_daily_panel_from_tracts: extracted %d/%d storm days",
          i,
          length(day_names)
        )
      )
    }

    gc(verbose = FALSE)
  }

  if (length(out_list) == 0) {
    out <- tibble::tibble(
      !!tract_id := character(),
      date = as.Date(character()),
      tc_poly_max_wind = numeric(),
      tc_poly_mean_wind = numeric()
    )
  } else {
    out <- bind_rows(out_list) %>%
      arrange(.data[[tract_id]], date)
  }

  # Optional date window restriction even when fill_zeros = FALSE.
  if (!is.null(start_date_in)) {
    out <- out %>% filter(date >= start_date_in)
  }
  if (!is.null(end_date_in)) {
    out <- out %>% filter(date <= end_date_in)
  }

  # Build a full tract x day panel and fill non-storm days with 0
  if (fill_zeros) {
    if (is.null(start_date_in)) start_date_in <- min(out$date, na.rm = TRUE)
    if (is.null(end_date_in))   end_date_in   <- max(out$date, na.rm = TRUE)

    full_dates <- seq.Date(start_date_in, end_date_in, by = "day")

    full_panel <- tidyr::expand_grid(
      !!tract_id := unique(id_lookup[[tract_id]]),
      date = full_dates
    )
    out <- full_panel %>%
      left_join(out, by = c(tract_id, "date")) %>%
      mutate(
        tc_poly_max_wind = dplyr::coalesce(tc_poly_max_wind, 0),
        tc_poly_mean_wind = dplyr::coalesce(tc_poly_mean_wind, 0),
        tc_poly_day = as.integer(tc_poly_max_wind > 0 | tc_poly_mean_wind > 0)
      ) %>%
      arrange(.data[[tract_id]], date)
  } else {
    out <- out %>%
      mutate(
        tc_poly_day = dplyr::if_else(
          is.na(tc_poly_max_wind) & is.na(tc_poly_mean_wind),
          NA_integer_,
          as.integer(
            dplyr::coalesce(tc_poly_max_wind, 0) > 0 |
            dplyr::coalesce(tc_poly_mean_wind, 0) > 0
          )
        )
      )
  }

  return(out)
}
