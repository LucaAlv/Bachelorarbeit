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



# This takes a vector of storm names
# cleans it and returns it as one string
collapse_storm_names <- function(x) {

  # Remove NAs, convert to character, keep each storm name once,
  # and sort them so the output is stable.
  x_out <- x[!is.na(x)]
  x_out <- as.character(x_out)
  x_out <- unique(x_out)
  x_out <- sort(x_out)

  if (length(x_out) == 0) {
    return(NA_character_)
  }
  else {
    return(paste(x_out, collapse = "; "))
  }

}


#### Timezone handling 

# Set correct timezones for different states (i.e. counties)
ibtracs_timezone_from_geoid <- function(geoid) {
  state_fips <- substr(geoid, 1, 2)

  tz <- rep(NA_character_, length(geoid))
  tz[state_fips == "72"] <- "America/Puerto_Rico"
  tz[state_fips %in% c("37", "45")] <- "America/New_York"
  tz[state_fips %in% c("22", "48")] <- "America/Chicago"
  tz[state_fips == "12"] <- "America/New_York"

  fl_central_counties <- c(
    "12005", "12013", "12033", "12045", "12059",
    "12063", "12091", "12113", "12131", "12133"
  )
  tx_mountain_counties <- c("48141", "48229")

  tz[geoid %in% fl_central_counties] <- "America/Chicago"
  tz[geoid %in% tx_mountain_counties] <- "America/Denver"

  missing_tz <- unique(geoid[is.na(tz)])
  if (length(missing_tz) > 0) {
    stop(
      "`tz = \"local\"` could not infer a timezone for GEOID(s): ",
      paste(head(missing_tz, 10), collapse = ", ")
    )
  }

  tz
}

# This small helper function returns the appropriate timezone
# for different kinds of inputs
# returns a nice GEOID / timezone df
resolve_ibtracs_timezones <- function(tract_ids, tz, tract_id = "GEOID") {

  if (length(tz) == 1 && tz == "local") {
    # this is the standard case - just call the function from above that assigns the appropriate tzs
    tz_values <- ibtracs_timezone_from_geoid(tract_ids)

    # one specific timezone is specified - the just repeat that one for all counties
  } else if (length(tz) == 1 && tz != "local") {
    tz_values <- rep(as.character(tz), length(tract_ids))
  }

  # Check for bad timezone values - 
  bad_tz <- setdiff(unique(tz_values), OlsonNames())
  if (length(bad_tz) > 0) {
    stop("Unknown timezones: ", paste(bad_tz, collapse = ", "))
  }

  out <- data.frame(
    tract_value = tract_ids,
    tc_timezone = tz_values,
    stringsAsFactors = FALSE
  )

  names(out)[1] <- tract_id
  
  return(out)
}

# Helper function to convert utc timestamps into corresponding local calendar date
ibtracs_local_date <- function(time_utc, tz) {
  time_utc <- as.POSIXct(time_utc, tz = "UTC")
  if (length(tz) == 1) {
    tz <- rep(tz, length(time_utc))
  }
  if (length(time_utc) != length(tz)) {
    stop("`time_utc` and `tz` must have the same length.")
  }

  out <- as.Date(rep(NA_character_, length(time_utc)))
  for (tz_value in unique(tz)) {
    idx <- tz == tz_value
    out[idx] <- as.Date(format(time_utc[idx], tz = tz_value, usetz = FALSE))
  }
  out
}









# small helper function to turn the terra vector into 
# character strings
ibtracs_tract_id_values <- function(tracts_v, tract_id) {
  tract_values <- terra::values(tracts_v)[[tract_id]]
  tract_values <- as.character(tract_values)

  if (length(tract_values) != nrow(tracts_v)) {
    stop(
      "Could not extract one `", tract_id, "` value per tract. ",
      "Check the ID column in `tracts`."
    )
  }
  if (any(is.na(tract_values)) || any(!nzchar(tract_values))) {
    stop("`", tract_id, "` contains missing or blank values.")
  }

  tract_values
}


# build local LAEA projected CRS from the input geometry
# is less annoying than choosing the right one for each state
# and still better than just using WGS84
ibtracs_input_laea_crs <- function(geom) {
  if (is.na(st_crs(geom))) {
    stop("`tracts` must have a CRS before representative points can be computed.")
  }

  # First transform into WGS 84 as geodetic CRS
  geom_ll <- st_transform(geom, 4326)
  # Put box around 
  bbox <- st_bbox(geom_ll)

  if (any(!is.finite(as.numeric(bbox)))) {
    stop("`tracts` has an invalid bounding box; representative points cannot be computed.")
  }

  # Calculate approximate center
  lon_0 <- mean(c(bbox[["xmin"]], bbox[["xmax"]]))
  lat_0 <- mean(c(bbox[["ymin"]], bbox[["ymax"]]))

  # Build function call for st_transform
  # Note: maybe improve this / make more elegant later
  sprintf(
    "+proj=laea +lat_0=%.10f +lon_0=%.10f +datum=WGS84 +units=m +no_defs",
    lat_0,
    lon_0
  )
}





#### Function that extracts max windspeeds for exactly one point per tract - the centroid
#### Note the confusing wording here. Originally I wrote this function to work on census tracts
#### The function however does not care about the name of the geometric shapes it works with
#### So it also works completely fine for counties and other geometric shapes derived from geographic entities
#### (not only on census tracts)
tc_daily_panel_centroids <- function(
  storm_list,
  tracts,
  tract_id = "GEOID",
  start_date = NULL,
  end_date = NULL,
  tz = "local",
  tempRes = 60,
  method = "Willoughby",
  asymmetry = "Chen",
  empiricalRMW = TRUE,
  verbose = 2,
  fill_zeros = TRUE
  ){




  ## Do some input checks

  # Check storms List is a StormR stormsList object
  # Intuition rememberer:
  # stormsList is a list of storms
  # for every storm it contains its path
  # path is identified by iso.time, lon, lat, 
  # for every time step we have msw, scale, rmw, pres, poci
  if (!inherits(storm_list, "stormsList")) {
    stop(
      "`storm_list` must be a StormR stormsList object (output of defStormsList()). ",
      "Current class: ", paste(class(storm_list), collapse = ", ")
    )
  }

  # Check input data
  # Intuition rememberer: 
  # This is a df with GEOID, Name and geometry of all relevant counties
  tracts <- tracts %>% st_make_valid()
  tracts <- tracts[!st_is_empty(st_geometry(tracts)), ]
  if (nrow(tracts) == 0) {
    stop("geom data has no valid non-empty geometries after cleaning.")
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








  # convert simple feature collection to terra spatial vector
  tracts_v <- terra::vect(tracts)


  ## Extract centroid
  # Convert to sf object
  tracts_sf <- st_as_sf(tracts)
  # Make sure that 
  point_crs <- ibtracs_input_laea_crs(tracts_sf)

  pts_sf <- st_geometry(tracts_sf) %>%
    # GEOS point-on-surface is planar, so use a projection centered on this input.
    st_transform(point_crs) %>%
    # safer than terra::centroids for polygons 
    # because for coastal counties it makes sure that the point
    # doesn't accidentally lie outside the county   
    st_point_on_surface() %>%    
    st_transform(4326)

  coords <- st_coordinates(pts_sf)

  tracts_points <- data.frame (
    x = coords[, 1],
    y = coords[, 2]
  )

  tract_values <- ibtracs_tract_id_values(tracts_v, tract_id)

  # Attach GEOID to point coordinates (centroids)
  rownames(tracts_points) <- tract_values

  # build quick tract ID lookup
  id_lookup <- data.frame(
    ID = seq_len(nrow(tracts_v)),
    tract_value = tract_values,
    stringsAsFactors = FALSE
  )

  names(id_lookup)[2] <- tract_id

  # Set appropriate timezone for every county
  timezone_lookup <- resolve_ibtracs_timezones(id_lookup[[tract_id]], tz, tract_id)














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
    verbose = 2
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
        # RMW == radius of maximum wind - the distance from a tropical cyclones center to its strongest winds
        # typically located
        empiricalRMW = empiricalRMW,
        tempRes = tempRes,
        method = method,
        asymmetry = asymmetry,
        verbose = verbose
      )
    })





    # Merge chunks into a final list
    merged <- vector("list", length(storm_list@data))
    names(merged) <- names(storm_list@data)

    # For each chunk
    for (res in chunk_results) {
      # Merge by position rather than name so duplicate storm names do not
      # collapse distinct storms into the same list entry.
      for (storm_idx in seq_along(res)) {
        if (is.null(merged[[storm_idx]])) {
          merged[[storm_idx]] <- list()
        }
        merged[[storm_idx]] <- c(merged[[storm_idx]], res[[storm_idx]])
      }
    }

    return(merged)

  }





  out_path <- paste0("Data/ibtracs_files/raw/tc_", start_date, "_", end_date, ".rds")

  if (!exists(file.path(out_path))) {

    # Get the storm estimates
    # This returns a nested list
    # summary(temporalBehaviour_result[[1]][["48505"]])
    temporalBehaviour_result <- do_storm_temp_chunked(
      storm_list = storm_list,
      points = tracts_points,
      empiricalRMW = empiricalRMW,
      tempRes = tempRes,
      method = method,
      asymmetry = asymmetry,
      verbose = verbose
    )

    saveRDS(temporalBehaviour_result, out_path)

  } else {
    temporalBehaviour_result <- readRDS(out_path)
  }









  ## Iterate through the storm dataset 

  ts_rows <- list()

  # For every storm
  for (storm_idx in seq_along(temporalBehaviour_result)) {
    storm_name <- names(temporalBehaviour_result)[storm_idx]
    storm_obj <- temporalBehaviour_result[[storm_idx]]

    # For every point / county inside that storm
    for (point_idx in seq_along(storm_obj)) {
      point_name <- names(storm_obj)[point_idx] # county fips code
      df_point <- storm_obj[[point_idx]]

      # For every measured point for the current storm in the current county
      point_ts <- tibble(
        storm = storm_name,
        GEOID = point_name,
        time_utc = as.POSIXct(df_point$isoTimes, tz = "UTC"),
        tc_wind = as.numeric(df_point$speed)
      )

      # Add to list
      ts_rows[[length(ts_rows) + 1]] <- point_ts

    }
  }

  # this is a large tibble with one row for every storm x GEOID x time (in utc)
  # and information on the windspeed foe each point
  ts_long <- bind_rows(ts_rows)

  # Add time information
  # Convert to daily
  out <- ts_long %>%
    left_join(timezone_lookup, by = "GEOID") %>%
    # Convert utc timestamp into corresponding local calendar date
    mutate(
      date = ibtracs_local_date(time_utc, tc_timezone)
    ) %>%
    group_by(GEOID, date) %>%
    summarise(
      storm_name = collapse_storm_names(storm[!is.na(tc_wind) & tc_wind > 0]),
      tc_max_wind = if (all(is.na(tc_wind))) 0 else max(tc_wind, na.rm = TRUE),
      tc_mean_wind = if (all(is.na(tc_wind))) 0 else mean(tc_wind, na.rm = TRUE),
      .groups = "drop"
    )

    # If no start or end date is specified
    if (is.null(start_date)) start_date <- min(out$date, na.rm = TRUE)
    if (is.null(end_date)) end_date <- max(out$date, na.rm = TRUE)

    full_dates <- seq.Date(as.Date(start_date), as.Date(end_date), by = "day")

    # Make panel for every day x county combination
    full_panel <- tidyr::expand_grid(
      !!tract_id := unique(id_lookup[[tract_id]]),
      date = full_dates
    )

  # Any days without data (i.e. all days where no storm was happening)
  # Put in zeroes for max wind speed and indicator if it was a storm that day
  if (fill_zeros) {
    out_final <- full_panel %>%
      left_join(out, by = c("GEOID", "date")) %>%
      mutate(
        # fill NAs with zeroes
        tc_max_wind = dplyr::coalesce(tc_max_wind, 0),
        tc_day = as.integer(tc_max_wind > 0)
      ) %>%
      arrange(GEOID, date)
  } else {
    out_final <- full_panel %>%
      left_join(out, by = c("GEOID", "date")) %>%
      mutate(
        tc_day = dplyr::if_else(
          is.na(tc_max_wind),
          0,
          as.integer(
            coalesce(tc_max_wind, 0) > 0,
            coalesce(tc_mean_wind, 0) > 0
          )
        )
      ) %>%
      arrange(storm_name, date, GEOID)
  }

  # This function returns a dataframe with rows for every combination of GEOID and date
  # and information on the storm name, max windspeed and indicator if a given day saw a storm
  return(out_final)

}















#### Function that calculates max and mean wind speeds across county polygons

tc_daily_panel_polygons <- function(
  storm_list,
  tracts,
  tract_id = "GEOID",
  start_date = NULL,
  end_date   = NULL,
  tz = "local",
  tempRes = 60,
  method = "Willoughby",
  asymmetry = "Chen",
  empiricalRMW = TRUE,
  verbose = 0,
  fill_zeros = TRUE
  ) {

  ## Do some input checks

  # Check storms List is a StormR stormsList object
  # Intuition rememberer:
  # stormsList is a list of storms
  # for every storm it contains its path
  # path is identified by iso.time, lon, lat, 
  # for every time step we have msw, scale, rmw, pres, poci
  if (!inherits(storm_list, "stormsList")) {
    stop(
      "storm_list must be a StormR stormsList object (output of defStormsList()). ",
      "Current class: ", paste(class(storm_list), collapse = ", ")
    )
  }

  # Check input data
  # Intuition rememberer: 
  # This is a df with GEOID, Name and geometry of all relevant counties
  tracts <- tracts %>% st_make_valid()
  tracts <- tracts[!st_is_empty(st_geometry(tracts)), ]
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





  ## Main part start
  # convert simple feature collection to terra spatial vector
  tracts_v <- terra::vect(tracts)

  # build quick tract ID lookup
  tract_values <- ibtracs_tract_id_values(tracts_v, tract_id)

  # build quick tract ID lookup
  id_lookup <- data.frame(
    ID = seq_len(nrow(tracts_v)),
    tract_value = tract_values,
    stringsAsFactors = FALSE
  )

  names(id_lookup)[2] <- tract_id

  # Set appropriate timezone for every county
  timezone_lookup <- resolve_ibtracs_timezones(id_lookup[[tract_id]], tz, tract_id)

  tract_timezones <- timezone_lookup$tc_timezone[
    match(id_lookup[[tract_id]], timezone_lookup[[tract_id]])
  ]
  timezones <- sort(unique(tract_timezones))
  tract_rows_by_tz <- stats::setNames(
    lapply(timezones, function(tz_value) which(tract_timezones == tz_value)),
    timezones
  )

  
  #### Set up functions

  # 
  extract_context_for_tz <- function(tz_value) {

    tract_rows <- tract_rows_by_tz[[tz_value]]
    id_lookup_tz <- id_lookup[tract_rows, , drop = FALSE]
    id_lookup_tz$ID <- seq_len(nrow(id_lookup_tz))

    list(
      tracts = tracts_v[tract_rows, ],
      id_lookup = id_lookup_tz
    )
  }

  # Function to safely extract data for each polygon
  build_wind_speed_raster <- function(storm_subset) {

    # This is a spat raster object
    pf <- spatialBehaviour(
      storm_subset,
      product = "Profiles",
      method = method,
      asymmetry = asymmetry,
      empiricalRMW = empiricalRMW,
      tempRes = tempRes,
      verbose = verbose
    )


    # Speed values are indexed by e.g. EARL-2010_Speed_65.67
    speed_idx <- grep("_Speed_", names(pf))

    wind_speed <- pf[[speed_idx]]

    wind_speed
  }


  # This helper function takes a sub-daily wind speed SpatRaster object
  # and collapses it into a daily wind-speed raster 
  # while being careful to convert correctly to local calendar days
  # it returns one raster layer per local day
  # each layer then contains the maximum wind speed reached that day
  build_daily_raster <- function(wind_speed, tz_value) {

    # Extract time stamps from windspeed raster
    # Note these are UTC times
    times_utc <- terra::time(wind_speed)

    # Convert utc timestamp into corresponding local calendar date 
    day_local <- ibtracs_local_date(times_utc, tz_value)

    daily_dates <- sort(unique(day_local))

    keep_dates <- rep(TRUE, length(daily_dates))
    if (!is.null(start_date)) keep_dates <- keep_dates & daily_dates >= start_date
    if (!is.null(end_date)) keep_dates <- keep_dates & daily_dates <= end_date
    if (!any(keep_dates)) {
      return(NULL)
    }

    # terra::tapp() returns groups in factor-level order. 
    # If dates are passed directly, multi-storm chunks can come back in 
    # first-seen storm order, 
    # this mislabels later storms when assigning sorted date names
    day_index <- factor(as.character(day_local), levels = as.character(daily_dates))

    # Building the actual raster
    # For each grid cell and each local day
    # This calculates the maximum wind speed reached on that day
    daily_rast <- terra::tapp(
      wind_speed,
      index = day_index,
      fun = max,
      na.rm = TRUE
    )

    # Bug fix
    # Raster use 0...360 degrees
    # County geometries use -180...180 degrees 
    ext_r <- terra::ext(daily_rast)
    ext_v <- terra::ext(tracts_v)
    if (ext_r$xmin >= 0 && ext_v$xmin < 0) {
      daily_rast <- terra::rotate(daily_rast)
    }


    daily_rast <- daily_rast[[keep_dates]]
    daily_dates <- daily_dates[keep_dates]

    # Attach dates
    # These are still raster layers -> add d to avoid confusion
    names(daily_rast) <- paste0("d_", format(daily_dates, "%Y_%m_%d"))

    return(daily_rast)
  }

  # This helper function builds the storm_name column
  # It returns basically a lookup table
  # Where each GEOID date combination is assigned a storm
  build_storm_name_panel <- function() {

    storm_name_rows <- list()
    storm_name_row_idx <- 0L
    storm_names <- names(storm_list@data)

    # For every storm i
    for (i in seq_along(storm_names)) {
      
      # Create a storm subset with only one storm (the current one)
      storm_subset <- storm_list
      storm_subset@data <- storm_list@data[i]

      # Build the wind speed raster for the current storm
      wind_speed <- build_wind_speed_raster(storm_subset)

      # For each timezone
      for (tz_value in timezones) {
        
        # Convert from sub-daily raster into local daily max-wind rasters
        storm_daily <- build_daily_raster(wind_speed, tz_value)
        
        if (is.null(storm_daily)) {
          next
        }

        tz_context <- extract_context_for_tz(tz_value)

        storm_day_rows <- vector("list", terra::nlyr(storm_daily))

        for (j in seq_len(terra::nlyr(storm_daily))) {
          layer_name <- names(storm_daily)[j]
          date_value <- as.Date(sub("^d_", "", layer_name), format = "%Y_%m_%d")
          day_rast <- storm_daily[[j]]

          # For each local day
          # Extract the max wind over each polygon
          tract_vals_max <- terra::extract(
            day_rast,
            tz_context$tracts,
            fun = max,
            na.rm = TRUE,
            ID = TRUE
          )

          names(tract_vals_max)[2] <- "tc_poly_max_wind"

          storm_day_rows[[j]] <- tract_vals_max %>%
            filter(dplyr::coalesce(tc_poly_max_wind, 0) > 0) %>%
            left_join(tz_context$id_lookup, by = "ID") %>%
            transmute(
              !!tract_id := GEOID,
              date = date_value,
              storm_name = storm_names[[i]]
            )
        }

        storm_name_row_idx <- storm_name_row_idx + 1L
        storm_name_rows[[storm_name_row_idx]] <- bind_rows(storm_day_rows)
      }

      if (verbose > 0 && (i %% 10 == 0 || i == length(storm_names))) {
        message(
          sprintf(
            "tc_daily_panel_polygons(): identified storm names for %d/%d storms",
            i,
            length(storm_names)
          )
        )
      }

      gc(verbose = TRUE)
    }



    storm_name_panel <- bind_rows(storm_name_rows)

    if (nrow(storm_name_panel) == 0) {
      return(tibble::tibble(
        !!tract_id := id_lookup[[tract_id]][0],
        date = as.Date(character()),
        storm_name = character()
      ))
    }

    return(
      storm_name_panel %>%
        group_by(GEOID, date) %>%
        summarise(
          storm_name = paste(sort(unique(storm_name)), collapse = "; "),
          .groups = "drop"
        )
    )
  }

  merge_daily_layers <- function(existing, incoming) {
    if (is.null(existing)) {
      return(incoming)
    }
    merged <- terra::mosaic(existing, incoming, fun = "max")
    names(merged) <- names(existing)
    merged
  }






  #### Call functions


  daily_layers <- setNames(vector("list", length(timezones)), timezones)

  # Make index for every storm
  storm_idx <- seq_along(storm_list@data)

  # Split indices into chunks 
  # Small batches avoid building one massive StormR raster object for the
  # entire AOI/time span, which is what regularly caused a crash
  storm_chunk_size <- 5L
  storm_chunks <- split(
    storm_idx,
    ceiling(storm_idx / storm_chunk_size)
  )

  # Iterate over the storm chunks
  for (i in seq_along(storm_chunks)) {
    
    # Load storm indices for current chunk
    idx <- storm_chunks[[i]]

    # Create subset of storms 
    storm_subset <- storm_list
    storm_subset@data <- storm_list@data[idx]

    # Some terminal diagnostics
    if (verbose > 0) {
      message(
        sprintf(
          "tc_daily_panel_polygons: processing chunk %d/%d (storms %d-%d)",
          i,
          length(storm_chunks),
          idx[[1]],
          idx[[length(idx)]]
        )
      )
    }



    # This calculates the actual wind-speed raster
    # This is a sub-daily SpatRaster object
    # for the current chunk of storms
    # Each layer of this spatraster object is a modeled windfield
    # at some UTC timestamp
    wind_speed <- build_wind_speed_raster(storm_subset)


    # Loop through all timezones
    # collect the daily wind raster separately for each local timezone
    for (tz_value in timezones) {

      # Convert into daily wind-speed raster
      # Extract maximum value per day
      storm_daily <- build_daily_raster(wind_speed, tz_value)

      if (is.null(storm_daily)) {
        next
      }

      # Set up list in current timezone
      # to store the raster layers
      if (is.null(daily_layers[[tz_value]])) {
        daily_layers[[tz_value]] <- list()
      }

      # Loop through every day (i.e. each daily layer)
      for (j in seq_len(terra::nlyr(storm_daily))) {
        layer_name <- names(storm_daily)[j]

        # for current timezone tz_value - for every day j / layer_name
        # if it is the first raster for the current timezone / date just store it
        # if not, merge it with the rest 
        daily_layers[[tz_value]][[layer_name]] <- merge_daily_layers(
          daily_layers[[tz_value]][[layer_name]],
          storm_daily[[j]]
        )
      }
    }

    gc(verbose = TRUE)
  }







  # Create the storm name lookup table
  storm_name_panel <- build_storm_name_panel()


  # rememberer
  # daily layers is a nested list
  # outer list are the four relevant time zones
  # each timezone is a list of spatrasters for every day there was a storm in this timezone
  # find out how many daily layers we have
  n_daily_layers <- sum(vapply(daily_layers, length, 1L))

  # Set up output list and some indices
  out_list <- list()
  out_idx <- 0L
  extracted_layers <- 0L


  for (tz_value in timezones) {

    # Names of all days with storms in the current timezone
    day_names <- sort(names(daily_layers[[tz_value]]))

    if (length(day_names) == 0) {
      next
    }

    # Assign ID to GEOID
    tz_context <- extract_context_for_tz(tz_value)

    # For every storm day in the current timezone
    for (i in seq_along(day_names)) {

      # Extract date
      layer_name <- day_names[[i]]
      date_value <- as.Date(sub("^d_", "", layer_name), format = "%Y_%m_%d")

      # Extract wind raster for current day and timezone
      day_rast <- daily_layers[[tz_value]][[layer_name]]

      # For each local day, extract the maximum wind over each polygon
      tract_vals_max <- terra::extract(
        day_rast,
        tz_context$tracts,
        fun = max,
        na.rm = TRUE,
        ID = TRUE
      )

      # For each local day, extract the mean wind over each polygon
      tract_vals_mean <- terra::extract(
        day_rast,
        tz_context$tracts,
        fun = mean,
        na.rm = TRUE,
        ID = TRUE
      )

      names(tract_vals_max)[2] <- "tc_poly_max_wind"
      names(tract_vals_mean)[2] <- "tc_poly_mean_wind"

      out_idx <- out_idx + 1L
      
      out_list[[out_idx]] <- tract_vals_max %>%

        left_join(tract_vals_mean, by = "ID") %>%
        # Join GEOID
        left_join(tz_context$id_lookup, by = "ID") %>%
        mutate(date = date_value) %>%
        select(all_of(tract_id), date, tc_poly_max_wind, tc_poly_mean_wind)

      extracted_layers <- extracted_layers + 1L

      if (
        verbose > 0 &&
        (extracted_layers %% 25 == 0 || extracted_layers == n_daily_layers)
      ) {
        message(
          sprintf(
            "tc_daily_panel_polygons(): extracted %d/%d timezone storm days",
            extracted_layers,
            n_daily_layers
          )
        )
      }

      gc(verbose = FALSE)
    }
  }

  # At this point we have another nested list
  # Outer layer: each storm day
  # Innerlayer: for each storm day x GEOID combination wind info



  if (length(out_list) == 0) {
    out <- tibble::tibble(
      !!tract_id := character(),
      date = as.Date(character()),
      tc_poly_max_wind = numeric(),
      tc_poly_mean_wind = numeric()
    )
  } else {
    # Convert into one panel
    out <- bind_rows(out_list) %>%
      arrange(GEOID, date)
  }

  # Add strom names
  # This is now a GEOID x date dataframe
  # For all GEOIDs, but only for those days where a storm acutally occured
  out <- out %>%
    left_join(storm_name_panel, by = c("GEOID", "date"))

  # If no start or end date is specified
  if (is.null(start_date)) {
    start_date <- min(out$date, na.rm = TRUE)
  } else {
    start_date <- as.Date(start_date)
  }

  if (is.null(end_date)) {
    end_date <- max(out$date, na.rm = TRUE)
  } else {
    end_date <- as.Date(end_date)
  }

  full_dates <- seq.Date(start_date, end_date, by = "day")

  # Build the daily GEOID x date panel
  full_panel <- tidyr::expand_grid(
    !!tract_id := unique(id_lookup[[tract_id]]),
    date = full_dates
  )

  # Build a full tract x day panel and fill non-storm days with 0
  if (fill_zeros) {

    out <- full_panel %>%
      left_join(out, by = c("GEOID", "date")) %>%
      mutate(
        # fill NAs with zeroes
        tc_poly_max_wind = coalesce(tc_poly_max_wind, 0),
        tc_poly_mean_wind = coalesce(tc_poly_mean_wind, 0),
        tc_poly_day = as.integer(tc_poly_max_wind > 0 | tc_poly_mean_wind > 0)
      ) %>%
      arrange(GEOID, date)

  # Or keep only the rows that already exist in out - i.e. storm days
  } else {

    out <- full_panel %>%
      left_join(out, by = c("GEOID", "date")) %>%
      mutate(
        tc_poly_day = if_else(
          is.na(tc_poly_max_wind) & is.na(tc_poly_mean_wind),
          0,
          as.integer(
            coalesce(tc_poly_max_wind, 0) > 0 |
            coalesce(tc_poly_mean_wind, 0) > 0
          )
        )
      )

  }

  return(out)
}
