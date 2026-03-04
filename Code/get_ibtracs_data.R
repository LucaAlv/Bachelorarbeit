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
