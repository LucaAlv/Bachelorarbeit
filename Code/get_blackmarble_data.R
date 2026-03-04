#### This file essentially defines one function that extracts and prepares the blackmarble data ####
#### The result is a panel for each day and each tract in puerto rico ####
#### The data contains information on acttual light intensity and multiple quality flags 

build_bm_tract_panel <- function(
  # Variables for bm_extract
  tracts_sf,                                              # defines puerto rico as region of interest
  dates,
  bearer,
  base_dir = file.path(getwd(), "Data", "bm_files"),
  product_id = "VNP46A2",
  ntl_variable = "DNB_BRDF-Corrected_NTL",                # Set the non-gap filled data as standard
  quality_flag_rm = 2,                                    # 0: high-quality, persistent nighttime lights / 1: high-quality, ephermal nighttitme lights
  # Variables for QF_Cloud_Mask
  cloud_quality_min = 2,  # 2 = medium, 3 = high (according to table 4, bits 4-5)
  cloud_detect_ok = c(0,1), # 0 = confident clear, 1 = probably clear (according to table 4, bits 6-7)
  require_night = TRUE, # from bit 0: 0 = night, 1 = day
  exclude_shadow = TRUE, # from bit 8
  exclude_cirrus = TRUE, # from bit 9
  exclude_snow_ice = TRUE, # from bit 10
  include_hq_share = TRUE,
  overwrite = FALSE,
  quiet = TRUE
) {
  #### Some checks and preparation ####

  ## Dependencies ##
  pkgs <- c("blackmarbler", "sf", "dplyr", "purrr", "lubridate", "terra", "exactextractr", "tibble")
  # checks for missing packages and returns the ones that are missing
  miss <- pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly = FALSE)]
  # if there are missing packages execution is stopped and the missing packages are returned
  if (length(miss) > 0) {
    stop(
      "Missing packages: ", 
      paste(miss, collapse = ", "))
  }

  ## Create directories (if they don't already exist) ##
  dir.create(base_dir, recursive = TRUE, showWarnings = FALSE)
  h5_dir <- file.path(base_dir, "h5")
  out_dir <- file.path(base_dir, "tract_day")
  ntl_dir <- file.path(out_dir, "ntl_daily")
  cloud_dir <- file.path(out_dir, "cloud_daily")
  hq_dir <- file.path(out_dir, "hq_daily")
  logs_dir <- file.path(base_dir, "logs")
  dir.create(h5_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(ntl_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(cloud_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(hq_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(logs_dir, recursive = TRUE, showWarnings = FALSE)

  ## Some logging (looks nice in the terminal) ##
  # Sets up a file path to createt a new file inn the logs_dir directory thats called something like build_log_20260301_145839.txt
  log_file <- file.path(logs_dir, paste0("build_log_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".txt"))
  # Prints this out to the command line
  log_line <- function(...) {
    cat(
      sprintf(
        "[%s] %s\n", 
        format(Sys.time(), "%F %T"), 
        paste0(...)
      ), 
      file = log_file, 
      append = TRUE
    )
  }

  ## Set up helper functinos (mainly Define helper functions to extract information from bitmask QF_Cloud_Mask) ##

  # get_bits extracts specific bits from an integer, specifically bit start until bits start+nbits
  # E.g. start = 4 and nbit = 2 - then this extracts exactly bit 4 and 5
  # Intuitively this takes an integer x moves the bits we care about to the right and then chops off everything else
  # Bits are counted from the right (least significant bit is most right)
  get_bits <- function(x, start, nbits = 1L) {
    x <- as.integer(x)
    # bitwShiftR shifts the bits right and replaces trailing spaces with 0s (e.g. 1101 shifted by 2 -> 0011)
    # bitwShiftL does the same leftwards
    # Subtracting 1L turns all 1 -> 0 and all 0 -> 1
    bitwAnd(bitwShiftR(x, start), bitwShiftL(1L, nbits) - 1L)
  }

  # Decode the bit mask using the above defined helper function
  # For each integer (i.e. for each day x municipaltiy combination gives)
  # See sources for this
  decode_qf_cloud_mask <- function(x) {
    tibble::tibble(
      day_night = get_bits(x, 0, 1),          # 0 night, 1 day
      cloud_quality = get_bits(x, 4, 2),      # 0 poor, 1 low, 2 medium, 3 high
      cloud_detect = get_bits(x, 6, 2),       # 0 clear, 1 probably clear, 2 probably cloudy, 3 cloudy
      shadow = get_bits(x, 8, 1),             # 1 shadow
      cirrus = get_bits(x, 9, 1),             # 1 cirrus
      snow_ice = get_bits(x, 10, 1)           # 1 snow/ice
    )
  }

  # Define an indicator for is it night and is it cloudy and ...
  cloud_free_indicator <- function(qf_vals) {
    # Create the tibble from above for a vector of values qf_vals
    d <- decode_qf_cloud_mask(qf_vals)
    # Initially mark all values as ok
    ok <- rep(TRUE, length(qf_vals))

    # Filter for night if this was specified in the orginial function call
    if (require_night) {
      # Is true if ok is true and d$day_night is 0 (so its day)
      ok <- ok & (d$day_night == 0L)
    }

    if (exclude_shadow) {
      ok <- ok & (d$shadow == 0L)
    }

    if (exclude_cirrus) {
      ok <- ok & (d$cirrus == 0L)
  }

    if (exclude_snow_ice) {
      ok <- ok & (d$snow_ice == 0L)
    }

    ok <- ok & (d$cloud_quality >= as.integer(cloud_quality_min))

    ok <- ok & (d$cloud_detect %in% as.integer(cloud_detect_ok))

    as.integer(ok)
  }

  extract_ntl_one_day <- function(date_i) {
    f <- file.path(ntl_dir, paste0("ntl_", date_i, ".rds"))
    # skip extraction if file already exists
    if (!overwrite && file.exists(f)) {
      # returns true without printing it to the console
      return(invisible(TRUE))
    }

    res <- tryCatch({
      x <- blackmarbler::bm_extract(
        roi_sf = tracts_sf,
        product_id = product_id,
        date = as.Date(date_i),
        bearer = bearer,
        variable = ntl_variable,
        # Aggregate according to roi_sf (i.e. points are aggregated for each mun)
        aggregation_fun = c("mean", "median"),
        # Gives us a proportion of non na pixels
        add_n_pixels = TRUE,
        quality_flag_rm = quality_flag_rm,   
        # or (after inspecting values) quality_flag_rm = c(1,2,3,4,5) for only qf==0
        output_location_type = "memory",
        download_method = "httr",
        h5_dir = h5_dir,
        quiet = quiet
      )

      x_out <- mutate(
        x,
        GEOID,
        date = as.Date(date),
        ntl_mean,
        ntl_median,
        valid_pixel_share = prop_non_na_pixels,
        n_pixels,
        n_non_na_pixels,
        .keep = "none"
      )

      saveRDS(x_out, f)
      # Return true if we were able to download and save the data for each day
      return(TRUE)
    # What happens when an error occurs
    }, error = function(e) {
      log_line("NTL failed for ", date_i, ": ", conditionMessage(e))
      # Return false if something went wrong
      return(FALSE)
    })

    invisible(res)
  }

  extract_cloud_one_day <- function(date_i) {
    # For each day compute the share of cloud free pixels inside each polygon (mun)
    f <- file.path(cloud_dir, paste0("cloud_", date_i, ".rds"))
    if (!overwrite && file.exists(f)) {
      return(invisible(TRUE))
    }

    res <- tryCatch({
      # qf_r is an integer raster where each pixel is an integer / bit flag that encodes cloud-related information in its bits
      qf_r <- blackmarbler::bm_raster(
        roi_sf = tracts_sf,
        product_id = product_id,
        date = as.Date(date_i),
        bearer = bearer,
        variable = "QF_Cloud_Mask",
        output_location_type = "memory",
        download_method = "httr",
        h5_dir = h5_dir,
        quiet = quiet
      )[[1]]

      qf_vals <- terra::values(qf_r, mat = FALSE)
      cf_vals <- ifelse(is.na(qf_vals), NA_integer_, cloud_free_indicator(qf_vals))

      cf_r <- qf_r
      terra::values(cf_r) <- cf_vals

      shares <- exactextractr::exact_extract(cf_r, tracts_sf, "mean", progress = FALSE)

      out <- dplyr::tibble(
        GEOID = tracts_sf$GEOID,
        date = as.Date(date_i),
        cloud_free_share = shares
      )

      saveRDS(out, f)
      return(TRUE)
    }, error = function(e) {
      log_line("CLOUD failed for ", date_i, ": ", conditionMessage(e))
      return(FALSE)
    }
  )

  invisible(res)
  }

  extract_hq_one_day <- function(date_i) {
    # For each day compute the share of high quality pixels inside each polygon (mun)
    if (!include_hq_share) {
      return(invisible(TRUE))
    }

    f <- file.path(hq_dir, paste0("hq_", date_i, ".rds"))

    if (!overwrite && file.exists(f)) {
      return(invisible(TRUE))
    }

    res <- tryCatch({
      # qf_r is an integer raster where each pixel is an integer / bit flag that encodes cloud-related information in its bits
      mqf_r <- blackmarbler::bm_raster(
        roi_sf = tracts_sf,
        product_id = product_id,
        date = as.Date(date_i),
        bearer = bearer,
        variable = "Mandatory_Quality_Flag",
        output_location_type = "memory",
        download_method = "httr",
        h5_dir = h5_dir,
        quiet = quiet
      )[[1]]

      mqf_vals <- terra::values(mqf_r, mat = FALSE)
      # Note 255 is basically defined as NA, 00/01 high quality, 02 poor quality
      hq_vals <- ifelse(is.na(mqf_vals) | mqf_vals == 255L, NA_integer_, as.integer(mqf_vals %in% c(0L, 1L)))

      hq_r <- mqf_r
      terra::values(hq_r) <- hq_vals

      shares <- exactextractr::exact_extract(hq_r, tracts_sf, "mean", progress = FALSE)

      out <- dplyr::tibble(
        GEOID = tracts_sf$GEOID,
        date = as.Date(date_i),
        hq_share = shares
      )

      saveRDS(out, f)
      return(TRUE)
    }, error = function(e) {
      log_line("HQ failed for ", date_i, ": ", conditionMessage(e))
      return(FALSE)
    }
  )
  
  invisible(res)
  }

  ## Run actual extraction ##

  log_line("Starting build for ", length(dates), " dates. Output: ", out_dir)

  for (d in dates) {
    log_line("Processing ", d)
    extract_ntl_one_day(d)
    extract_cloud_one_day(d)
    extract_hq_one_day(d)
  }

  ## Combine files into one final panel ##

  read_all <- function(dir_path, pattern_prefix) {
    fs <- list.files(dir_path, pattern = paste0("^", pattern_prefix, ".*\\.rds$"), full.names = TRUE)
    if (length(fs) == 0) {
      return(dplyr::tibble())    
    }
    purrr::map_df(fs, readRDS)
  }

  ntl_panel <- read_all(ntl_dir, "ntl_")
  cloud_panel <- read_all(cloud_dir, "cloud_")
  hq_panel <- if (include_hq_share) read_all(hq_dir, "hq_") else dplyr::tibble()

  # Some diagnostics to find the problem
  if (!all(c("GEOID","date") %in% names(ntl_panel))) {
  stop("ntl_panel has no GEOID/date. Check extraction outputs in: ", ntl_dir,
       "\nAlso check log file(s) in: ", logs_dir)
  }
  if (!all(c("GEOID","date") %in% names(cloud_panel))) {
   stop("cloud_panel has no GEOID/date. Check extraction outputs in: ", cloud_dir,
         "\nAlso check log file(s) in: ", logs_dir)
  }

  panel <- ntl_panel %>%
    dplyr::left_join(cloud_panel, by = c("GEOID", "date")) %>%
    { if (include_hq_share) dplyr::left_join(., hq_panel, by = c("GEOID", "date")) else . } %>%
    dplyr::arrange(GEOID, date)

  final_path <- file.path(out_dir, "pr_tract_day_panel.rds")
  saveRDS(panel, final_path)
  log_line("Finished. Final panel saved to: ", final_path)

  return(panel)
}