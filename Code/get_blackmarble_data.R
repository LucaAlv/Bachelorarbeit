#### This file essentially defines one function that extracts and prepares the blackmarble data ####
#### The result is a panel for each day and each tract in puerto rico ####
#### The data contains information on actual light intensity and multiple quality flags

build_bm_tract_panel <- function(
  # Variables for bm_extract
  # For bit values see here: https://ladsweb.modaps.eosdis.nasa.gov/api/v2/content/archives/Document%20Archive/Science%20Data%20Product%20Documentation/Black-Marble_v2.0_UG_2024.pdf
  tracts_sf,                                              # defines puerto rico as region of interest
  dates,
  bearer,
  cache_key,
  product_id = "VNP46A2",         
  ntl_variable = "DNB_BRDF-Corrected_NTL",                # Set the non-gap filled data as standard
  quality_flag_rm = 2,                                    # Remove poor-quality/outlier/potential cloud-contaminated pixels; 0/1 are high-quality
  # Variables for QF_Cloud_Mask
  cloud_quality_min = 2,  # 2 = medium, 3 = high (according to table 4, bits 4-5)
  cloud_detect_ok = c(0,1), # 0 = confident clear, 1 = probably clear (according to table 4, bits 6-7)
  require_night = TRUE, # from bit 0: 0 = night, 1 = day
  exclude_shadow = TRUE, # from bit 8
  exclude_cirrus = TRUE, # from bit 9
  exclude_snow_ice = TRUE, # from bit 10
  include_hq_share = TRUE,
  delete_downloads = FALSE,                                # delete raw H5 downloads after a date is fully cached
  quiet = FALSE
) {






  #### Some checks and preparation ####

  dates_expr <- paste(deparse(substitute(dates)), collapse = "")

  if (any(is.na(dates))) {
    stop("dates must contain valid Date values.")
  }

  # Input checks for the geometric information
  # This is important because it usually didn't throw an error
  # When something was wrong with this
  if (!inherits(tracts_sf, "sf")) {
    stop("tracts_sf must be an sf object.")
  }

  if (anyDuplicated(tracts_sf$GEOID)) {
    stop("tracts_sf$GEOID must be unique so the output panel has one row per geography and date.")
  }

  if (is.na(st_crs(tracts_sf))) {
    stop("tracts_sf must have a defined CRS. Black Marble extraction expects WGS84 (EPSG:4326).")
  }

  if (any(!st_is_valid(tracts_sf))) {
    if (!quiet) {
      message("Repairing invalid geometries.")
    }
    tracts_sf <- st_make_valid(tracts_sf)
  }

  if (any(st_is_empty(tracts_sf))) {
    stop("tracts_sf contains empty geometries after validation. Please remove or repair them before extraction.")
  }

  geom_types <- unique(as.character(st_geometry_type(tracts_sf, by_geometry = TRUE)))
  
  if (any(!geom_types %in% c("POLYGON", "MULTIPOLYGON"))) {
    stop("tracts_sf must contain polygon geometries. Found: ", paste(sort(geom_types), collapse = ", "))
  }

  # Transform to WGS84
  target_crs <- st_crs(4326)
  if (!isTRUE(st_crs(tracts_sf) == target_crs)) {
    if (!quiet) {
      message("Transforming tracts_sf to WGS84 (EPSG:4326) for Black Marble extraction.")
    }
    tracts_sf <- st_transform(tracts_sf, target_crs)
  }







  ## Create directories (if they don't already exist) ##
  
  base_dir <- file.path(getwd(), "Data/bm_files")

  out_dir <- file.path(base_dir, "tract_day", cache_key)
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  h5_dir <- file.path(base_dir, "h5")
  ntl_dir <- file.path(out_dir, "ntl_daily")
  cloud_dir <- file.path(out_dir, "cloud_daily")
  hq_dir <- file.path(out_dir, "hq_daily")
  logs_dir <- file.path(base_dir, "logs")

  required_dirs <- c(base_dir, out_dir, h5_dir, ntl_dir, cloud_dir, logs_dir)
  if (include_hq_share) {
    required_dirs <- c(required_dirs, hq_dir)
  }

  for (dir_i in required_dirs) {
    if (!dir.exists(dir_i)) {
      dir.create(dir_i, recursive = TRUE, showWarnings = FALSE)
    }
    if (!dir.exists(dir_i)) {
      stop("Could not create required directory: ", dir_i)
    }
  }







  # Prints this out to the command line
  log_line <- function(...) {
    line <- sprintf(
      "[%s] %s\n",
      format(Sys.time(), "%F %T"),
      paste0(...)
    )
    cat(line)
    cat(line, file = file.path(logs_dir, paste0(cache_key, ".log")), append = TRUE)
  }

  format_condition <- function(e) {
    msg <- conditionMessage(e)
    call <- conditionCall(e)

    if (!is.null(call)) {
      msg <- paste0(msg, " | call: ", paste(deparse(call), collapse = " "))
    }

    paste0(msg, " | class: ", paste(class(e), collapse = "/"))
  }

  manifest <- list(
    cache_key = cache_key,
    product_id = product_id,
    ntl_variable = ntl_variable,
    quality_flag_rm = quality_flag_rm,
    cloud_quality_min = cloud_quality_min,
    cloud_detect_ok = cloud_detect_ok,
    require_night = require_night,
    exclude_shadow = exclude_shadow,
    exclude_cirrus = exclude_cirrus,
    exclude_snow_ice = exclude_snow_ice,
    include_hq_share = include_hq_share,
    delete_downloads = delete_downloads,
    n_geographies = nrow(tracts_sf),
    geoid_sample = head(tracts_sf$GEOID, 10),
    bbox_wgs84 = unclass(st_bbox(tracts_sf)),
    date_min = min(dates),
    date_max = max(dates),
    created_at = Sys.time(),
    completed = FALSE
  )

  saveRDS(manifest, file.path(out_dir, "run_manifest.rds"))











  ## Set up helper functinos - mainly to extract information from bitmask QF_Cloud_Mask) ##

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
    tibble(
      day_night = get_bits(x, 0, 1),          # 0 night, 1 day
      cloud_quality = get_bits(x, 4, 2),      # 0 poor, 1 low, 2 medium, 3 high
      cloud_detect = get_bits(x, 6, 2),       # 0 clear, 1 probably clear, 2 probably cloudy, 3 cloudy
      shadow = get_bits(x, 8, 1),             # 1 shadow
      cirrus = get_bits(x, 9, 1),             # 1 cirrus
      snow_ice = get_bits(x, 10, 1)           # 1 snow/ice
    )
  }

  # Small helper function to check if
  # It is night, 
  cloud_free_indicator <- function(qf_vals) {

    # Create the tibble from above for a vector of values qf_vals
    # Returns 
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

    # Check for minimal cloud quality
    ok <- ok & (d$cloud_quality >= as.integer(cloud_quality_min))

    ok <- ok & (d$cloud_detect %in% as.integer(cloud_detect_ok))

    as.integer(ok)
  }

  # Small helper function to make sure that downloaded data 
  # missing or invalid data is correctly represented as such in the final data
  normalize_ntl_stats <- function(df) {

    # Check if all columns where downloaded correctly
    needed <- c("ntl_mean", "ntl_median", "n_non_na_pixels")
    if (!all(needed %in% names(df))) {
      return(df)
    }

    # If n_non_na_pixels is NA, assume there are no valid pixels
    # This is to make sure no information is lost, when doing e.g. na.rm = TRUE in aggregations
    zero_valid <- coalesce(df$n_non_na_pixels, 0) == 0

    if ("prop_non_na_pixels" %in% names(df)) {
      zero_valid <- zero_valid | (coalesce(df$prop_non_na_pixels, 0) == 0)
    }

    if ("valid_pixel_share" %in% names(df)) {
      zero_valid <- zero_valid | (coalesce(df$valid_pixel_share, 0) == 0)
    }

    # replace ntl values with NA when there was no credible information
    # according to prop_non_na_pixels or valid_pixel_share
    df %>%
      mutate(
        ntl_mean = ifelse(zero_valid | is.nan(ntl_mean), NA_real_, ntl_mean),
        ntl_median = ifelse(zero_valid | is.nan(ntl_median), NA_real_, ntl_median)
      )
  }

  is_missing_imagery_warning <- function(msg, date_i) {
    grepl(
      paste0("No satellite imagery exists for ", format(as.Date(date_i), "%Y-%m-%d")),
      msg,
      fixed = TRUE
    )
  }

  # This small helper function creates an output if no data could be downloaded
  empty_ntl_day <- function(date_i) {
    tibble(
      GEOID = tracts_sf$GEOID,
      date = as.Date(date_i),
      ntl_mean = NA_real_,
      ntl_median = NA_real_,
      valid_pixel_share = NA_real_,
      n_pixels = NA_real_,
      n_non_na_pixels = NA_real_
    )
  }

  empty_share_day <- function(date_i, value_name) {
    out <- tibble(
      GEOID = tracts_sf$GEOID,
      date = as.Date(date_i)
    )
    out[[value_name]] <- NA_real_
    out
  }

  # Small helper function to return the names of the files
  # for each day
  expected_day_output_files <- function(date_i) {
    
    # Convert to date
    date_tag <- format(as.Date(date_i), "%Y-%m-%d")
    
    # Name of output files
    files <- c(
      file.path(ntl_dir, paste0("ntl_", date_tag, ".rds")),
      file.path(cloud_dir, paste0("cloud_", date_tag, ".rds"))
    )

    if (include_hq_share) {
      files <- c(files, file.path(hq_dir, paste0("hq_", date_tag, ".rds")))
    }

    files
  }

  # This deletes the H5 file downloads after they were processed
  cleanup_downloads_for_day <- function(date_i) {
    if (!delete_downloads) {
      return(invisible(0L))
    }

    expected_files <- expected_day_output_files(date_i)

    # Only delete H5 files if all processing worked
    # I.e. all output files were generated and saved
    if (!all(file.exists(expected_files))) {
      log_line(
        "Skipping H5 cleanup for ", as.Date(date_i),
        " because one or more daily outputs are missing."
      )
      return(invisible(0L))
    }

    ## Find h5 files for this day (don't want to delete all H5 files)
    # Just the ones we just downlaoded
    # Extract year and day from date
    date_id <- format(as.Date(date_i), "%Y%j")
    pattern <- paste0("^", product_id, "\\.A", date_id, "\\..*\\.h5$")
    h5_files <- list.files(h5_dir, pattern = pattern, full.names = TRUE)

    # All files already deleted - or nothing downloaded
    if (length(h5_files) == 0) {
      return(invisible(0L))
    }

    # Delete files
    unlink(h5_files, force = TRUE)
    
    ## Do some nice diagnostics in the terminal
    n_deleted <- sum(!file.exists(h5_files))

    if (n_deleted > 0) {
      log_line("Deleted ", n_deleted, " downloaded H5 file(s) for ", as.Date(date_i), ".")
    }

    if (n_deleted < length(h5_files)) {
      log_line(
        "Some H5 files could not be deleted for ", as.Date(date_i),
        "; check permissions in ", h5_dir
      )
    }

    invisible(n_deleted)
  }



  ### Main extraction functions ###

  # Function to extract the NTL data for one day
  extract_ntl_one_day <- function(date_i) {

    # Make sure formatting is correct
    date_tag <- format(as.Date(date_i), "%Y-%m-%d")
    
    # Set up output file path
    f <- file.path(ntl_dir, paste0("ntl_", date_tag, ".rds"))
    
    # skip extraction if file already exists
    if (file.exists(f)) {
      # returns true without printing it to the console
      message(paste0(f, " already exists. Continuing with next date."))
      return(invisible(TRUE))
    }

    missing_imagery <- FALSE

    # Error handling so this doesn't always crash the whole program 
    # if something goes wrong for one date (e.g. imagery is missing)
    # First the protected expression (everything inside the {}) 
    # if this should crash not the whole programm crashes
    # But the second part (i.e. the error function) is executed
    # Outer safety net:
    res <- tryCatch(
    {

      # Inner safety net:
      # Watches for warnings from bm_extract
      # If bm_extract reutrns a warning message that there is data missing
      # I.e. the warning that matches is_missing_imager_warning
      # Then it executes the warning function
      x <- withCallingHandlers(
        
        # This loads the actual nightlight data
        # x is then a dataframe
        # with one row for every GEOID
        # and ntl_mean, ntl_median, n_pixels, n_non_na_pixels, prop_non_na_pixels information
        blackmarbler::bm_extract(
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
        ),

        warning = function(w) {
          if (is_missing_imagery_warning(conditionMessage(w), date_i)) {
            # There is missing imagery
            # use <<- for global assignment
            missing_imagery <<- TRUE
            invokeRestart("muffleWarning")
          }
        }

      )

      # After the bm_extract function ran
      # Check if there was missing data

      # If there was missing data:
      if (missing_imagery || is.null(x) || nrow(x) == 0) {
        
        # Return dataframe with NA rows
        x_out <- empty_ntl_day(date_i)
        saveRDS(x_out, f)
        log_line("NTL missing imagery for ", date_i, "; wrote NA rows.")
        return(TRUE)

      }

      # If there was no missing data:
      x <- normalize_ntl_stats(x)

      # Set up output panel
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
      if (missing_imagery) {
        x_out <- empty_ntl_day(date_i)
        saveRDS(x_out, f)
        log_line("NTL missing imagery for ", date_i, "; wrote NA rows.")
        return(TRUE)
      }
      log_line("NTL failed for ", date_i, ": ", format_condition(e))
      # Return false if something went wrong
      return(FALSE)
    })

    invisible(res)
  }



  # Function to extract the cloud mask quality flag data for one day
  # This returns a tibble with one row for every GEOID and date
  # And a cloud free share
  # Note the wording here is sort of wrong
  # This function doesn't only check for clouds
  # Also for night, cloud quality, cloud detection, shadows, cirrus and snow/ice
  extract_cloud_one_day <- function(date_i) {

    # See the extract ntl function above for more details on how this works
    date_tag <- format(as.Date(date_i), "%Y-%m-%d")
    f <- file.path(cloud_dir, paste0("cloud_", date_tag, ".rds"))
    if (file.exists(f)) {
      return(invisible(TRUE))
    }

    missing_imagery <- FALSE

    res <- tryCatch({
      # qf_r is an integer raster where each pixel is an integer / 
      # bit flag that encodes cloud-related information in its bits
      # This means that one number encodes several things
      qf_r <- withCallingHandlers(
        blackmarbler::bm_raster(
          roi_sf = tracts_sf,
          product_id = product_id,
          date = as.Date(date_i),
          bearer = bearer,
          variable = "QF_Cloud_Mask",
          output_location_type = "memory",
          download_method = "httr",
          h5_dir = h5_dir,
          quiet = quiet
        ),
        warning = function(w) {
          if (is_missing_imagery_warning(conditionMessage(w), date_i)) {
            missing_imagery <<- TRUE
            invokeRestart("muffleWarning")
          }
        }
      )[[1]]

      # Check for missing images
      if (missing_imagery || is.null(qf_r)) {
        out <- empty_share_day(date_i, "cloud_free_share")
        saveRDS(out, f)
        log_line("CLOUD missing imagery for ", date_i, "; wrote NA rows.")
        return(TRUE)
      }

      # Extract information from bitmasks
      # Pull all pixels values out of qf_r
      qf_vals <- terra::values(qf_r, mat = FALSE)

      # Convert raw quality-flag values into cloud-free (and some other stuff) indicator
      # checks for clear/probably clear, night, acceptable cloud quality, not shadow/cirrus/snow-ice contaminated
      cf_vals <- ifelse(is.na(qf_vals), NA_integer_, cloud_free_indicator(qf_vals))

      # Add indicator values to the raster
      cf_r <- qf_r
      terra::values(cf_r) <- cf_vals
      # cf_r is now a binary raster 

      # the mean over a county now equals the share of-cloud free pixels
      shares <- exactextractr::exact_extract(cf_r, tracts_sf, "mean", progress = FALSE)

      out <- tibble(
        GEOID = tracts_sf$GEOID,
        date = as.Date(date_i),
        cloud_free_share = shares
      )

      saveRDS(out, f)
      return(TRUE)

    }, error = function(e) {
      if (missing_imagery) {
        out <- empty_share_day(date_i, "cloud_free_share")
        saveRDS(out, f)
        log_line("CLOUD missing imagery for ", date_i, "; wrote NA rows.")
        return(TRUE)
      }
      log_line("CLOUD failed for ", date_i, ": ", format_condition(e))
      return(FALSE)
    }
  )

  invisible(res)
  }

  # Function to extract the mandatory quality flag data for one day
  extract_hq_one_day <- function(date_i) {
    # For each day compute the share of high quality pixels inside each polygon (mun)
    if (!include_hq_share) {
      return(invisible(TRUE))
    }

    date_tag <- format(as.Date(date_i), "%Y-%m-%d")
    f <- file.path(hq_dir, paste0("hq_", date_tag, ".rds"))

    if (file.exists(f)) {
      return(invisible(TRUE))
    }

    missing_imagery <- FALSE
    res <- tryCatch({
      # qf_r is an integer raster where each pixel is an integer / bit flag that encodes cloud-related information in its bits
      mqf_r <- withCallingHandlers(
        blackmarbler::bm_raster(
          roi_sf = tracts_sf,
          product_id = product_id,
          date = as.Date(date_i),
          bearer = bearer,
          variable = "Mandatory_Quality_Flag",
          output_location_type = "memory",
          download_method = "httr",
          h5_dir = h5_dir,
          quiet = quiet
        ),
        warning = function(w) {
          if (is_missing_imagery_warning(conditionMessage(w), date_i)) {
            missing_imagery <<- TRUE
            invokeRestart("muffleWarning")
          }
        }
      )[[1]]

      if (missing_imagery || is.null(mqf_r)) {
        out <- empty_share_day(date_i, "hq_share")
        saveRDS(out, f)
        log_line("HQ missing imagery for ", date_i, "; wrote NA rows.")
        return(TRUE)
      }

      mqf_vals <- terra::values(mqf_r, mat = FALSE)
      # Note 255 is basically defined as NA, 00/01 high quality, 02 poor quality
      hq_vals <- ifelse(is.na(mqf_vals) | mqf_vals == 255L, NA_integer_, as.integer(mqf_vals %in% c(0L, 1L)))

      hq_r <- mqf_r
      terra::values(hq_r) <- hq_vals

      shares <- exactextractr::exact_extract(hq_r, tracts_sf, "mean", progress = FALSE)

      out <- tibble(
        GEOID = tracts_sf$GEOID,
        date = as.Date(date_i),
        hq_share = shares
      )

      saveRDS(out, f)
      return(TRUE)
    }, error = function(e) {
      if (missing_imagery) {
        out <- empty_share_day(date_i, "hq_share")
        saveRDS(out, f)
        log_line("HQ missing imagery for ", date_i, "; wrote NA rows.")
        return(TRUE)
      }
      log_line("HQ failed for ", date_i, ": ", format_condition(e))
      return(FALSE)
    }
  )
  
  invisible(res)
  }








  ## Run actual extraction ##

  log_line("Starting build for ", length(dates), " dates. Cache key: ", cache_key, ". Output: ", out_dir)

  for (d in as.list(dates)) {
    # Print log diagnostics
    log_line("Processing ", d)

    # Exctract nightlight data
    extract_ntl_one_day(d)

    extract_cloud_one_day(d)

    extract_hq_one_day(d)

    cleanup_downloads_for_day(d)
  }





  ## Check if any files are missing or weren't processed correctly

  check_missing_files <- function(dir_path, pattern_prefix, dates_use, label) {

    # Build expected .rds filename for every requested date
    expected <- file.path(
      dir_path,
      paste0(pattern_prefix, format(as.Date(dates_use), "%Y-%m-%d"), ".rds")
    )

    missing <- expected[!file.exists(expected)]

    # Stop with error if any file is missing
    if (length(missing) > 0) {

      missing_tags <- sub(paste0("^", dir_path, "/?", pattern_prefix), "", missing)
      missing_tags <- sub("\\.rds$", "", missing_tags)
      
      stop(
        label, " extraction is incomplete for ", length(missing), " requested date(s): ",
        paste(missing_tags, collapse = ", "),
        "\nCheck log file(s) in: ", logs_dir
      )

    }
  }

  check_missing_files(ntl_dir, "ntl_", dates, "NTL")

  check_missing_files(cloud_dir, "cloud_", dates, "Cloud")
  
  if (include_hq_share) {
    check_missing_files(hq_dir, "hq_", dates, "HQ")
  }







  ## Combine files into one final panel

  # Small helper function to read in all files for the specified dates
  read_all <- function(dir_path, pattern_prefix, dates_use) {

    # List of file paths of processed daily files
    fs <- file.path(
      dir_path,
      paste0(pattern_prefix, format(as.Date(dates_use), "%Y-%m-%d"), ".rds")
    )

    fs <- fs[file.exists(fs)]

    if (length(fs) == 0) {
      return(tibble())    
    }

    # Read in all daily panels
    daily_panels <- map(fs, readRDS)
    # Rowbind them into one panel
    list_rbind(daily_panels)
  }

  ntl_panel <- read_all(ntl_dir, "ntl_", dates) %>%
    normalize_ntl_stats()
    
  cloud_panel <- read_all(cloud_dir, "cloud_", dates)

  hq_panel <- if (include_hq_share) read_all(hq_dir, "hq_", dates) else tibble()





  # Some diagnostics to find the problem
  if (!all(c("GEOID","date") %in% names(ntl_panel))) {
  stop("ntl_panel has no GEOID/date. Check extraction outputs in: ", ntl_dir,
       "\nAlso check log file(s) in: ", logs_dir)
  }
  if (!all(c("GEOID","date") %in% names(cloud_panel))) {
   stop("cloud_panel has no GEOID/date. Check extraction outputs in: ", cloud_dir,
         "\nAlso check log file(s) in: ", logs_dir)
  }




  # Create output panel
  panel <- ntl_panel %>%
    left_join(cloud_panel, by = c("GEOID", "date")) %>%
    { if (include_hq_share) left_join(., hq_panel, by = c("GEOID", "date")) else . } %>%
    arrange(GEOID, date)

  final_path <- file.path(out_dir, "bm_panel.rds")
  saveRDS(panel, final_path)

  manifest$completed <- TRUE
  manifest$completed_at <- Sys.time()
  saveRDS(manifest, file.path(out_dir, "run_manifest.rds"))

  log_line("Finished. Final panel saved to: ", final_path)

  return(panel)
}
