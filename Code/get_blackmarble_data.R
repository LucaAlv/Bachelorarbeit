## Define NASA input_nasa_bearer token for blackmarble api ##
input_nasa_bearer <- get_nasa_token(
  username = Sys.getenv("NASA_USER"),
  password = Sys.getenv("NASA_PASS")
)

#### This file essentially defines one function that extracts and prepares the blackmarble data ####
#### The result is a panel for each day and each tract in puerto rico ####
#### The data contains information on acttual light intensity and multiple quality flags 

build_bm_tract_panel <- function(
  # Variables for bm_extract
  tracts_sf,                                              # defines puerto rico as region of interest
  dates,
  bearer,
  product_id = "VNP46A2",         
  ntl_variable = "DNB_BRDF-Corrected_NTL",                # Set the non-gap filled data as standard
  quality_flag_rm = 2,                                    # 0: high-quality, persistent nighttime lights / 1: high-quality, ephermal nighttitme lights
  # Variables for QF_Cloud_Mask
  cloud_quality_min = 1,  # 2 = medium, 3 = high (according to table 4, bits 4-5)
  cloud_detect_ok = c(0,1), # 0 = confident clear, 1 = probably clear (according to table 4, bits 6-7)
  require_night = TRUE, # from bit 0: 0 = night, 1 = day
  exclude_shadow = TRUE, # from bit 8
  exclude_cirrus = TRUE, # from bit 9
  exclude_snow_ice = TRUE, # from bit 10
  include_hq_share = TRUE,
  overwrite = FALSE,
  delete_downloads = FALSE,                                # delete raw H5 downloads after a date is fully cached
  quiet = TRUE,
  cache_key = NULL
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

  dates_expr <- paste(deparse(substitute(dates), width.cutoff = 500L), collapse = "")

  dates <- tryCatch(
    sort(unique(as.Date(dates))),
    error = function(e) {
      stop(
        "`dates` must evaluate to valid Date values. ",
        "The supplied expression was: ", dates_expr,
        "\nOriginal error: ", conditionMessage(e),
        call. = FALSE
      )
    }
  )

  if (any(is.na(dates))) {
    stop("`dates` must contain valid Date values.")
  }

  if (!inherits(tracts_sf, "sf")) {
    stop("`tracts_sf` must be an sf object.")
  }

  if (!("GEOID" %in% names(tracts_sf))) {
    stop("`tracts_sf` must contain a `GEOID` column.")
  }

  if (anyDuplicated(tracts_sf$GEOID)) {
    stop("`tracts_sf$GEOID` must be unique so the output panel has one row per geography and date.")
  }

  if (is.na(sf::st_crs(tracts_sf))) {
    stop("`tracts_sf` must have a defined CRS. Black Marble extraction expects WGS84 (EPSG:4326).")
  }

  if (any(!sf::st_is_valid(tracts_sf))) {
    if (!quiet) {
      message("Repairing invalid geometries in `tracts_sf` before Black Marble extraction.")
    }
    tracts_sf <- sf::st_make_valid(tracts_sf)
  }

  if (any(sf::st_is_empty(tracts_sf))) {
    stop("`tracts_sf` contains empty geometries after validation. Please remove or repair them before extraction.")
  }

  target_crs <- sf::st_crs(4326)
  if (!isTRUE(sf::st_crs(tracts_sf) == target_crs)) {
    if (!quiet) {
      message("Transforming `tracts_sf` to WGS84 (EPSG:4326) for Black Marble extraction.")
    }
    tracts_sf <- sf::st_transform(tracts_sf, target_crs)
  }

  geom_types <- unique(as.character(sf::st_geometry_type(tracts_sf, by_geometry = TRUE)))
  if (any(!geom_types %in% c("POLYGON", "MULTIPOLYGON"))) {
    stop("`tracts_sf` must contain polygon geometries. Found: ", paste(sort(geom_types), collapse = ", "))
  }

  tracts_sf <- dplyr::arrange(tracts_sf, GEOID)

  make_cache_key <- function() {
    cache_seed <- list(
      geoid = tracts_sf$GEOID,
      geometry = sf::st_as_binary(sf::st_geometry(tracts_sf), EWKB = TRUE),
      product_id = product_id,
      ntl_variable = ntl_variable,
      quality_flag_rm = as.integer(quality_flag_rm),
      cloud_quality_min = as.integer(cloud_quality_min),
      cloud_detect_ok = as.integer(cloud_detect_ok),
      require_night = require_night,
      exclude_shadow = exclude_shadow,
      exclude_cirrus = exclude_cirrus,
      exclude_snow_ice = exclude_snow_ice,
      include_hq_share = include_hq_share
    )

    tf <- tempfile(fileext = ".rds")
    saveRDS(cache_seed, tf)
    key <- unname(tools::md5sum(tf))
    unlink(tf)
    key
  }

  sanitize_cache_key <- function(x) {
    x <- gsub("[^A-Za-z0-9_-]+", "_", x)
    x <- gsub("_+", "_", x)
    x <- gsub("^_|_$", "", x)
    if (!nzchar(x)) {
      x <- "bm_panel"
    }
    x
  }

  auto_cache_key <- paste0(
    "bm_",
    substr(make_cache_key(), 1, 12),
    "_",
    substr(sanitize_cache_key(ntl_variable), 1, 30),
    "_n",
    nrow(tracts_sf)
  )
  cache_key <- sanitize_cache_key(if (is.null(cache_key)) auto_cache_key else cache_key)

  ## Create directories (if they don't already exist) ##
  
  base_dir <- file.path(getwd(), "Data/bm_files")
  dir.create(base_dir, recursive = TRUE, showWarnings = FALSE)
  h5_dir <- file.path(base_dir, "h5")
  out_dir <- file.path(base_dir, "tract_day", cache_key)
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
      )
    )
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
    geoid_sample = utils::head(tracts_sf$GEOID, 10),
    bbox_wgs84 = unclass(sf::st_bbox(tracts_sf)),
    date_min = min(dates),
    date_max = max(dates),
    created_at = Sys.time(),
    log_file = log_file
  )
  saveRDS(manifest, file.path(out_dir, "run_manifest.rds"))

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

  normalize_ntl_stats <- function(df) {
    needed <- c("ntl_mean", "ntl_median", "n_non_na_pixels")
    if (!all(needed %in% names(df))) {
      return(df)
    }

    zero_valid <- dplyr::coalesce(df$n_non_na_pixels, 0) == 0

    if ("prop_non_na_pixels" %in% names(df)) {
      zero_valid <- zero_valid | (dplyr::coalesce(df$prop_non_na_pixels, 0) == 0)
    }

    if ("valid_pixel_share" %in% names(df)) {
      zero_valid <- zero_valid | (dplyr::coalesce(df$valid_pixel_share, 0) == 0)
    }

    df %>%
      dplyr::mutate(
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

  empty_ntl_day <- function(date_i) {
    dplyr::tibble(
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
    out <- dplyr::tibble(
      GEOID = tracts_sf$GEOID,
      date = as.Date(date_i)
    )
    out[[value_name]] <- NA_real_
    out
  }

  expected_day_output_files <- function(date_i) {
    date_tag <- format(as.Date(date_i), "%Y-%m-%d")
    files <- c(
      file.path(ntl_dir, paste0("ntl_", date_tag, ".rds")),
      file.path(cloud_dir, paste0("cloud_", date_tag, ".rds"))
    )

    if (include_hq_share) {
      files <- c(files, file.path(hq_dir, paste0("hq_", date_tag, ".rds")))
    }

    files
  }

  cleanup_downloads_for_day <- function(date_i) {
    if (!delete_downloads) {
      return(invisible(0L))
    }

    expected_files <- expected_day_output_files(date_i)
    if (!all(file.exists(expected_files))) {
      log_line(
        "Skipping H5 cleanup for ", as.Date(date_i),
        " because one or more daily outputs are missing."
      )
      return(invisible(0L))
    }

    date_id <- format(as.Date(date_i), "%Y%j")
    pattern <- paste0("^", product_id, "\\.A", date_id, "\\..*\\.h5$")
    h5_files <- list.files(h5_dir, pattern = pattern, full.names = TRUE)

    if (length(h5_files) == 0) {
      return(invisible(0L))
    }

    unlink(h5_files, force = TRUE)
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

  extract_ntl_one_day <- function(date_i) {
    date_tag <- format(as.Date(date_i), "%Y-%m-%d")
    f <- file.path(ntl_dir, paste0("ntl_", date_tag, ".rds"))
    # skip extraction if file already exists
    if (!overwrite && file.exists(f)) {
      # returns true without printing it to the console
      return(invisible(TRUE))
    }

    missing_imagery <- FALSE
    res <- tryCatch({
      x <- withCallingHandlers(
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
            missing_imagery <<- TRUE
            invokeRestart("muffleWarning")
          }
        }
      )

      if (missing_imagery || is.null(x) || nrow(x) == 0) {
        x_out <- empty_ntl_day(date_i)
        saveRDS(x_out, f)
        log_line("NTL missing imagery for ", date_i, "; wrote NA rows.")
        return(TRUE)
      }

      x <- normalize_ntl_stats(x)

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
      log_line("NTL failed for ", date_i, ": ", conditionMessage(e))
      # Return false if something went wrong
      return(FALSE)
    })

    invisible(res)
  }

  extract_cloud_one_day <- function(date_i) {
    # For each day compute the share of cloud free pixels inside each polygon (mun)
    date_tag <- format(as.Date(date_i), "%Y-%m-%d")
    f <- file.path(cloud_dir, paste0("cloud_", date_tag, ".rds"))
    if (!overwrite && file.exists(f)) {
      return(invisible(TRUE))
    }

    missing_imagery <- FALSE
    res <- tryCatch({
      # qf_r is an integer raster where each pixel is an integer / bit flag that encodes cloud-related information in its bits
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

      if (missing_imagery || is.null(qf_r)) {
        out <- empty_share_day(date_i, "cloud_free_share")
        saveRDS(out, f)
        log_line("CLOUD missing imagery for ", date_i, "; wrote NA rows.")
        return(TRUE)
      }

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
      if (missing_imagery) {
        out <- empty_share_day(date_i, "cloud_free_share")
        saveRDS(out, f)
        log_line("CLOUD missing imagery for ", date_i, "; wrote NA rows.")
        return(TRUE)
      }
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

    date_tag <- format(as.Date(date_i), "%Y-%m-%d")
    f <- file.path(hq_dir, paste0("hq_", date_tag, ".rds"))

    if (!overwrite && file.exists(f)) {
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

      out <- dplyr::tibble(
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
      log_line("HQ failed for ", date_i, ": ", conditionMessage(e))
      return(FALSE)
    }
  )
  
  invisible(res)
  }

  ## Run actual extraction ##

  log_line("Starting build for ", length(dates), " dates. Cache key: ", cache_key, ". Output: ", out_dir)

  for (d in as.list(dates)) {
    log_line("Processing ", d)
    extract_ntl_one_day(d)
    extract_cloud_one_day(d)
    extract_hq_one_day(d)
    cleanup_downloads_for_day(d)
  }

  ## Combine files into one final panel ##

  check_missing_files <- function(dir_path, pattern_prefix, dates_use, label) {
    expected <- file.path(
      dir_path,
      paste0(pattern_prefix, format(as.Date(dates_use), "%Y-%m-%d"), ".rds")
    )
    missing <- expected[!file.exists(expected)]

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

  read_all <- function(dir_path, pattern_prefix, dates_use) {
    fs <- file.path(
      dir_path,
      paste0(pattern_prefix, format(as.Date(dates_use), "%Y-%m-%d"), ".rds")
    )
    fs <- fs[file.exists(fs)]
    if (length(fs) == 0) {
      return(dplyr::tibble())    
    }
    purrr::map_df(fs, readRDS)
  }

  ntl_panel <- read_all(ntl_dir, "ntl_", dates) %>%
    normalize_ntl_stats()
  cloud_panel <- read_all(cloud_dir, "cloud_", dates)
  hq_panel <- if (include_hq_share) read_all(hq_dir, "hq_", dates) else dplyr::tibble()

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

  final_path <- file.path(out_dir, "bm_panel.rds")
  saveRDS(panel, final_path)
  log_line("Finished. Final panel saved to: ", final_path)

  return(panel)
}
