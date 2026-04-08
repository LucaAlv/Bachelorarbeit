get_meta_movement_distribution <- function(
  start_date,
  end_date,
  country_iso3 = "PRI",
  region_name = NULL,
  admin_level = NULL,
  cache_dir = "hdx_cache/movement_distribution"
) {
  pkgs <- c("archive", "dplyr", "fs", "httr2", "janitor", "lubridate", "purrr", "readr", "stringr", "tibble")
  miss <- pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly = TRUE)]

  if (length(miss) > 0) {
    stop("Missing packages: ", paste(miss, collapse = ", "))
  }

  start_date <- as.Date(start_date)
  end_date <- as.Date(end_date)
  country_iso3 <- toupper(country_iso3)

  if (is.na(start_date) || is.na(end_date)) {
    stop("start_date and end_date must be valid dates.")
  }

  if (start_date > end_date) {
    stop("start_date must be on or before end_date.")
  }

  fs::dir_create(cache_dir, recurse = TRUE)

  guess_col <- function(nms, candidates, pattern = NULL) {
    hit <- intersect(candidates, nms)
    if (length(hit) > 0) {
      return(hit[[1]])
    }

    if (!is.null(pattern)) {
      hit <- nms[stringr::str_detect(nms, pattern)]
      if (length(hit) > 0) {
        return(hit[[1]])
      }
    }

    NULL
  }

  guess_date_col <- function(nms) {
    guess_col(
      nms,
      candidates = c("date", "ds", "day", "observation_date"),
      pattern = "date|day|ds"
    )
  }

  parse_resource_window <- function(x) {
    matches <- stringr::str_match(x, "(\\d{4}-\\d{2}-\\d{2})[_-](\\d{4}-\\d{2}-\\d{2})")

    tibble::tibble(
      file_start = lubridate::ymd(matches[, 2]),
      file_end = lubridate::ymd(matches[, 3])
    )
  }

  build_resources_from_meta <- function() {
    meta <- httr2::request("https://data.humdata.org/api/3/action/package_show") |>
      httr2::req_url_query(id = "movement-distribution") |>
      httr2::req_perform() |>
      httr2::resp_body_json(simplifyVector = TRUE)

    resources <- tibble::as_tibble(meta$result$resources) |>
      dplyr::transmute(
        resource_id = id,
        resource_name = name,
        download_url = url,
        zip_path = fs::path(cache_dir, basename(url))
      ) |>
      dplyr::filter(
        stringr::str_detect(resource_name, "movement-distribution-data-for-good"),
        !stringr::str_detect(resource_name, "maps"),
        stringr::str_detect(resource_name, "\\.zip$")
      )

    dplyr::bind_cols(resources, parse_resource_window(resources$resource_name))
  }

  build_resources_from_cache <- function() {
    zip_paths <- list.files(
      cache_dir,
      pattern = "^movement-distribution-data-for-good.*\\.zip$",
      full.names = TRUE
    )

    zip_paths <- zip_paths[!stringr::str_detect(basename(zip_paths), "maps")]

    resources <- tibble::tibble(
      resource_id = NA_character_,
      resource_name = basename(zip_paths),
      download_url = NA_character_,
      zip_path = zip_paths
    )

    dplyr::bind_cols(resources, parse_resource_window(resources$resource_name))
  }

  meta_error <- NULL
  resources <- tryCatch(
    build_resources_from_meta(),
    error = function(e) {
      meta_error <<- conditionMessage(e)
      NULL
    }
  )

  if (is.null(resources)) {
    resources <- build_resources_from_cache()
  }

  resources <- resources |>
    dplyr::filter(
      !is.na(file_start),
      !is.na(file_end),
      file_start <= end_date,
      file_end >= start_date
    ) |>
    dplyr::distinct(resource_name, .keep_all = TRUE) |>
    dplyr::arrange(file_start)

  if (nrow(resources) == 0) {
    if (!is.null(meta_error)) {
      stop(
        "Failed to retrieve HDX metadata and no matching cached zip files were found. Original error: ",
        meta_error
      )
    }

    stop("No overlapping movement-distribution zip files found for the requested period.")
  }

  ensure_extracted_csvs <- function(zip_path, download_url) {
    if (!fs::file_exists(zip_path)) {
      if (is.na(download_url) || download_url == "") {
        stop("Missing cached zip and no download URL is available for: ", basename(zip_path))
      }

      httr2::request(download_url) |>
        httr2::req_perform(path = zip_path)
    }

    extract_dir <- fs::path(cache_dir, sub("\\.zip$", "", basename(zip_path)))
    csv_files <- list.files(extract_dir, pattern = "\\.csv$", recursive = TRUE, full.names = TRUE)

    if (length(csv_files) == 0) {
      fs::dir_create(extract_dir, recurse = TRUE)
      archive::archive_extract(zip_path, dir = extract_dir)
      csv_files <- list.files(extract_dir, pattern = "\\.csv$", recursive = TRUE, full.names = TRUE)
    }

    if (length(csv_files) == 0) {
      stop("No CSV files found after extracting: ", basename(zip_path))
    }

    csv_dates <- lubridate::ymd(
      stringr::str_match(basename(csv_files), "(\\d{4}-\\d{2}-\\d{2})")[, 2]
    )

    has_date <- !is.na(csv_dates)
    if (any(has_date)) {
      csv_files <- csv_files[!has_date | (csv_dates >= start_date & csv_dates <= end_date)]
    }

    csv_files
  }

  read_one_csv <- function(csv_path) {
    df <- readr::read_csv(csv_path, show_col_types = FALSE, progress = FALSE) |>
      janitor::clean_names()

    date_col <- guess_date_col(names(df))

    if (!is.null(date_col) && date_col != "date") {
      names(df)[names(df) == date_col] <- "date"
    }

    if ("date" %in% names(df)) {
      df$date <- as.Date(df$date)
    } else {
      file_date <- lubridate::ymd(
        stringr::str_match(basename(csv_path), "(\\d{4}-\\d{2}-\\d{2})")[, 2]
      )
      df$date <- as.Date(file_date)
    }

    df <- df[!is.na(df$date) & df$date >= start_date & df$date <= end_date, , drop = FALSE]

    country_col <- guess_col(names(df), c("country", "iso", "country_iso3"))
    if (is.null(country_col)) {
      stop("Couldn't find a country column automatically in: ", basename(csv_path))
    }

    df <- df[toupper(df[[country_col]]) == country_iso3, , drop = FALSE]

    if (!is.null(region_name)) {
      region_col <- guess_col(names(df), c("gadm_name", "name", "region_name", "admin_name"))
      if (is.null(region_col)) {
        stop("Couldn't find a region name column automatically in: ", basename(csv_path))
      }

      keep_region <- !is.na(df[[region_col]]) &
        stringr::str_detect(df[[region_col]], stringr::fixed(region_name, ignore_case = TRUE))
      df <- df[keep_region, , drop = FALSE]
    }

    if (!is.null(admin_level)) {
      admin_col <- guess_col(names(df), c("admin_level", "polygon_level"))
      if (is.null(admin_col)) {
        stop("Couldn't find an admin level column automatically in: ", basename(csv_path))
      }

      df <- df[!is.na(df[[admin_col]]) & df[[admin_col]] == admin_level, , drop = FALSE]
    }

    tibble::as_tibble(df)
  }

  out <- purrr::map_dfr(seq_len(nrow(resources)), function(i) {
    csv_files <- ensure_extracted_csvs(resources$zip_path[i], resources$download_url[i])
    purrr::map_dfr(csv_files, read_one_csv)
  }) |>
    dplyr::distinct() |>
    dplyr::arrange(date)

  out
}
