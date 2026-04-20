# ERA5 hourly total precipitation for Puerto Rico, 2017
# Dataset: reanalysis-era5-single-levels
# Variable: total_precipitation
# Output: 12 monthly files downloaded in parallel via wf_request_batch()
#
# Notes:
# - The CDS website currently offers GRIB or NetCDF output.
# - GRIB is usually the lighter request; NetCDF is often easier to inspect.
# - terra::rast() can read either format.

# -----------------------------
# 1) Install packages (once)
# -----------------------------
# install.packages(c("ecmwfr", "terra"))

library(ecmwfr)
library(terra)

# -----------------------------
# 2) Set your CDS API token (once)
# -----------------------------
# Get your Personal Access Token from your CDS profile, then run once:
# wf_set_key(key = "YOUR_CDS_PERSONAL_ACCESS_TOKEN")
#
# You also need to:
# - create a CDS account
# - accept the Terms of Use for the ERA5 dataset in the CDS web interface

# -----------------------------
# 3) User settings
# -----------------------------
year_req    <- "2017"
out_dir     <- "era5_pr_2017_tp_batch"
workers     <- 3                    # keep modest; CDS allows up to 20 concurrent requests
file_format <- "netcdf"             # choose "grib" or "netcdf"

# -----------------------------
# 4) Define output folder
# -----------------------------
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

# -----------------------------
# 5) Puerto Rico bounding box
#    CDS area order = North, West, South, East
# -----------------------------
puerto_rico_area <- c(18.7, -67.5, 17.8, -65.1)

# -----------------------------
# 6) Helper objects
# -----------------------------
months_req <- sprintf("%02d", 1:12)
hours_req  <- sprintf("%02d:00", 0:23)

# Validate requested output format
file_format <- match.arg(file_format, choices = c("grib", "netcdf"))
file_ext <- if (file_format == "grib") "grib" else "nc"

days_in_month <- function(year_chr, month_chr) {
  start_date <- as.Date(sprintf("%s-%s-01", year_chr, month_chr))
  next_month <- seq(start_date, length = 2, by = "1 month")[2]
  n_days <- as.integer(next_month - start_date)
  sprintf("%02d", seq_len(n_days))
}

# -----------------------------
# 7) Build one request per month
# -----------------------------
make_monthly_request <- function(month_chr) {
  target_file <- sprintf(
    "era5_tp_puerto_rico_%s_%s.%s",
    year_req,
    month_chr,
    file_ext
  )

  request <- list(
    dataset_short_name = "reanalysis-era5-single-levels",
    product_type       = "reanalysis",
    variable           = "total_precipitation",
    year               = year_req,
    month              = month_chr,
    day                = days_in_month(year_req, month_chr),
    time               = hours_req,
    data_format        = file_format,
    area               = puerto_rico_area,
    target             = target_file
  )

  # Optional: for some CDS NetCDF downloads, unarchived output is preferred.
  # If you need it, uncomment the next line.
  # request$download_format <- "unarchived"

  request
}

request_list <- lapply(months_req, make_monthly_request)

# Check the first request before submitting all 12
str(request_list[[1]])

# -----------------------------
# 8) Submit the 12 monthly requests in parallel
# -----------------------------
files_downloaded <- wf_request_batch(
  request_list  = request_list,
  workers       = workers,
  path          = out_dir,
  time_out      = 3600,
  retry         = 10,
  total_timeout = length(request_list) * 3600 / workers
)

print(files_downloaded)

# -----------------------------
# 9) Read the downloaded monthly files back into R
# -----------------------------
r_list  <- lapply(files_downloaded, rast)
era5_tp <- do.call(c, r_list)
print(era5_tp)

# ERA5 total_precipitation is in metres of water.
# Convert to millimetres if desired:
era5_tp_mm <- era5_tp * 1000
names(era5_tp_mm) <- names(era5_tp)

# -----------------------------
# 10) Optional: extract a point time series for San Juan
# -----------------------------
san_juan <- data.frame(x = -66.1057, y = 18.4655)
pt_vals  <- terra::extract(era5_tp_mm, san_juan)
pt_time  <- terra::time(era5_tp_mm)

san_juan_ts <- data.frame(
  datetime_utc = pt_time,
  tp_mm        = as.numeric(pt_vals[1, -1])
)

head(san_juan_ts)

# Optional: save the time series to CSV
# write.csv(
#   san_juan_ts,
#   file = file.path(out_dir, "san_juan_era5_tp_hourly_2017_mm.csv"),
#   row.names = FALSE
# )

# Optional: save a combined NetCDF after reading everything into R
# writeCDF(
#   era5_tp_mm,
#   filename  = file.path(out_dir, "era5_tp_puerto_rico_2017_mm.nc"),
#   overwrite = TRUE,
#   varname   = "tp_mm",
#   longname  = "ERA5 total precipitation (mm)",
#   unit      = "mm"
# )
