library(tidyverse)
library(sf)

out_panel <- readRDS("Data/out_panel.rds")

make_weekly_geo_grid <- function(
  data,
  fill_var,
  legend_name,
  title,
  subtitle,
  output_filename,
  facet_var = week,
  caption = "Facet labels show the week number",
  facet_ncol = 6,
  color_option = "magma",
  na_value = "grey90",
  border_color = "white",
  border_linewidth = 0.2,
  width = 20,
  height = 18,
  dpi = 300
) {
  fill_var <- rlang::ensym(fill_var)
  facet_var <- rlang::ensym(facet_var)

  fill_name <- rlang::as_string(fill_var)
  facet_name <- rlang::as_string(facet_var)

  missing_vars <- base::setdiff(c(fill_name, facet_name), names(data))
  if (length(missing_vars) > 0) {
    stop(
      "The following variables are missing from `data`: ",
      paste(missing_vars, collapse = ", "),
      call. = FALSE
    )
  }

  if (!inherits(data, "sf")) {
    stop("`data` must be an `sf` object.", call. = FALSE)
  }

  empty_geometry <- sf::st_is_empty(data)
  if (all(empty_geometry)) {
    stop(
      "`data` only contains empty geometries. Rebuild the panel with the correct county polygons before plotting.",
      call. = FALSE
    )
  }

  if (any(empty_geometry)) {
    data <- dplyr::filter(data, !empty_geometry)
  }

  fill_limits <- range(data[[fill_name]], na.rm = TRUE)
  if (!all(is.finite(fill_limits))) {
    fill_limits <- NULL
  }

  plot <- ggplot2::ggplot(data) +
    ggplot2::geom_sf(
      ggplot2::aes(fill = .data[[fill_name]]),
      color = border_color,
      linewidth = border_linewidth
    ) +
    ggplot2::facet_wrap(ggplot2::vars(.data[[facet_name]]), ncol = facet_ncol) +
    ggplot2::scale_fill_viridis_c(
      option = color_option,
      na.value = na_value,
      limits = fill_limits,
      name = legend_name
    ) +
    ggplot2::labs(
      title = title,
      subtitle = subtitle,
      caption = caption
    ) +
    ggplot2::coord_sf(datum = NA, lims_method = "geometry_bbox") +
    ggplot2::theme_void() +
    ggplot2::theme(
      strip.text = ggplot2::element_text(face = "bold", size = 8),
      legend.position = "bottom",
      plot.title.position = "plot"
    )

  output_dir <- dirname(output_filename)
  if (!identical(output_dir, ".") && !dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  }

  ggplot2::ggsave(
    filename = output_filename,
    plot = plot,
    width = width,
    height = height,
    dpi = dpi
  )

  return(plot)
}

wind_weekly_2017_grid <- make_weekly_geo_grid(
  data = filter(out_panel, year == 2017),
  fill_var = max_max_wind_poly,
  legend_name = "Weekly max\nwind",
  title = "U.S. weekly max wind exposure in 2017",
  subtitle = "County-level weekly max wind speeds",
  output_filename = "Output/ibtracs_output/wind_weekly_2017_grid.png"
)

ntl_weekly_2017_grid <- make_weekly_geo_grid(
  data = filter(out_panel, year == 2017),
  fill_var = ntl_mean,
  legend_name = "Weekly mean\nNTL",
  title = "U.S. weekly night lights in 2017",
  subtitle = "County-level weekly mean of Gap-Filled DNB BRDF-Corrected NTL",
  output_filename = "Output/ntl_output/ntl_weekly_2017_grid.png"
)


## Mean wind - Puerto Rico - Weekly - 2017 - Gridplot
mean_wind_pr_weekly_2017_grid <- make_weekly_geo_grid(
  data = filter(out_panel, year == 2017),
  fill_var = mean_wind,
  legend_name = "Weekly mean\nwind",
  title = "Puerto Rico weekly mean wind exposure in 2017",
  subtitle = "County-level weekly mean wind speeds",
  output_filename = "Output/ibtracs_output/mean_wind_pr_weekly_2017_grid.png"
)

## NTL - Puerto Rico - Weekly - 2017 - Gridplot
ntl_pr_weekly_2017_grid <- make_weekly_geo_grid(
  data = filter(out_panel, year == 2017),
  fill_var = ntl_mean,
  legend_name = "Weekly mean\nNTL",
  title = "Puerto Rico weekly night lights in 2017",
  subtitle = "County-level weekly mean of Gap-Filled DNB BRDF-Corrected NTL",
  output_filename = "Output/ntl_output/ntl_pr_weekly_2017_grid.png"
)

## Total Precipitation - Puerto Rico - Weekly - 2017 - Gridplot
prec_pr_weekly_2017_grid <- make_weekly_geo_grid(
  data = filter(out_panel, year == 2017),
  fill_var = precip_total_mm,
  legend_name = "Weekly total precipitation (mm)",
  title = "Puerto Rico weekly total precipitation in 2017",
  subtitle = "County-level weekly total precipitation",
  output_filename = "Output/ibtracs_output/prec_pr_weekly_2017_grid.png"
)

## Mean wind - Puerto Rico - Weekly - 2018 - Gridplot
mean_wind_pr_weekly_2018_grid <- make_weekly_geo_grid(
  data = filter(out_panel, year == 2018),
  fill_var = mean_wind,
  legend_name = "Weekly mean\nwind",
  title = "Puerto Rico weekly mean wind exposure in 2018",
  subtitle = "County-level weekly mean wind speeds",
  output_filename = "Output/ibtracs_output/mean_wind_pr_weekly_2018_grid.png"
)

## NTL - Puerto Rico - Weekly - 2018 - Gridplot
ntl_pr_weekly_2018_grid <- make_weekly_geo_grid(
  data = filter(out_panel, year == 2018),
  fill_var = ntl_mean,
  legend_name = "Weekly mean\nNTL",
  title = "Puerto Rico weekly night lights in 2018",
  subtitle = "County-level weekly mean of Gap-Filled DNB BRDF-Corrected NTL",
  output_filename = "Output/ntl_output/ntl_pr_weekly_2018_grid.png"
)

## Total Precipitation - Puerto Rico - Weekly - 2018 - Gridplot
prec_pr_weekly_2018_grid <- make_weekly_geo_grid(
  data = filter(out_panel, year == 2018),
  fill_var = precip_total_mm,
  legend_name = "Weekly total precipitation (mm)",
  title = "Puerto Rico weekly total precipitation in 2018",
  subtitle = "County-level weekly total precipitation",
  output_filename = "Output/ibtracs_output/prec_pr_weekly_2018_grid.png"
)


#### Florida

## Mean wind - Florida - Weekly - 2017 - Gridplot
mean_wind_pr_weekly_2017_grid <- make_weekly_geo_grid(
  data = filter(out_panel_fl, year == 2017),
  fill_var = mean_wind,
  legend_name = "Weekly mean\nwind",
  title = "Florida weekly mean wind exposure in 2017",
  subtitle = "County-level weekly mean wind speeds",
  output_filename = "Output/ibtracs_output/mean_wind_fl_weekly_2017_grid.png"
)

## NTL - Florida - Weekly - 2017 - Gridplot
ntl_pr_weekly_2017_grid <- make_weekly_geo_grid(
  data = filter(out_panel_fl, year == 2017),
  fill_var = ntl_mean,
  legend_name = "Weekly mean\nNTL",
  title = "Florida weekly night lights in 2017",
  subtitle = "County-level weekly mean of Gap-Filled DNB BRDF-Corrected NTL",
  output_filename = "Output/ntl_output/ntl_fl_weekly_2017_grid.png"
)
