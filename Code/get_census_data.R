get_dhc_data <- function() {
  ## Decennial Census - 2020

  # P -> Population tables (cover total population, race, age distributions, sex by age) = counts
  # H -> Housing tables (housing unit counts, occupancy status, vacancy status, household types)
  # H1, P1, P2, etc. -> Table number
  # _001, _002 -> Line number
  # N -> Suffix for numeric value
  # => P1_001N = total population
  # _001 is always the grand total
  # subsequent numbers are a breakdown of that grand total
  vars_dhc_2020_raw <- tidycensus::load_variables(
    year = 2020,
    dataset = "dhc",
    cache = TRUE
  ) 

  # Filter out all the race specific variables 
  vars_dhc_2020 <- vars_dhc_2020_raw %>%
    filter(!grepl("WHITE|BLACK|ASIAN|RACE|HISPANIC|ALONE", concept, ignore.case = TRUE))

  dhc_census_data_raw <- get_decennial(
    geography = "tract",
    variables = c(
      "H1_001N",   # Total Housing Units
      # Urban and Rural Housing Units
      "H2_001N",  # Total Housing Units - Urban and Rural (check if this is the same as H1_001N)
      "H2_002N",  # Total Housing Units Urban
      "H2_003N",  # Total Housing Units Rural
      "H2_004N",  # Total Housing Units Not defined
      # Household size
      "H9_001N",  # Total Household Size
      # Total Population
      "P1_001N",  # Total Population
      # Urban and Rural Persons
      "P2_001N",  # Total Population - Urban and Rural
      "P2_002N",  # Total Population Urban
      "P2_003N",  # Total Population Rural
      "P2_004N",  # Total Population Urban Rural not defined
      # Sex by Age for selected Age Categories
      "P12_001N", # Total Population
      "P12_002N", # Total Population Male
      "P12_026N", # Total Population Female
      # Median Age by sex
      "P13_001N", # Medien Age both sexes
      "P13_002N", # Median Age Male
      "P13_003N", # Median Age Female
      # Population in Households by Age
      "P15_001N", # Total Population
      "P15_002N", # Population < 18 years
      "P15_003N" # Population >= 18 years
      # Family Type in PCT10
      # Coupled Household Type in PCT13
      ),
    year = 2020,
    state = "PR",
    sumfile = "dhc",
    key = census_key,
    show_call = TRUE
  )

  dhc_census_data <- dhc_census_data_raw %>%
    pivot_wider(
      names_from = variable,
      values_from = value
    )

  return(dhc_census_data)

}

  ## American Community Survey 5-year
get_acs_5_data <- function() {

  vars_acs_2024_raw <- tidycensus::load_variables(
    year = 2024,
    dataset = "acs5",
    cache = TRUE
  )

  # Filter out all the race specific variables 
  vars_acs_2024 <- vars_acs_2024_raw %>%
    filter(!grepl("WHITE|BLACK|ASIAN|RACE|HISPANIC|ALONE", concept, ignore.case = TRUE))

  acs_census_data_raw <- get_acs(
    geography = "tract",
    variables = c(
      # Sex by Age
      "B01001_001", # Estimated Total
      "B01001_002", # Estimated Total Male
      "B01001_026", # Estimated Total Female
      # Median Age
      "B01002_001", # Estimated Total Median Age 
      "B01002_002", # Estimated Median Age Male
      "B01002_003", # Estimated Median Age Female
      # Total Population
      "B01003_001", # Estimated Total
      # Nativity and Citizenship in Puerto Rico
      "B05001PR_001", # Estimate Total
      "B05001PR_002", # Estimate Total U.S. citizen, born in the United States
      "B05001PR_003", # Estimate Total U.S. citizen, born in Puerto Rico or U.S. Island Areas
      "B05001PR_004", # Estimate Total U.S. citizen, born abroad of American parent(s)
      "B05001PR_005", # Estimate Total U.S. citizen by naturalization
      "B05001PR_006", # Estimate Total Not a U.S. citizen
      # Nativity in Puerto Rico
      "B05012PR_001", # Estimated Total
      "B05012PR_002", # Estimated Total Native
      "B05012PR_003", # Estimated Total Foreign-born
      # Place of Birth by Individual Income in the Past 12 Months (in 2024 Inflation-Adjusted Dollars) in Puerto Rico
      "B06010PR_001", # Estimated Total 
      "B06010PR_002", # Estimated Total No Income
      "B06010PR_003", # Estimated Total With Income
      # Median Income in the Past 12 Months (in 2024 Inflation Adjusted Dollars) by Place of Birth in Puerto Rico
      "B06010PR_001", # Estimated Median Income in the past 12 months Total 
      "B06010PR_002", # Estimated Median Income in the past 12 months Total Born in Puerto Rico
      "B06010PR_003", # Estimated Median Income in the past 12 months Total Born in other state of the U.S.
      "B06010PR_004", # Estimated Median Income in the past 12 months Total Native, born elsewhere
      "B06010PR_005", # Estimated Median Income in the past 12 months Total Foreign Born
      # Place of Birth by Poverty Status in the Past 12 Months in Puerto Rico
      "B06012PR_001", # Estimated Total
      "B06012PR_001", # Estimated Total < 100 percent of the poverty level
      "B06012PR_001", # Estimated Total 100 to 149 percent of the poverty level
      "B06012PR_001", # Estimated Total > 150 percent of the poverty level
      # Geographical Mobility in the past year by age for current residence in puerto rico
      "B07001PR_001", # Estimated Total
      "B07001PR_017", # Estimated Total Same house 1 year ago
      "B07001PR_033", # Estimated Total Moved within same municipio
      "B07001PR_049", # Esitmated Total Moved from different municipio
      "B07001PR_065", # Esitmated Total Moved from the United States
      "B07001PR_081", # Esitmated Total Moved from elsewhere
      # Sex of Workers by Vehicles Available
      "B08014_001",   # Estimated Total
      "B08014_002",   # Estimated Total No vehicle available
      "B08014_003",   # Estimated Total 1 vehicle available
      "B08014_004",   # Estimated Total 2 vehicle available
      "B08014_005",   # Estimated Total 3 vehicle available
      "B08014_006",   # Estimated Total 4 vehicle available
      "B08014_007",   # Estimated Total 5 vehicle available
      # Median Household Income in the Past 12 Months (in 2024 Inflation-Adjusted Dollars)
      "B19013_001",   # Estimated Median household income in the past 12 months (in 2024 inflation-adjusted dollars)
      # Aggregate Household Income in the Past 12 Months (in 2024 Inflation-Adjusted Dollars)
      "B19025_001",   # Estimated Aggregate Household Income in the Past 12 Months (in 2024 Inflation-Adjusted Dollars)
      # Aggregate Earnings in the Past 12 Months (in 2024 Inflation-Adjusted Dollars) for Households
      "B19061_001",   # Estimate Aggregate earnings in the past 12 months (in 2024 inflation-adjusted dollars)
      # Aggregate Wage or Salary Income in the Past 12 Months (in 2024 Inflation-Adjusted Dollars) for Households
      "B19062_001",   # Estimated Aggregate wage or salary income in the past 12 months (in 2024 inflation-adjusted dollars)
      # Aggregate Self-Employment Income in the Past 12 Months (in 2024 Inflation-Adjusted Dollars) for Households
      "B19063_001",   # Estimated Aggregate self-employment income in the past 12 months (in 2024 inflation-adjusted dollars)
      # Aggregate Interest, Dividends, or Net Rental Income in the Past 12 Months (in 2024 Inflation-Adjusted Dollars) for Households
      "B19064_001",   # Estimated Aggregate interest, dividends, or net rental income in the past 12 months (in 2024 inflation-adjusted dollars)
      # Aggregate Social Security Income in the Past 12 Months (in 2024 Inflation-Adjusted Dollars) for Households
      "B19065_001",   # Estimated Aggregate Social Security income in the past 12 months (in 2024 inflation-adjusted dollars)
      # Aggregate Supplemental Security Income (SSI) in the Past 12 Months (in 2024 Inflation-Adjusted Dollars) for Households
      "B19066_001",   # Estimated Aggregate Supplemental Security Income (SSI) in the past 12 months (in 2024 inflation-adjusted dollars)
      # Aggregate Public Assistance Income in the Past 12 Months (in 2024 Inflation-Adjusted Dollars) for Households
      "B19067_001",   # Estimated Aggregate public assistance income in the past 12 months (in 2024 inflation-adjusted dollars)
      # Aggregate Retirement Income in the Past 12 Months (in 2024 Inflation-Adjusted Dollars) for Households
      "B19069_001",   # Estimated Aggregate retirement income in the past 12 months (in 2024 inflation-adjusted dollars)
      # Aggregate Other Types of Income in the Past 12 Months (in 2024 Inflation-Adjusted Dollars) for Households
      "B19070_001",   # Estimated Aggregate other types of income in the past 12 months (in 2024 inflation-adjusted dollars):
      # Gini Index of Income Inequality
      "B19083_001",   # Estimated Gini Index
      # Per Capita Income in the Past 12 Months (in 2024 Inflation-Adjusted Dollars)
      "B19301_001",   # Estimated Per capita income in the past 12 months (in 2024 inflation-adjusted dollars)
      # Aggregate Income in the Past 12 Months (in 2024 Inflation-Adjusted Dollars)
      "B19313_001",   # Estimated Aggregate income in the past 12 months (in 2024 inflation-adjusted dollars)
      # Median Gross Rent (Dollars)
      "B25064_001"    # Estimated median Gross Rent
    ),
    year = 2024,
    state = "PR",
    geometry = FALSE,
    keep_geo_vars = FALSE,
    key = census_key,
    survey = "acs5",
    show_call = TRUE
  )

  acs_census_data <- acs_census_data_raw %>%
  pivot_wider(
    names_from = variable,
    values_from = c(estimate, moe)
  )

  return(acs_census_data)

}

get_geoinfo <- function(year = 2024,
                        vars = c("GEO_ID", "NAME", "SUMLEVEL",
                                 "INTPTLAT", "INTPTLON",
                                 "AREALAND_SQMI", "AREAWATR_SQMI"),
                        for_clause,
                        in_clause = NULL,
                        key = Sys.getenv("CENSUS_API_KEY")) {

  req <- request(sprintf("https://api.census.gov/data/%s/geoinfo", year)) %>%
    req_url_query(
      get = paste(vars, collapse = ","),
      `for` = for_clause
    )

  if (!is.null(in_clause)) {
    req <- req %>% req_url_query(`in` = in_clause)
  }

  if (nzchar(key)) {
    req <- req %>% req_url_query(key = key)
  }

  x <- req %>%
    req_perform() %>%
    resp_body_json(simplifyVector = TRUE)

  out <- as.data.frame(x[-1, , drop = FALSE], stringsAsFactors = FALSE)
  names(out) <- x[1, ]
  as_tibble(out)
}


get_acs_data <- function() {

  vars_acs_2024_raw <- tidycensus::load_variables(
    year = 2024,
    dataset = "acs1",
    cache = TRUE
  )

  # Filter out all the race specific variables 
  vars_acs_2024 <- vars_acs_2024_raw %>%
    filter(!grepl("WHITE|BLACK|ASIAN|RACE|HISPANIC|ALONE", concept, ignore.case = TRUE))

  acs_census_data_raw <- get_acs(
    geography = "county",
    year = 2023,
    variables = c("B01003_001"),
    state = "FL",
    geometry = FALSE,
    keep_geo_vars = FALSE,
    key = census_key,
    survey = "acs1",
    show_call = TRUE
  )

  acs_census_data <- acs_census_data_raw %>%
  pivot_wider(
    names_from = variable,
    values_from = c(estimate, moe)
  )

  return(acs_census_data)

}

get_pep_county_panel <- function() {

  # ==============================================================================
  # 0. LOAD PACKAGES
  # ==============================================================================
  
  required_pkgs <- c("tidyverse", "readxl", "janitor", "httr")
  
  for (pkg in required_pkgs) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      install.packages(pkg, repos = "https://cloud.r-project.org")
    }
    library(pkg, character.only = TRUE)
  }
  
  # ==============================================================================
  # 1. DOWNLOAD FILES
  # ==============================================================================

  ## URLs
  urls <- c(
    # CO-EST2020-ALLDATA
    co_est2020   = "https://www2.census.gov/programs-surveys/popest/datasets/2010-2020/counties/totals/co-est2020-alldata.csv",
    # CO-EST2025-alldata -> Annual Resident Population Estimates, Estimated Components of Resident Population Change, and Rates of the Components of Resident Population Change for States and Counties: April 1, 2020 to July 1, 2025
    co_est2025   = "https://www2.census.gov/programs-surveys/popest/datasets/2020-2025/counties/totals/co-est2025-alldata.csv",
    # CO-EST2020INT-POP -> Intercensal Estimates of the Resident Population for Counties: April 1, 2010 to April 1, 2020
    int_pop      = "https://www2.census.gov/programs-surveys/popest/tables/2010-2020/intercensal/county/co-est2020int-pop.xlsx",
    # CO-EST2020INT-HU -> Intercensal Estimates of Housing Units for Counties in the United States: April 1, 2010 to April 1, 2020
    int_hu       = "https://www2.census.gov/programs-surveys/popest/tables/2010-2020/intercensal/housing/co-est2020int-hu.xlsx",
    # CO-EST2024-HU -> Annual Estimates of Housing Units for Counties in the United States: April 1, 2020 to July 1, 2024
    hu_2024      = "https://www2.census.gov/programs-surveys/popest/tables/2020-2024/housing/totals/CO-EST2024-HU.xlsx",
    # CC-EST2020INT-AGESEX -> Annual Intercensal County and Puerto Rico Municipio Resident Population Estimates by Selected Age Groups and Sex: April 1, 2010 to April 1, 2020
    int_agesex   = "https://www2.census.gov/programs-surveys/popest/datasets/2010-2020/intercensal/county/asrh/cc-est2020int-agesex-all.csv",
    # CC-EST2020INT-ALLDATA -> Annual Intercensal County Resident Population Estimates by Age, Sex, Race, and Hispanic Origin: April 1, 2010 to April 1, 2020
    int_alldata  = "https://www2.census.gov/programs-surveys/popest/datasets/2010-2020/intercensal/county/asrh/cc-est2020int-alldata.csv",
    # CC-EST2024-AGESEX -> Annual County and Puerto Rico Municipio Resident Population Estimates by Selected Age Groups and Sex: April 1, 2020 to July 1, 2024
    cc_agesex24  = "https://www2.census.gov/programs-surveys/popest/datasets/2020-2024/counties/asrh/cc-est2024-agesex-all.csv",
    # CC-EST2024-ALLDATA -> Annual County Resident Population Estimates by Age, Sex, Race, and Hispanic Origin: April 1, 2020 to July 1, 2024
    cc_alldata24 = "https://www2.census.gov/programs-surveys/popest/datasets/2020-2024/counties/asrh/cc-est2024-alldata.csv",
    # PRM-EST2020INT-POP-72 -> Intercensal Estimates of the Resident Population for Puerto Rico Municipios: April 1, 2010 to April 1, 2020
    pr_int_pop   = "https://www2.census.gov/programs-surveys/popest/tables/2010-2020/intercensal/municipios/prm-est2020int-pop-72.xlsx",
    # CC-EST2020INT-AGESEX-72 -> Annual Intercensal Puerto Rico Municipio Resident Population Estimates by Selected Age Groups and Sex: April 1, 2010 to April 1, 2020
    pr_int_agesex = "https://www2.census.gov/programs-surveys/popest/datasets/2010-2020/intercensal/county/asrh/cc-est2020int-agesex-72.csv",
    # CC-EST2024-AGESEX-72 -> Annual Puerto Rico Municipio Resident Population Estimates by Selected Age Groups and Sex: April 1, 2020 to July 1, 2024
    pr_agesex24  = "https://www2.census.gov/programs-surveys/popest/datasets/2020-2024/counties/asrh/cc-est2024-agesex-72.csv",
    # PRM-EST2024-POP -> Annual Estimates of the Resident Population for Puerto Rico Municipios: April 1, 2020 to July 1, 2024
    pr_pop24     = "https://www2.census.gov/programs-surveys/popest/tables/2020-2024/municipios/totals/prm-est2024-pop.xlsx"
  )
  
  raw_dir <- file.path(getwd(), "Data/pep_files")
  dir.create(raw_dir, showWarnings = FALSE, recursive = TRUE)
  
  local_paths <- setNames(
    file.path(raw_dir, basename(urls)),
    names(urls)
  )
  
  cat("--- Downloading files ---\n")
  for (nm in names(urls)) {
    dest <- local_paths[[nm]]
    if (!file.exists(dest)) {
      cat("  Downloading:", nm, "\n")
      tryCatch({
        download.file(urls[[nm]], destfile = dest, mode = "wb", quiet = TRUE)
      }, error = function(e) {
        warning("  Failed to download ", nm, ": ", conditionMessage(e))
      })
    } else {
      cat("  Already exists:", nm, "\n")
    }
  }
  
  
  # ==============================================================================
  # 2. HELPER FUNCTIONS
  # ==============================================================================
  
  #' Build a 5-character FIPS code from state + county FIPS components
  make_fips <- function(state, county) {
    sprintf("%02d%03d", as.integer(state), as.integer(county))
  }
  
  #' Read a Census-formatted XLSX "table" file (population or housing).
  #' These files have 3-4 header rows, with "Geographic Area" in col 1 and
  #' yearly estimates across columns. The function auto-detects the header row,
  #' then parses state/county FIPS from the geographic area name column.
  read_census_xlsx_table <- function(path, value_name = "value") {
  
    # Read all sheets — Census tables sometimes put data in the first or only sheet
    sheets <- readxl::excel_sheets(path)
  
    # Try to read the first sheet and find the header row
    raw <- readxl::read_excel(
      path,
      sheet = sheets[1],
      col_names = FALSE,
      col_types = "text",
      .name_repair = "minimal"
    )
  
    # Detect the header row from the first column, which should contain
    # "Geographic Area" for these Census workbook tables.
    header_row <- NA
    for (i in seq_len(min(10, nrow(raw)))) {
      first_cell <- raw[[1]][i]
      if (!is.na(first_cell) && grepl("^geographic area$", trimws(first_cell), ignore.case = TRUE)) {
        header_row <- i
        break
      }
    }
  
    if (is.na(header_row)) {
      warning("Could not detect header row in: ", path)
      return(tibble::tibble())
    }
  
    header_top <- as.character(unlist(raw[header_row, ], use.names = FALSE))
    header_bottom <- if (header_row < nrow(raw)) {
      as.character(unlist(raw[header_row + 1, ], use.names = FALSE))
    } else {
      rep(NA_character_, ncol(raw))
    }

    for (j in seq_along(header_top)) {
      if (j > 1 && (is.na(header_top[j]) || header_top[j] == "")) {
        header_top[j] <- header_top[j - 1]
      }
    }

    combined_names <- vapply(seq_len(ncol(raw)), function(j) {
      if (j == 1) {
        return("geographic_area")
      }

      parts <- c(header_top[j], header_bottom[j])
      parts <- parts[!is.na(parts) & nzchar(parts)]

      if (length(parts) == 0) {
        paste0("col_", j)
      } else {
        paste(parts, collapse = " ")
      }
    }, character(1))

    df <- raw[(header_row + 2):nrow(raw), , drop = FALSE]
    names(df) <- make.unique(combined_names)
  
    # Remove completely empty rows and footnote rows
    df <- df %>%
      filter(!is.na(geographic_area), geographic_area != "") %>%
      filter(!grepl("^(Note|Source|Copyright|Suggested|Release)", geographic_area, ignore.case = TRUE))
  
    # Clean column names: extract year from column headers like "July 1, 2015" or "April 1, 2010"
    # or just "2015" etc.
    orig_names <- names(df)
    new_names  <- orig_names
  
    for (j in 2:length(orig_names)) {
      col_text <- orig_names[j]
      # Try to extract a 4-digit year
      yr_match <- regmatches(col_text, regexpr("\\d{4}", col_text))
      if (length(yr_match) == 1) {
        # Determine if it's an April (Census/base) or July (estimate) column
        if (grepl("April", col_text, ignore.case = TRUE)) {
          # Determine if it's the Census count or estimates base
          if (grepl("Census", col_text, ignore.case = TRUE)) {
            new_names[j] <- paste0("census_", yr_match)
          } else if (grepl("Base|base", col_text, ignore.case = TRUE)) {
            new_names[j] <- paste0("base_", yr_match)
          } else {
            new_names[j] <- paste0("apr_", yr_match)
          }
        } else if (grepl("July", col_text, ignore.case = TRUE)) {
          new_names[j] <- paste0("jul_", yr_match)
        } else {
          new_names[j] <- paste0("col_", yr_match)
        }
      }
    }
    names(df) <- make.unique(new_names)
  
    return(df)
  }
  
  #' Parse geographic area strings from XLSX tables to extract state/county FIPS.
  #' The typical format is ".CountyName, StateName" where the leading period
  #' indicates a county-level record. State-level rows have no leading period.
  parse_geo_area <- function(df) {
    df %>%
      mutate(
        # County rows typically start with a period "."
        is_county = grepl("^\\.", geographic_area),
        # Clean the area name
        geo_clean = trimws(gsub("^\\.", "", geographic_area))
      ) %>%
      filter(is_county) %>%
      select(-is_county)
  }
  
  #' Pivot a Census XLSX table from wide (one column per year) to long format,
  #' keeping only July 1 estimate columns.
  pivot_xlsx_long <- function(df, value_name = "value") {
    # Select only the July columns and the geographic area
    jul_cols <- grep("^jul_\\d{4}$", names(df), value = TRUE)
  
    if (length(jul_cols) == 0) {
      # Fallback: try col_ prefixed columns
      jul_cols <- grep("^col_\\d{4}$", names(df), value = TRUE)
    }
  
    if (length(jul_cols) == 0) {
      warning("No year columns found to pivot.")
      return(tibble::tibble())
    }
  
    df %>%
      select(geographic_area, geo_clean, all_of(jul_cols)) %>%
      pivot_longer(
        cols      = all_of(jul_cols),
        names_to  = "year_col",
        values_to = value_name
      ) %>%
      mutate(
        year       = as.integer(gsub("^(jul_|col_)", "", year_col)),
        !!value_name := as.numeric(!!rlang::sym(value_name))
      ) %>%
      select(-year_col)
  }

  # ==============================================================================
  # 3. EXTRACT & CLEAN EACH FILE
  # ==============================================================================

  cat("\n--- Extracting and cleaning data ---\n")

  # --------------------------------------------------------------------------
  # 3A. CO-EST2020-ALLDATA (Vintage 2020 components of change, 2010-2020)
  # --------------------------------------------------------------------------
  cat("  3A. co-est2020-alldata.csv\n")

  co2020 <- read_csv(local_paths[["co_est2020"]], show_col_types = FALSE,
                      locale = locale(encoding = "latin1")) %>%
    clean_names() %>%
    filter(sumlev == "050") %>%                         # counties only

  mutate(fips = make_fips(state, county))

  # Pivot population estimates to long
  co2020_pop <- co2020 %>%
    select(fips, state, county, stname, ctyname,
           matches("^popestimate\\d{4}$")) %>%
    pivot_longer(
      cols      = matches("^popestimate\\d{4}$"),
      names_to  = "year_col",
      values_to = "pop_vintage2020"
    ) %>%
    mutate(year = as.integer(gsub("popestimate", "", year_col))) %>%
    select(-year_col)

  # Pivot components of change to long
  component_vars <- c("births", "deaths", "naturalinc", "internationalmig",
                       "domesticmig", "netmig", "residual")

  co2020_comp_list <- purrr::map(component_vars, function(var) {
    cols <- paste0(var, 2010:2020)
    cols_present <- intersect(cols, names(co2020))

    if (length(cols_present) == 0) return(tibble::tibble())

    co2020 %>%
      select(fips, all_of(cols_present)) %>%
      pivot_longer(
        cols      = all_of(cols_present),
        names_to  = "year_col",
        values_to = var
      ) %>%
      mutate(year = as.integer(gsub(var, "", year_col))) %>%
      select(-year_col)
  })

  co2020_comp <- purrr::reduce(co2020_comp_list, full_join, by = c("fips", "year"))

  # --------------------------------------------------------------------------
  # 3B. CO-EST2025-ALLDATA (Vintage 2025 population + components, 2020-2025)
  # --------------------------------------------------------------------------
  cat("  3B. co-est2025-alldata.csv\n")

  co2025 <- read_csv(local_paths[["co_est2025"]], show_col_types = FALSE,
                      locale = locale(encoding = "latin1")) %>%
    clean_names() %>%
    filter(sumlev == "050") %>%
    mutate(fips = make_fips(state, county))

  co2025_pop <- co2025 %>%
    select(fips, state, county, stname, ctyname,
           matches("^popestimate\\d{4}$")) %>%
    pivot_longer(
      cols      = matches("^popestimate\\d{4}$"),
      names_to  = "year_col",
      values_to = "pop_postcensal"
    ) %>%
    mutate(year = as.integer(gsub("popestimate", "", year_col))) %>%
    select(-year_col)

  # Components for the 2020s decade
  # Note: The 2020s series uses "naturalchg" instead of "naturalinc"
  component_vars_2020s <- c("births", "deaths", "naturalchg", "internationalmig",
                            "domesticmig", "netmig", "residual")

  co2025_comp_list <- purrr::map(component_vars_2020s, function(var) {
    possible_years <- 2020:2025
    cols <- paste0(var, possible_years)
    cols_present <- intersect(cols, names(co2025))

    if (length(cols_present) == 0) return(tibble::tibble())

    co2025 %>%
      select(fips, all_of(cols_present)) %>%
      pivot_longer(
        cols      = all_of(cols_present),
        names_to  = "year_col",
        values_to = var
      ) %>%
      mutate(year = as.integer(gsub(var, "", year_col))) %>%
      select(-year_col)
  })

  co2025_comp <- purrr::reduce(co2025_comp_list, full_join, by = c("fips", "year"))

  # Harmonize the natural increase/change column name
  if ("naturalinc" %in% names(co2020_comp) && !"naturalchg" %in% names(co2020_comp)) {
    co2020_comp <- co2020_comp %>% rename(naturalchg = naturalinc)
  }

  # --------------------------------------------------------------------------
  # 3C. CO-EST2020INT-POP.XLSX (Intercensal population, 2010-2020)
  # --------------------------------------------------------------------------
  cat("  3C. co-est2020int-pop.xlsx\n")

  tryCatch({
    int_pop_raw <- read_census_xlsx_table(local_paths[["int_pop"]])

    if (nrow(int_pop_raw) > 0) {
      int_pop_counties <- parse_geo_area(int_pop_raw)
      int_pop_long     <- pivot_xlsx_long(int_pop_counties, value_name = "pop_intercensal")

      # We need to match these to FIPS codes.
      # Strategy: Join on county+state names from the CO-EST2020 file.
      county_xwalk <- co2020 %>%
        select(fips, stname, ctyname) %>%
        distinct() %>%
        mutate(geo_clean = paste0(ctyname, ", ", stname))

      int_pop_long <- int_pop_long %>%
        left_join(county_xwalk, by = "geo_clean") %>%
        filter(!is.na(fips)) %>%
        select(fips, year, pop_intercensal)
    } else {
      int_pop_long <- tibble(fips = character(), year = integer(), pop_intercensal = numeric())
    }
  }, error = function(e) {
    warning("  Could not process intercensal population xlsx: ", conditionMessage(e))
    int_pop_long <<- tibble(fips = character(), year = integer(), pop_intercensal = numeric())
  })

  # --------------------------------------------------------------------------
  # 3D. CO-EST2020INT-HU.XLSX (Intercensal housing units, 2010-2020)
  # --------------------------------------------------------------------------
  cat("  3D. co-est2020int-hu.xlsx\n")

  tryCatch({
    int_hu_raw <- read_census_xlsx_table(local_paths[["int_hu"]])

    if (nrow(int_hu_raw) > 0) {
      int_hu_counties <- parse_geo_area(int_hu_raw)
      int_hu_long     <- pivot_xlsx_long(int_hu_counties, value_name = "housing_units_intercensal")

      county_xwalk <- co2020 %>%
        select(fips, stname, ctyname) %>%
        distinct() %>%
        mutate(geo_clean = paste0(ctyname, ", ", stname))

      int_hu_long <- int_hu_long %>%
        left_join(county_xwalk, by = "geo_clean") %>%
        filter(!is.na(fips)) %>%
        select(fips, year, housing_units_intercensal)
    } else {
      int_hu_long <- tibble(fips = character(), year = integer(),
                            housing_units_intercensal = numeric())
    }
  }, error = function(e) {
    warning("  Could not process intercensal housing xlsx: ", conditionMessage(e))
    int_hu_long <<- tibble(fips = character(), year = integer(),
                           housing_units_intercensal = numeric())
  })

  # --------------------------------------------------------------------------
  # 3E. CO-EST2024-HU.XLSX (Housing units, 2020-2024)
  # --------------------------------------------------------------------------
  cat("  3E. CO-EST2024-HU.xlsx\n")

  tryCatch({
    hu2024_raw <- read_census_xlsx_table(local_paths[["hu_2024"]])

    if (nrow(hu2024_raw) > 0) {
      hu2024_counties <- parse_geo_area(hu2024_raw)
      hu2024_long     <- pivot_xlsx_long(hu2024_counties, value_name = "housing_units_postcensal")

      county_xwalk <- co2025 %>%
        select(fips, stname, ctyname) %>%
        distinct() %>%
        mutate(geo_clean = paste0(ctyname, ", ", stname))

      hu2024_long <- hu2024_long %>%
        left_join(county_xwalk, by = "geo_clean") %>%
        filter(!is.na(fips)) %>%
        select(fips, year, housing_units_postcensal)
    } else {
      hu2024_long <- tibble(fips = character(), year = integer(),
                            housing_units_postcensal = numeric())
    }
  }, error = function(e) {
    warning("  Could not process 2024 housing xlsx: ", conditionMessage(e))
    hu2024_long <<- tibble(fips = character(), year = integer(),
                           housing_units_postcensal = numeric())
  })

  # --------------------------------------------------------------------------
  # 3F. CC-EST2020INT-AGESEX-ALL.CSV (Intercensal age/sex, 2010-2020)
  # --------------------------------------------------------------------------
  cat("  3F. cc-est2020int-agesex-all.csv\n")

  # This file has columns: SUMLEV, STATE, COUNTY, STNAME, CTYNAME, YEAR, AGEGRP,
  # TOT_POP, TOT_MALE, TOT_FEMALE, plus additional age-group columns.
  # YEAR coding for intercensal CC files:
  #   1 = 4/1/2010 population estimates base
  #   2 = 7/1/2010 population estimate
  #   ...
  #   11 = 7/1/2019 population estimate
  #   12 = 4/1/2020 Census population

  tryCatch({
    cc_int_agesex <- read_csv(local_paths[["int_agesex"]], show_col_types = FALSE,
                              locale = locale(encoding = "latin1")) %>%
      clean_names()

    # This file is already at the county-year total level.
    cc_int_agesex <- cc_int_agesex %>%
      filter(sumlev == "050" | sumlev == 50) %>%
      mutate(
        fips   = make_fips(state, county),
        year_code = as.integer(year)
      ) %>%
      rename(
        year_orig = year,
        tot_pop = popestimate,
        tot_male = popest_male,
        tot_female = popest_fem
      )

    # Decode YEAR to calendar year using the Census intercensal file layout.
    # We keep only July 1 estimates, so the 4/1/2010 base and 4/1/2020 Census
    # rows are dropped.
    max_yc <- max(cc_int_agesex$year_code, na.rm = TRUE)
    if (max_yc == 12) {
      year_decode_int <- tibble::tibble(
        year_code = 1:12,
        year      = c(NA_integer_, 2010L:2019L, NA_integer_)
      )
    } else {
      stop("Unexpected YEAR coding in cc-est2020int-agesex-all.csv")
    }

    cc_int_agesex <- cc_int_agesex %>%
      left_join(year_decode_int, by = "year_code") %>%
      filter(!is.na(year))  # drop Census/base rows

    cat("    Intercensal agesex year range:", range(cc_int_agesex$year), "\n")

  }, error = function(e) {
    warning("  Could not process intercensal agesex csv: ", conditionMessage(e))
    cc_int_agesex <<- tibble::tibble()
  })

  # --------------------------------------------------------------------------
  # 3G. CC-EST2020INT-ALLDATA.CSV (Intercensal full race/ethnicity, 2010-2020)
  # --------------------------------------------------------------------------
  cat("  3G. cc-est2020int-alldata.csv\n")

  tryCatch({
    cc_int_alldata <- read_csv(local_paths[["int_alldata"]], show_col_types = FALSE,
                               locale = locale(encoding = "latin1")) %>%
      clean_names()

    cc_int_alldata <- cc_int_alldata %>%
      filter(sumlev == "050" | sumlev == 50) %>%
      mutate(
        fips      = make_fips(state, county),
        year_code = as.integer(year)
      )

    max_yc <- max(cc_int_alldata$year_code, na.rm = TRUE)
    if (max_yc == 12) {
      year_decode <- tibble::tibble(
        year_code = 1:12,
        year      = c(NA_integer_, 2010L:2019L, NA_integer_)
      )
    } else {
      stop("Unexpected YEAR coding in cc-est2020int-alldata.csv")
    }

    cc_int_alldata <- cc_int_alldata %>%
      rename(year_orig = year) %>%
      left_join(year_decode, by = "year_code") %>%
      filter(!is.na(year))

    cat("    Intercensal alldata year range:", range(cc_int_alldata$year), "\n")

  }, error = function(e) {
    warning("  Could not process intercensal alldata csv: ", conditionMessage(e))
    cc_int_alldata <<- tibble::tibble()
  })

  # --------------------------------------------------------------------------
  # 3H. CC-EST2024-AGESEX-ALL.CSV (Postcensal age/sex, 2020-2024)
  # --------------------------------------------------------------------------
  cat("  3H. cc-est2024-agesex-all.csv\n")

  # YEAR coding for CC-EST2024:
  #   1 = 4/1/2020 estimates base
  #   2 = 7/1/2020
  #   3 = 7/1/2021
  #   4 = 7/1/2022
  #   5 = 7/1/2023
  #   6 = 7/1/2024

  year_decode_2024 <- tibble::tibble(
    year_code = 1:6,
    year      = c(NA_integer_, 2020L:2024L)
  )

  tryCatch({
    cc24_agesex <- read_csv(local_paths[["cc_agesex24"]], show_col_types = FALSE,
                            locale = locale(encoding = "latin1")) %>%
      clean_names()

    cc24_agesex <- cc24_agesex %>%
      filter(sumlev == "050" | sumlev == 50) %>%
      mutate(
        fips      = make_fips(state, county),
        year_code = as.integer(year)
      ) %>%
      rename(
        year_orig = year,
        tot_pop = popestimate,
        tot_male = popest_male,
        tot_female = popest_fem
      ) %>%
      left_join(year_decode_2024, by = "year_code") %>%
      filter(!is.na(year))

    cat("    Postcensal agesex year range:", range(cc24_agesex$year), "\n")

  }, error = function(e) {
    warning("  Could not process cc-est2024-agesex csv: ", conditionMessage(e))
    cc24_agesex <<- tibble::tibble()
  })

  # --------------------------------------------------------------------------
  # 3I. CC-EST2024-ALLDATA.CSV (Postcensal full race/ethnicity, 2020-2024)
  # --------------------------------------------------------------------------
  cat("  3I. cc-est2024-alldata.csv\n")

  tryCatch({
    cc24_alldata <- read_csv(local_paths[["cc_alldata24"]], show_col_types = FALSE,
                             locale = locale(encoding = "latin1")) %>%
      clean_names()

    cc24_alldata <- cc24_alldata %>%
      filter(sumlev == "050" | sumlev == 50) %>%
      mutate(
        fips      = make_fips(state, county),
        year_code = as.integer(year)
      ) %>%
      rename(year_orig = year) %>%
      left_join(year_decode_2024, by = "year_code") %>%
      filter(!is.na(year))

    cat("    Postcensal alldata year range:", range(cc24_alldata$year), "\n")

  }, error = function(e) {
    warning("  Could not process cc-est2024-alldata csv: ", conditionMessage(e))
    cc24_alldata <<- tibble::tibble()
  })

  # --------------------------------------------------------------------------
  # 3J. CC-EST2020INT-AGESEX-72.CSV (Puerto Rico intercensal age/sex, 2010-2020)
  # --------------------------------------------------------------------------
  cat("  3J. cc-est2020int-agesex-72.csv\n")

  tryCatch({
    pr_int_agesex_raw <- read_csv(local_paths[["pr_int_agesex"]], show_col_types = FALSE,
                                   locale = locale(encoding = "latin1")) %>%
      clean_names()

    pr_int_agesex_raw <- pr_int_agesex_raw %>%
      filter(sumlev == "050" | sumlev == 50 |
             sumlev == "162" | sumlev == 162) %>%
      mutate(
        state     = "72",
        county    = sprintf("%03d", as.integer(municipio)),
        stname    = "Puerto Rico",
        ctyname   = name,
        fips      = make_fips(state, county),
        year_code = as.integer(year)
      ) %>%
      rename(
        year_orig  = year,
        tot_pop    = popestimate,
        tot_male   = popest_male,
        tot_female = popest_fem
      )

    # Decode YEAR to calendar year using the Census intercensal file layout.
    # We keep only July 1 estimates, so the 4/1/2010 base and 4/1/2020 Census
    # rows are dropped.
    max_yc_pr <- max(pr_int_agesex_raw$year_code, na.rm = TRUE)
    if (max_yc_pr == 12) {
      year_decode_pr_int <- tibble::tibble(
        year_code = 1:12,
        year      = c(NA_integer_, 2010L:2019L, NA_integer_)
      )
    } else {
      stop("Unexpected YEAR coding in cc-est2020int-agesex-72.csv")
    }

    pr_int_agesex_raw <- pr_int_agesex_raw %>%
      left_join(year_decode_pr_int, by = "year_code") %>%
      filter(!is.na(year))

    cat("    PR intercensal agesex year range:", range(pr_int_agesex_raw$year), "\n")

  }, error = function(e) {
    warning("  Could not process PR intercensal agesex csv: ", conditionMessage(e))
    pr_int_agesex_raw <<- tibble::tibble()
  })

  # --------------------------------------------------------------------------
  # 3K. CC-EST2024-AGESEX-72.CSV (Puerto Rico postcensal age/sex, 2020-2024)
  # --------------------------------------------------------------------------
  cat("  3K. cc-est2024-agesex-72.csv\n")

  tryCatch({
    pr24_agesex <- read_csv(local_paths[["pr_agesex24"]], show_col_types = FALSE,
                             locale = locale(encoding = "latin1")) %>%
      clean_names()

    pr24_agesex <- pr24_agesex %>%
      filter(sumlev == "050" | sumlev == 50 |
             sumlev == "162" | sumlev == 162) %>%
      mutate(
        state     = "72",
        county    = sprintf("%03d", as.integer(county)),
        stname    = "Puerto Rico",
        fips      = make_fips(state, county),
        year_code = as.integer(year)
      ) %>%
      rename(
        year_orig  = year,
        tot_pop    = popestimate,
        tot_male   = popest_male,
        tot_female = popest_fem
      ) %>%
      left_join(year_decode_2024, by = "year_code") %>%
      filter(!is.na(year))

    cat("    PR postcensal agesex year range:", range(pr24_agesex$year), "\n")

  }, error = function(e) {
    warning("  Could not process PR cc-est2024-agesex csv: ", conditionMessage(e))
    pr24_agesex <<- tibble::tibble()
  })

  # --------------------------------------------------------------------------
  # 3L. PRM-EST2020INT-POP-72.XLSX (Puerto Rico intercensal population, 2010-2020)
  # --------------------------------------------------------------------------
  cat("  3L. prm-est2020int-pop-72.xlsx\n")

  tryCatch({
    pr_int_pop_raw <- read_census_xlsx_table(local_paths[["pr_int_pop"]])

    if (nrow(pr_int_pop_raw) > 0) {
      pr_int_pop_counties <- parse_geo_area(pr_int_pop_raw)
      pr_int_pop_long     <- pivot_xlsx_long(pr_int_pop_counties, value_name = "pop_intercensal")

      # Build PR FIPS crosswalk from the PR agesex CSV (which has STATE, COUNTY, STNAME, CTYNAME)
      pr_xwalk <- if (exists("pr_int_agesex_raw") && nrow(pr_int_agesex_raw) > 0) {
        pr_int_agesex_raw %>%
          select(fips, stname, ctyname) %>%
          distinct() %>%
          mutate(geo_clean = paste0(ctyname, ", ", stname))
      } else if (exists("pr24_agesex") && nrow(pr24_agesex) > 0) {
        pr24_agesex %>%
          select(fips, stname, ctyname) %>%
          distinct() %>%
          mutate(geo_clean = paste0(ctyname, ", ", stname))
      } else {
        tibble::tibble(fips = character(), stname = character(),
                       ctyname = character(), geo_clean = character())
      }

      pr_int_pop_long <- pr_int_pop_long %>%
        left_join(pr_xwalk, by = "geo_clean") %>%
        filter(!is.na(fips)) %>%
        select(fips, year, pop_intercensal)

      cat("    PR intercensal pop rows:", nrow(pr_int_pop_long), "\n")
    } else {
      pr_int_pop_long <- tibble(fips = character(), year = integer(),
                                pop_intercensal = numeric())
    }
  }, error = function(e) {
    warning("  Could not process PR intercensal pop xlsx: ", conditionMessage(e))
    pr_int_pop_long <<- tibble(fips = character(), year = integer(),
                               pop_intercensal = numeric())
  })

  # --------------------------------------------------------------------------
  # 3M. PRM-EST2024-POP.XLSX (Puerto Rico postcensal population, 2020-2024)
  # --------------------------------------------------------------------------
  cat("  3M. prm-est2024-pop.xlsx\n")

  tryCatch({
    pr_pop24_raw <- read_census_xlsx_table(local_paths[["pr_pop24"]])

    if (nrow(pr_pop24_raw) > 0) {
      pr_pop24_counties <- parse_geo_area(pr_pop24_raw)
      pr_pop24_long     <- pivot_xlsx_long(pr_pop24_counties, value_name = "pop_postcensal")

      # Reuse the PR FIPS crosswalk
      pr_xwalk24 <- if (exists("pr24_agesex") && nrow(pr24_agesex) > 0) {
        pr24_agesex %>%
          select(fips, stname, ctyname) %>%
          distinct() %>%
          mutate(geo_clean = paste0(ctyname, ", ", stname))
      } else if (exists("pr_int_agesex_raw") && nrow(pr_int_agesex_raw) > 0) {
        pr_int_agesex_raw %>%
          select(fips, stname, ctyname) %>%
          distinct() %>%
          mutate(geo_clean = paste0(ctyname, ", ", stname))
      } else {
        tibble::tibble(fips = character(), stname = character(),
                       ctyname = character(), geo_clean = character())
      }

      pr_pop24_long <- pr_pop24_long %>%
        left_join(pr_xwalk24, by = "geo_clean") %>%
        filter(!is.na(fips)) %>%
        select(fips, year, pop_postcensal)

      cat("    PR postcensal pop rows:", nrow(pr_pop24_long), "\n")
    } else {
      pr_pop24_long <- tibble(fips = character(), year = integer(),
                              pop_postcensal = numeric())
    }
  }, error = function(e) {
    warning("  Could not process PR postcensal pop xlsx: ", conditionMessage(e))
    pr_pop24_long <<- tibble(fips = character(), year = integer(),
                             pop_postcensal = numeric())
  })


  # ==============================================================================
  # 4. BUILD DEMOGRAPHIC SUMMARIES (county × year totals)
  # ==============================================================================

  cat("\n--- Building demographic summaries ---\n")

  # We extract the county-year totals (AGEGRP == 0) from each CC file, then
  # combine intercensal (2010-2019) with postcensal (2020-2024).

  # --- 4A. Age/Sex summary -------------------------------------------------

  build_agesex_summary <- function(df, label = "") {
    if (nrow(df) == 0) {
      return(tibble::tibble(fips = character(), year = integer()))
    }

    # Possible columns of interest at the total level
    keep_cols <- c("fips", "year", "agegrp",
                   "tot_pop", "tot_male", "tot_female",
                   # Common age-group total columns (from AGESEX files)
                   "under5_tot", "age513_tot", "age1417_tot", "age1824_tot",
                   "age16plus_tot", "age18plus_tot", "age1544_fem",
                   "age65plus_tot", "median_age_tot")

    available <- intersect(keep_cols, names(df))

    out <- df

    if ("agegrp" %in% names(out)) {
      out <- out %>% filter(agegrp == 0)
    }

    out <- out %>% select(all_of(available))

    cat("    ", label, "– rows:", nrow(out), "\n")
    return(out)
  }

  agesex_int  <- build_agesex_summary(cc_int_agesex, "Intercensal agesex")
  agesex_post <- build_agesex_summary(cc24_agesex,   "Postcensal agesex")

  # Puerto Rico age/sex summaries
  pr_agesex_int  <- build_agesex_summary(pr_int_agesex_raw, "PR intercensal agesex")
  pr_agesex_post <- build_agesex_summary(pr24_agesex,       "PR postcensal agesex")

  # Stack, preferring postcensal for overlapping year 2020
  agesex_combined <- bind_rows(
    agesex_int  %>% filter(year < 2020),
    agesex_post,
    pr_agesex_int  %>% filter(year < 2020),
    pr_agesex_post
  ) %>%
    select(-any_of("agegrp")) %>%
    distinct(fips, year, .keep_all = TRUE)

  # --- 4B. Race/Ethnicity summary -----------------------------------------

  build_race_summary <- function(df, label = "") {
    if (nrow(df) == 0) {
      return(tibble::tibble(fips = character(), year = integer()))
    }

    keep_cols <- c("fips", "year", "agegrp",
                   "tot_pop", "tot_male", "tot_female",
                   # Race alone
                   "wa_male", "wa_female", "ba_male", "ba_female",
                   "ia_male", "ia_female", "aa_male", "aa_female",
                   "na_male", "na_female", "tom_male", "tom_female",
                   # Not-Hispanic
                   "nh_male", "nh_female",
                   "nhwa_male", "nhwa_female", "nhba_male", "nhba_female",
                   # Hispanic
                   "h_male", "h_female",
                   "hwa_male", "hwa_female")

    available <- intersect(keep_cols, names(df))

    out <- df %>%
      filter(agegrp == 0) %>%
      select(all_of(available))

    cat("    ", label, "– rows:", nrow(out), "\n")
    return(out)
  }

  race_int  <- build_race_summary(cc_int_alldata, "Intercensal race")
  race_post <- build_race_summary(cc24_alldata,   "Postcensal race")

  # Combine: intercensal for 2010-2019, postcensal for 2020+
  race_combined <- bind_rows(
    race_int  %>% filter(year < 2020),
    race_post
  ) %>%
    select(-any_of("agegrp")) %>%
    distinct(fips, year, .keep_all = TRUE)

  # Derive convenience totals from race data
  race_combined <- race_combined %>%
    mutate(
      white_alone_total    = coalesce(wa_male,  0) + coalesce(wa_female,  0),
      black_alone_total    = coalesce(ba_male,  0) + coalesce(ba_female,  0),
      aian_alone_total     = coalesce(ia_male,  0) + coalesce(ia_female,  0),
      asian_alone_total    = coalesce(aa_male,  0) + coalesce(aa_female,  0),
      nhpi_alone_total     = coalesce(na_male,  0) + coalesce(na_female,  0),
      two_or_more_total    = coalesce(tom_male, 0) + coalesce(tom_female, 0),
      hispanic_total       = coalesce(h_male,   0) + coalesce(h_female,   0),
      not_hispanic_total   = coalesce(nh_male,  0) + coalesce(nh_female,  0),
      nhwa_total           = coalesce(nhwa_male,0) + coalesce(nhwa_female,0)
    )


  # ==============================================================================
  # 5. BUILD POPULATION & COMPONENTS BACKBONE
  # ==============================================================================

  cat("\n--- Building population backbone ---\n")

  # Create the county identifier reference table
  # Build PR reference from the PR agesex CSVs
  pr_ref_parts <- list()
  if (exists("pr_int_agesex_raw") && nrow(pr_int_agesex_raw) > 0) {
    pr_ref_parts[[1]] <- pr_int_agesex_raw %>%
      select(fips, state, county, stname, ctyname) %>%
      rename(state_fips = state, county_fips = county)
  }
  if (exists("pr24_agesex") && nrow(pr24_agesex) > 0) {
    pr_ref_parts[[length(pr_ref_parts) + 1]] <- pr24_agesex %>%
      select(fips, state, county, stname, ctyname) %>%
      rename(state_fips = state, county_fips = county)
  }
  if (length(pr_ref_parts) > 0) {
    pr_county_ref <- bind_rows(pr_ref_parts) %>%
      distinct(fips, .keep_all = TRUE)
  } else {
    pr_county_ref <- tibble(fips = character(), state_fips = character(),
                            county_fips = character(), stname = character(),
                            ctyname = character())
  }

  county_ref <- bind_rows(
    co2020 %>% select(fips, state, county, stname, ctyname) %>%
      rename(state_fips = state, county_fips = county),
    co2025 %>% select(fips, state, county, stname, ctyname) %>%
      rename(state_fips = state, county_fips = county),
    pr_county_ref
  ) %>%
    distinct(fips, .keep_all = TRUE)

  # --- Population: Prefer intercensal for 2010-2019, postcensal for 2020+ ---

  pop_backbone <- bind_rows(
    # 2010-2019: intercensal if available, else Vintage 2020
    int_pop_long %>%
      filter(year >= 2010, year <= 2019) %>%
      rename(population = pop_intercensal),

    # 2020-2025 from Vintage 2025
    co2025_pop %>%
      filter(year >= 2020) %>%
      select(fips, year, pop_postcensal) %>%
      rename(population = pop_postcensal),

    # Puerto Rico 2010-2019: intercensal
    pr_int_pop_long %>%
      filter(year >= 2010, year <= 2019) %>%
      rename(population = pop_intercensal),

    # Puerto Rico 2020+: postcensal
    pr_pop24_long %>%
      filter(year >= 2020) %>%
      rename(population = pop_postcensal)
  )

  # If intercensal xlsx was unavailable, fall back to Vintage 2020 for 2010-2019
  if (nrow(int_pop_long) == 0) {
    cat("  NOTE: Intercensal pop xlsx not available; using Vintage 2020 for 2010-2019.\n")
    pop_backbone <- bind_rows(
      co2020_pop %>%
        filter(year >= 2010, year <= 2019) %>%
        select(fips, year, pop_vintage2020) %>%
        rename(population = pop_vintage2020),
      co2025_pop %>%
        filter(year >= 2020) %>%
        select(fips, year, pop_postcensal) %>%
        rename(population = pop_postcensal),
      # Still include PR data
      pr_int_pop_long %>%
        filter(year >= 2010, year <= 2019) %>%
        rename(population = pop_intercensal),
      pr_pop24_long %>%
        filter(year >= 2020) %>%
        rename(population = pop_postcensal)
    )
  }

  pop_backbone <- pop_backbone %>% distinct(fips, year, .keep_all = TRUE)

  # --- Components of change: Vintage 2020 for 2010-2020, Vintage 2025 for 2020+ ---
  # For overlap year 2020, prefer Vintage 2025

  comp_backbone <- bind_rows(
    co2020_comp %>% filter(year >= 2010, year <= 2019),
    co2025_comp %>% filter(year >= 2020)
  ) %>%
    distinct(fips, year, .keep_all = TRUE)


  # ==============================================================================
  # 6. HOUSING UNITS
  # ==============================================================================

  cat("--- Building housing units ---\n")

  hu_backbone <- bind_rows(
    int_hu_long %>%
      filter(year >= 2010, year <= 2019) %>%
      rename(housing_units = housing_units_intercensal),
    hu2024_long %>%
      filter(year >= 2020) %>%
      rename(housing_units = housing_units_postcensal)
  ) %>%
    distinct(fips, year, .keep_all = TRUE)

  # Fallback if xlsx files were not processable
  if (nrow(hu_backbone) == 0) {
    cat("  NOTE: Housing xlsx files yielded no data. Housing will be NA.\n")
    hu_backbone <- tibble(fips = character(), year = integer(), housing_units = numeric())
  }


  # ==============================================================================
  # 7. MERGE EVERYTHING INTO FINAL PANEL
  # ==============================================================================

  cat("\n--- Merging into final county panel ---\n")

  county_panel <- pop_backbone %>%
    # Join county identifiers
    left_join(county_ref, by = "fips") %>%
    # Join components of change
    left_join(comp_backbone, by = c("fips", "year")) %>%
    # Join housing units
    left_join(hu_backbone, by = c("fips", "year")) %>%
    # Join age/sex summary
    left_join(
      agesex_combined %>% select(-any_of(c("tot_pop"))),
      by = c("fips", "year")
    ) %>%
    # Join race/ethnicity summary
    left_join(
      race_combined %>% select(-any_of(c("tot_pop", "tot_male", "tot_female"))),
      by = c("fips", "year")
    ) %>%
    # Reorder columns for clarity
    select(
      fips, state_fips, county_fips, stname, ctyname, year,
      # Population
      population,
      # Components of change
      births, deaths, naturalchg,
      internationalmig, domesticmig, netmig, residual,
      # Housing
      housing_units,
      # Sex totals
      tot_male, tot_female,
      # Race totals (derived)
      starts_with("white_"), starts_with("black_"), starts_with("aian_"),
      starts_with("asian_"), starts_with("nhpi_"), starts_with("two_or_more"),
      hispanic_total, not_hispanic_total, nhwa_total,
      # Age groups (if available)
      starts_with("under5"), starts_with("age"),
      starts_with("median_age"),
      # Everything else not yet selected
      everything()
    ) %>%
    arrange(fips, year)


  # ==============================================================================
  # 8. DIAGNOSTICS
  # ==============================================================================

  cat("\n========== FINAL PANEL SUMMARY ==========\n")
  cat("Dimensions:", nrow(county_panel), "rows ×", ncol(county_panel), "columns\n")
  cat("Year range:", min(county_panel$year), "–", max(county_panel$year), "\n")
  cat("Unique counties:", n_distinct(county_panel$fips), "\n")
  cat("Rows per year:\n")
  print(count(county_panel, year))

  cat("\nColumn names:\n")
  cat(paste(" ", names(county_panel)), sep = "\n")

  cat("\nFirst few rows:\n")
  print(head(county_panel, 10))

  cat("\nMissingness summary (% NA per key column):\n")
  key_cols <- c("population", "births", "deaths", "housing_units",
                "tot_male", "tot_female", "hispanic_total")
  key_cols <- intersect(key_cols, names(county_panel))
  missing_pct <- county_panel %>%
    summarise(across(all_of(key_cols), ~ round(mean(is.na(.x)) * 100, 1)))
  print(as.data.frame(missing_pct))


  # ==============================================================================
  # 9. SAVE OUTPUT
  # ==============================================================================

  out_path <- file.path(getwd(), "Output/pep_output/county_panel_2010_2025.csv")
  write_csv(county_panel, out_path)
  cat("\nPanel saved to:", out_path, "\n")

  # Also save as .rds for full fidelity
  rds_path <- file.path(getwd(), "Output/pep_output/county_panel_2010_2025.rds")
  saveRDS(county_panel, rds_path)
  cat("Panel saved to:", rds_path, "\n")

  cat("\n========== DONE ==========\n")
}
