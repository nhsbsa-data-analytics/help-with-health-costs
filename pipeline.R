# pipeline.R
# this script provides the code to run the reproducible analytical pipeline
# and produce the Help with Health Costs (HWHC) publication

# contains the following sections:
# 1. Setup and package installation
# 2. Data import
# 3. Aggregations and analysis
# 4. Data tables
# 5. Charts and figures
# 6. Render outputs

# clear environment
rm(list = ls())

# source functions
# select all .R files in functions sub-folder
function_files <- list.files(path = "functions", pattern = "\\.R$")

# loop over function_files to source all files in functions sub-folder
for (file in function_files) {
  source(file.path("functions", file))
}

#1. Setup and package installation ---------------------------------------------

# load GITHUB_KEY if available in environment or enter if not
if (Sys.getenv("GITHUB_PAT") == "") {
  usethis::edit_r_environ()
  stop(
    "You need to set your GITHUB_PAT = YOUR PAT KEY in the .Renviron file which pops up. Please restart your R Studio after this and re-run the pipeline."
  )
}

# load database credentials if available in environment or enter if not
if (Sys.getenv("DB_DWCP_USERNAME") == "") {
  usethis::edit_r_environ()
  stop(
    "You need to set your DB_DWCP_USERNAME = YOUR DWCP USERNAME and  DB_DWCP_PASSWORD = YOUR DWCP PASSWORD in the .Renviron file which pops up. Please restart your R Studio after this and re-run the pipeline."
  )
}

# install and load devtools package
install.packages("devtools")
library(devtools)

# install nhsbsaUtils package first to use function check_and_install_packages()
devtools::install_github(
  "nhsbsa-data-analytics/nhsbsaUtils",
  auth_token = Sys.getenv("GITHUB_PAT"),
  force = TRUE
)

library(nhsbsaUtils)

# install required packages
# double check required packages once full pipeline built eg. if maps used
req_pkgs <- c(
  "broom",
  "data.table",
  "devtools",
  "DBI",
  "dbplyr",
  "dplyr",
  "DT" ,
  "geojsonsf",
  "highcharter",
  "htmltools",
  "janitor",
  "kableExtra",
  "lubridate",
  "logr",
  "magrittr",
  "nhsbsa-data-analytics/nhsbsaR",
  "nhsbsa-data-analytics/nhsbsaExternalData",
  "nhsbsa-data-analytics/accessibleTables",
  "nhsbsa-data-analytics/nhsbsaVis",
  "openxlsx",
  "rmarkdown",
  "rsample",
  "sf",
  "stringr",
  "svDialogs",
  "tcltk",
  "tidyr",
  "tidyverse",
  "vroom",
  "yaml"
)

# library/install packages as required
nhsbsaUtils::check_and_install_packages(req_pkgs)

# set up logging
lf <-
  logr::log_open(paste0(
    "./log/hwhc_log",
    format(Sys.time(), "%d%m%y%H%M%S"),
    ".log"
  ))

# load config
config <- yaml::yaml.load_file("config.yml")
logr::log_print("Config loaded", hide_notes = TRUE)
logr::log_print(config, hide_notes = TRUE)

# load options
nhsbsaUtils::publication_options()
logr::log_print("Options loaded", hide_notes = TRUE)


# 2. Data import ----------------------------------------------------------

# establish connection to database
con <- nhsbsaR::con_nhsbsa(
  dsn = "FBS_8192k",
  driver = "Oracle in OraClient19Home1",
  "DWCP"
)


# 2.1 Data Import: NHS Low Income Scheme ----------------------------------

# create the base dataset for NHS Low Income Scheme
# data at individual case level (ID) for all applications/certificates
if(config$rebuild_base_data == TRUE){
  create_dataset_from_sql(
    db_connection = con,
    path_to_sql_file = "./SQL/LIS_FACT.sql",
    db_table_name = "LIS_FACT",
    ls_variables = list(
      var = c("p_extract_date"),
      val = c(config$extract_date_lis)
    )
  )
}

# create the base dataset for HES areas (PPC, MATEX, MEDEX and Tax Credit)
# data at individual case level (ID) for all applications/certificates
if(config$rebuild_base_data == TRUE){
  create_dataset_from_sql(
    db_connection = con,
    path_to_sql_file = "./SQL/HES_FACT.sql",
    db_table_name = "HES_FACT",
    ls_variables = list(
      var = c("p_extract_date"),
      val = c(config$extract_date_hes)
    )
  )
}

# create dbplyr connection to table 
db_lis <- dplyr::tbl(
  con, 
  from = dbplyr::in_schema(toupper(con@info$username), "LIS_FACT")
)

# 3. Aggregations and analysis --------------------------------------------


# 3.1 Aggregation and analysis: NHS Low Income Scheme ---------------------


# 3.1.1 LIS Applications --------------------------------------------------
# Applications will include any application to the scheme regardless of current status or outcome
# The dataset is already limited to HC1 applications excluding HC5 (refund only) applications

# Data:
df_lis_fy_app <- db_lis |> 
  dplyr::filter(APPLICATION_YM >= config$min_trend_ym_lis & APPLICATION_YM <= config$max_trend_ym_lis) |> 
  dplyr::select(FINANCIAL_YEAR = APPLICATION_FY, COUNTRY, ID) |> 
  dplyr::group_by(FINANCIAL_YEAR, COUNTRY) |> 
  dplyr::summarise(
    APPLICATIONS = n(),
    .groups = "keep"
  ) |> 
  dplyr::arrange(COUNTRY, FINANCIAL_YEAR) |> 
  dplyr::collect()

# Chart Data: Combine all areas and include a rounded column for display
ch_data_lis_fy_app <- df_lis_fy_app |>
  dplyr::group_by(FINANCIAL_YEAR) |> 
  dplyr::summarise(APPLICATIONS = sum(APPLICATIONS)) |> 
  dplyr::mutate(APPLICATIONS_SF = signif(APPLICATIONS,3))

# Chart:
ch_lis_fy_app <- ch_data_lis_fy_app |>
  nhsbsaVis::basic_chart_hc(
    x = FINANCIAL_YEAR,
    y = APPLICATIONS_SF,
    type = "line",
    xLab = "Financial Year",
    yLab = "Number of applications received",
    seriesName = "Applications received",
    title = "",
    dlOn = FALSE
  ) |> 
  highcharter::hc_tooltip(
    enabled = T,
    shared = T,
    sort = T
  )

# Chart Data Download:
dl_lis_fy_app <- ch_data_lis_fy_app |> 
  dplyr::select(
    `Financial Year` = FINANCIAL_YEAR,
    `Number of applications to NHS Low Income Scheme` = APPLICATIONS
  )

# Support Data:
sd_lis_fy_app <- df_lis_fy_app |> 
  dplyr::select(
    `Financial Year` = FINANCIAL_YEAR,
    `Country` = COUNTRY,
    `Number of applications` = APPLICATIONS
  )


# 3.1.2 LIS: Outcome type -------------------------------------------------
# An "outcome/decision" will only be reached when the application is completed and assessed
# If applicants qualify for support they will be issued a HC2/HC3 certificate
# All other outcomes have simply been captured under the group "No certificate issued"
# Where no certificate is issued the ISSUE date will actually represent the APPLICATION date
# Not all applications will reach the "outcome/decision" stage as applicants may drop out during the application/assessment process

# Data:
df_lis_fy_issue <- db_lis |> 
  dplyr::filter(
    ISSUE_YM >= config$min_trend_ym_lis & 
      ISSUE_YM <= config$max_trend_ym_lis &
      APPLICATION_COMPLETE_FLAG == 1
  ) |> 
  dplyr::select(FINANCIAL_YEAR = ISSUE_FY, COUNTRY, CERTIFICATE_TYPE, ID) |> 
  dplyr::group_by(FINANCIAL_YEAR, COUNTRY, CERTIFICATE_TYPE) |> 
  dplyr::summarise(
    ISSUED = n(),
    .groups = "keep"
  ) |> 
  dplyr::arrange(COUNTRY, FINANCIAL_YEAR, CERTIFICATE_TYPE) |> 
  dplyr::collect()

# Chart Data: Combine all areas and include a rounded column for display
ch_data_lis_fy_issue <- df_lis_fy_issue |>
  dplyr::group_by(FINANCIAL_YEAR, CERTIFICATE_TYPE) |> 
  dplyr::summarise(ISSUED = sum(ISSUED)) |> 
  dplyr::mutate(ISSUED_SF = signif(ISSUED,3))

# Chart:
ch_lis_fy_issue <- ch_data_lis_fy_issue |>
  nhsbsaVis::group_chart_hc(
    x = FINANCIAL_YEAR,
    y = ISSUED_SF,
    type = "line",
    group = "CERTIFICATE_TYPE",
    xLab = "Financial Year",
    yLab = "Number of outcome decisions",
    title = "",
    dlOn = FALSE
  ) |>
  highcharter::hc_tooltip(
    enabled = T,
    shared = T,
    sort = T
  )

# Chart Data Download:
dl_lis_fy_issue <- ch_data_lis_fy_issue |>
  dplyr::select(-ISSUED_SF) |> 
  tidyr::pivot_wider(
    names_from = CERTIFICATE_TYPE,
    values_from = ISSUED
  ) |> 
  dplyr::mutate(`Total decisions issued` = HC2 + HC3 + `No certificate issued`) |> 
  dplyr::rename(
    `Financial Year` = FINANCIAL_YEAR,
    `HC2 certificates issued` = HC2,
    `HC3 certificates issued` = HC3
  )

# Support Data:
sd_lis_fy_issue <- df_lis_fy_issue |>
  tidyr::pivot_wider(
    names_from = CERTIFICATE_TYPE,
    values_from = ISSUED
  ) |> 
  dplyr::mutate(`Total decisions issued` = HC2 + HC3 + `No certificate issued`) |> 
  dplyr::rename(
    `Financial Year` = FINANCIAL_YEAR,
    `Country` = COUNTRY,
    `HC2 certificates issued` = HC2,
    `HC3 certificates issued` = HC3
  )


# 3.1.3 LIS active certificates -------------------------------------------
# Only issued HC2/HC3 certificates should be considered in "active" counts
# Certificates could be active for multiple years and therefore should be included in counts for each applicable year
# The validity period for a HC2/HC3 is determined when the certificate is issued so will not change if the holders circumstances change during this period

# identify a distinct list of financial years
df_fy_list <- dplyr::tbl(
  con, 
  from = dbplyr::in_schema("DIM", "YEAR_MONTH_DIM")
) |> 
  dplyr::filter(
    YEAR_MONTH >= config$min_trend_ym_lis & 
      YEAR_MONTH <= config$max_trend_ym_lis
  ) |> 
  dplyr::group_by(FINANCIAL_YEAR) |> 
  dplyr::summarise(
    MIN_YM = min(YEAR_MONTH),
    MAX_YM = max(YEAR_MONTH),
    .groups = "keep"
  ) |> 
  dplyr::ungroup()

# join HC2/HC3 data to financial year data to account for certificates appearing in multiple years
df_lis_fy_act <- df_fy_list |> 
  dplyr::left_join(
    db_lis |> 
      dplyr::filter(
        CERTIFICATE_ISSUED_FLAG == 1
      ) |> 
      dplyr::select(COUNTRY, CERTIFICATE_TYPE, ID, CERTIFICATE_START_YM, CERTIFICATE_EXPIRY_YM),
    dplyr::join_by("MAX_YM" >= "CERTIFICATE_START_YM", "MIN_YM" <= "CERTIFICATE_EXPIRY_YM")
  ) |> 
  dplyr::group_by(FINANCIAL_YEAR, COUNTRY, CERTIFICATE_TYPE) |> 
  dplyr::summarise(ACTIVE = n(), .groups = "keep") |> 
  dplyr::arrange(COUNTRY, FINANCIAL_YEAR) |> 
  dplyr::collect()

# Chart Data: Combine all data and include a rounded column for display
ch_data_lis_fy_act <- df_lis_fy_act |>
  dplyr::group_by(FINANCIAL_YEAR, CERTIFICATE_TYPE) |> 
  dplyr::summarise(ACTIVE = sum(ACTIVE)) |> 
  dplyr::mutate(ACTIVE_SF = signif(ACTIVE,3))

# Chart:
ch_lis_fy_act <- ch_data_lis_fy_act |>
  nhsbsaVis::group_chart_hc(
    x = FINANCIAL_YEAR,
    y = ACTIVE_SF,
    type = "line",
    group = "CERTIFICATE_TYPE",
    xLab = "Financial Year",
    yLab = "Number of active certificates",
    title = "",
    dlOn = FALSE
  ) |>
  highcharter::hc_tooltip(
    enabled = T,
    shared = T,
    sort = T
  )

# Chart Data Download: 
dl_lis_fy_act <- ch_data_lis_fy_act |> 
  dplyr::select(-ACTIVE_SF) |> 
  tidyr::pivot_wider(
    names_from = CERTIFICATE_TYPE,
    values_from = ACTIVE
  ) |> 
  dplyr::mutate(`Total active certificates` = HC2 + HC3) |> 
  dplyr::rename(
    `Financial Year` = FINANCIAL_YEAR,
    `Number of active HC2 certificates` = HC2,
    `Number of active HC3 certificates` = HC3
  )

# Support Data: 
sd_lis_fy_act <- df_lis_fy_act |>
  tidyr::pivot_wider(
    names_from = CERTIFICATE_TYPE,
    values_from = ACTIVE
  ) |> 
  dplyr::mutate(`Total active certificates` = HC2 + HC3) |> 
  dplyr::rename(
    `Financial Year` = FINANCIAL_YEAR,
    `Country` = COUNTRY,
    `Active HC2 certificates` = HC2,
    `Active HC3 certificates` = HC3
  )


# 3.1.4 LIS: Duration -------------------------------------------------
# The duration of HC2/HC3 certificates will vary based on applicants circumstances
# This reporting is only applicable for issued HC2/HC3 certificates
# CERTIFICATE_DURATION in the data uses some grouping categories to bundle certificates into common categories

# Data:
df_lis_duration <- db_lis |> 
  dplyr::filter(
    ISSUE_YM >= config$min_focus_ym_lis & 
      ISSUE_YM <= config$max_focus_ym_lis &
      CERTIFICATE_ISSUED_FLAG == 1
  ) |> 
  dplyr::select(
    FINANCIAL_YEAR = ISSUE_FY, 
    COUNTRY, 
    CERTIFICATE_TYPE, 
    CERTIFICATE_DURATION, 
    ID
  ) |> 
  dplyr::group_by(FINANCIAL_YEAR, COUNTRY, CERTIFICATE_TYPE, CERTIFICATE_DURATION) |> 
  dplyr::summarise(ISSUED = n(), .groups = "keep") |> 
  dplyr::arrange(COUNTRY, FINANCIAL_YEAR, CERTIFICATE_TYPE, CERTIFICATE_DURATION) |> 
  dplyr::collect()

# Chart Data: Combine all data and include a rounded column for display
ch_data_lis_duration <- df_lis_duration |>
  dplyr::group_by(FINANCIAL_YEAR, CERTIFICATE_TYPE, CERTIFICATE_DURATION) |> 
  dplyr::summarise(ISSUED = sum(ISSUED, na.rm = TRUE)) |> 
  dplyr::mutate(ISSUED_SF = signif(ISSUED,3))

# Chart:
ch_lis_duration <- ch_data_lis_duration |>
  nhsbsaVis::group_chart_hc(
    x = CERTIFICATE_DURATION,
    y = ISSUED_SF,
    type = "column",
    group = "CERTIFICATE_TYPE",
    xLab = "Certificate Duration",
    yLab = "Number of issued certificates",
    title = "",
    dlOn = FALSE
  ) |>
  highcharter::hc_tooltip(
    enabled = T,
    shared = T,
    sort = T
  ) |> 
  highcharter::hc_yAxis(labels = list(enabled = TRUE))

# Chart Data Download:
dl_lis_duration <- ch_data_lis_duration |>
  dplyr::select(-ISSUED_SF) |> 
  tidyr::pivot_wider(
    names_from = CERTIFICATE_TYPE,
    values_from = ISSUED
  ) |> 
  dplyr::mutate(`Total issued certificates` = HC2 + HC3) |> 
  dplyr::rename(
    `Financial Year` = FINANCIAL_YEAR,
    `Certificate Duration` = CERTIFICATE_DURATION,
    `HC2 certificates issued` = HC2,
    `HC3 certificates issued` = HC3
  )

# Support Data:
sd_lis_duration <- df_lis_duration |>
  tidyr::pivot_wider(
    names_from = CERTIFICATE_TYPE,
    values_from = ISSUED
  ) |> 
  dplyr::mutate(`Total issued certificates` = HC2 + HC3) |> 
  dplyr::rename(
    `Financial Year` = FINANCIAL_YEAR,
    `Country` = COUNTRY,
    `Certificate Duration` = CERTIFICATE_DURATION,
    `HC2 certificates issued` = HC2,
    `HC3 certificates issued` = HC3
  )

# 3.1.5 LIS: Age Breakdown -------------------------------------------------
# Age is available for the lead applicant only
# In a small number of cases the age is not available from the data and
# there are also some cases where the age is <15 or >99
# These have been grouped as "Unknown" in the AGE_BAND field calculation

# Data:
df_lis_age <- db_lis |> 
  dplyr::filter(
    ISSUE_YM >= config$min_focus_ym_lis & 
      ISSUE_YM <= config$max_focus_ym_lis &
      CERTIFICATE_ISSUED_FLAG == 1
  ) |> 
  dplyr::select(FINANCIAL_YEAR = ISSUE_FY, COUNTRY, CERTIFICATE_TYPE, AGE_BAND, ID) |> 
  dplyr::group_by(FINANCIAL_YEAR, COUNTRY, CERTIFICATE_TYPE, AGE_BAND) |> 
  dplyr::summarise(ISSUED = n(), .groups = "keep") |> 
  dplyr::arrange(COUNTRY, FINANCIAL_YEAR, CERTIFICATE_TYPE, AGE_BAND) |> 
  dplyr::collect()

# Chart Data: Combine all data and include a rounded column for display
ch_data_lis_age <- df_lis_age |> 
  dplyr::filter(AGE_BAND != 'Unknown') |> 
  dplyr::group_by(FINANCIAL_YEAR, CERTIFICATE_TYPE, AGE_BAND) |> 
  dplyr::summarise(ISSUED = sum(ISSUED, na.rm = TRUE)) |> 
  dplyr::mutate(ISSUED_SF = signif(ISSUED,3))

# Chart:
ch_lis_age <- ch_data_lis_age |>
  nhsbsaVis::group_chart_hc(
    x = AGE_BAND,
    y = ISSUED_SF,
    type = "column",
    group = "CERTIFICATE_TYPE",
    xLab = "Age Band",
    yLab = "Number of issued certificates",
    title = "",
    dlOn = FALSE
  ) |>
  highcharter::hc_tooltip(
    enabled = T,
    shared = T,
    sort = T
  ) |> 
  highcharter::hc_yAxis(labels = list(enabled = TRUE))

# Chart Data Download:
dl_lis_age <- ch_data_lis_age |>
  dplyr::select(-ISSUED_SF) |> 
  tidyr::pivot_wider(
    names_from = CERTIFICATE_TYPE,
    values_from = ISSUED
  ) |> 
  dplyr::mutate(`Total issued certificates` = HC2 + HC3) |> 
  dplyr::rename(
    `Financial Year` = FINANCIAL_YEAR,
    `Age Band` = AGE_BAND,
    `HC2 certificates issued` = HC2,
    `HC3 certificates issued` = HC3
  )

# Support Data:
# Include unknown ages
sd_lis_age <- df_lis_age |> 
  tidyr::pivot_wider(
    names_from = CERTIFICATE_TYPE,
    values_from = ISSUED
  ) |> 
  dplyr::mutate(`Total issued certificates` = HC2 + HC3) |> 
  dplyr::rename(
    `Financial Year` = FINANCIAL_YEAR,
    `Country` = COUNTRY,
    `Age Band` = AGE_BAND,
    `HC2 certificates issued` = HC2,
    `HC3 certificates issued` = HC3
  )

# 3.1.5 LIS: IMD Breakdown -------------------------------------------------
# IMD quintile assigned based on the applicants postcode (only available for England)
# Limited to issued HC2/HC3 certificates in the latest financial year
# Chart based on all activity by support data split by applicant country

# Data:
df_lis_imd <- db_lis |> 
  dplyr::filter(
    ISSUE_YM >= config$min_focus_ym_lis & 
      ISSUE_YM <= config$max_focus_ym_lis &
      CERTIFICATE_ISSUED_FLAG == 1
  ) |> 
  dplyr::select(ISSUE_FY, COUNTRY, CERTIFICATE_TYPE, IMD_QUINTILE, ID) |> 
  dplyr::group_by(ISSUE_FY, COUNTRY, CERTIFICATE_TYPE, IMD_QUINTILE) |> 
  dplyr::summarise(ISSUED = n(), .groups = "keep") |> 
  dplyr::arrange(COUNTRY, ISSUE_FY, CERTIFICATE_TYPE, IMD_QUINTILE) |> 
  dplyr::ungroup() |> 
  dplyr::collect()

# Chart Data:
ch_data_lis_imd <- df_lis_imd |> 
  dplyr::filter(!is.na(IMD_QUINTILE)) |> 
  dplyr::mutate(ISSUED_SF = signif(ISSUED,3))
  
# Chart: 
ch_lis_imd <- ch_data_lis_imd |>
  nhsbsaVis::group_chart_hc(
    x = IMD_QUINTILE,
    y = ISSUED_SF,
    type = "column",
    group = "CERTIFICATE_TYPE",
    xLab = "IMD Quintile (1 = most deprived)",
    yLab = "Number of issued certificates",
    title = "",
    dlOn = FALSE
  ) |>
  highcharter::hc_tooltip(
    enabled = T,
    shared = T,
    sort = T
  ) |> 
  highcharter::hc_yAxis(labels = list(enabled = TRUE))

# Chart Data Download:
dl_lis_imd <- ch_data_lis_imd |>
  dplyr::select(-COUNTRY, -ISSUED_SF) |> 
  tidyr::pivot_wider(
    names_from = CERTIFICATE_TYPE,
    values_from = ISSUED
  ) |> 
  dplyr::mutate(`Total issued certificates` = HC2 + HC3) |> 
  dplyr::rename(
  `Financial Year` = ISSUE_FY,
  `IMD Quintile` = IMD_QUINTILE,
  `HC2 certificates issued` = HC2,
  `HC3 certificates issued` = HC3
)

# Support Data:
sd_lis_imd <- df_lis_imd |>
  tidyr::pivot_wider(
    names_from = CERTIFICATE_TYPE,
    values_from = ISSUED
  ) |> 
  dplyr::mutate(`Total issued certificates` = HC2 + HC3) |> 
  dplyr::rename(
    `Financial Year` = ISSUE_FY,
    `Country` = COUNTRY,
    `IMD Quintile` = IMD_QUINTILE,
    `HC2 certificates issued` = HC2,
    `HC3 certificates issued` = HC3
  )

# 3.1.6 LIS: ICB Breakdown -------------------------------------------------
# ICBs can vary in size and therefore are not appropriate for direct comparison
# Figures should be standardised by a population denominator
# ONS only publish mid-year estimates and at a delayed schedule
# latest available population year should be defined in the config file

# Data:
df_lis_icb <- db_lis |> 
  dplyr::filter(
    ISSUE_YM >= config$min_focus_ym_lis & 
      ISSUE_YM <= config$max_focus_ym_lis &
      CERTIFICATE_ISSUED_FLAG == 1
  ) |> 
  dplyr::select(FINANCIAL_YEAR = ISSUE_FY, COUNTRY, CERTIFICATE_TYPE, ICB, ICB_NAME, ID) |> 
  dplyr::group_by(FINANCIAL_YEAR, COUNTRY, CERTIFICATE_TYPE, ICB, ICB_NAME) |> 
  dplyr::summarise(ISSUED = n(), .groups = "keep") |> 
  dplyr::ungroup() |> 
  dplyr::arrange(COUNTRY, FINANCIAL_YEAR, CERTIFICATE_TYPE, ICB, ICB_NAME) |> 
  dplyr::collect() |> 
  tidyr::pivot_wider(
    names_from = CERTIFICATE_TYPE,
    values_from = ISSUED
  ) |> 
  dplyr::mutate(OVR = HC2 + HC3) |> 
  # join to base population figures
  # limit to total population aged 16 to 90
  dplyr::left_join(
    icb_population_data(
      year = config$ons_pop_year, 
      geo = "ICB", 
      min_age = config$lis_min_pop_age, 
      max_age = config$lis_max_pop_age
      
    ) |> 
      dplyr::select(
        ICB, 
        BASE_POPULATION = Total
      ),
    dplyr::join_by("ICB")
  ) |> 
  # calculate rates per 10000 population
  dplyr::mutate(
    HC2_PER_10000_POP = round(HC2 / BASE_POPULATION * 10000,0),
    HC3_PER_10000_POP = round(HC3 / BASE_POPULATION * 10000,0),
    OVR_PER_10000_POP = round(OVR / BASE_POPULATION * 10000,0)
  )

# Chart Data:
ch_data_lis_icb <- df_lis_icb |> 
  dplyr::filter(!is.na(ICB)) |> 
  dplyr::mutate(
    OVR_PER_10000_POP_SF = round(OVR_PER_10000_POP,0),
    OVR_SF = signif(OVR,3),
    BASE_POPULATION_SF = signif(BASE_POPULATION,3)
    )

# Chart:
ch_lis_icb <- ch_data_lis_icb |>
  dplyr::arrange(desc(OVR_PER_10000_POP)) |> 
  nhsbsaVis::basic_chart_hc(
    x = ICB_NAME,
    y = OVR_PER_10000_POP,
    type = "column",
    xLab = "ICB",
    yLab = "Number of issued certificates per 10,000 population",
    title = "",
    dlOn = FALSE
  ) |>
  highcharter::hc_tooltip(
    enabled = T,
    shared = T,
    sort = T,
    pointFormat = "Certificates issued per 10,000 population: {point.OVR_PER_10000_POP_SF} <br> Certificates issued (HC2 + HC3): {point.OVR_SF} <br> Population (aged 16+): {point.BASE_POPULATION_SF}"
  ) |> 
  highcharter::hc_xAxis(labels = list(enabled = FALSE)) |> 
  highcharter::hc_yAxis(labels = list(enabled = TRUE))

# Map:
map_lis_icb <- basic_map_hc(
  geo_data = get_icb_map_boundaries(config$icb_classification),
  df = ch_data_lis_icb,
  ons_code_field = "ICB",
  area_name_field = "ICB_NAME",
  value_field = "OVR_PER_10000_POP",
  metric_definition_string = "Certificates issued per 10,000 population",
  decimal_places = 0,
  value_prefix = "",
  order_of_magnitude = "",
  custom_tooltip = paste0(
    "<b>ICB:</b> {point.ICB_NAME}<br>",
    "<b>Certificates issued per 10,000 population:</b> {point.OVR_PER_10000_POP_SF}<br>",
    "<b>Certificates issued (HC2 + HC3):</b> {point.OVR_SF}<br>",
    "<b>Population (aged 16+):</b> {point.BASE_POPULATION_SF}"
  )
)

# Chart Data Download:
dl_lis_icb <- ch_data_lis_icb |>
  dplyr::select(
    `Financial Year` = FINANCIAL_YEAR,
    `Country` = COUNTRY,
    `ICB` = ICB_NAME,
    `HC2 certificates issued` = HC2,
    `HC3 certificates issued` = HC3,
    `Total certificates issued` = OVR,
    `Population` = BASE_POPULATION,
    `Certificates issued per 10,000 population` = OVR_PER_10000_POP
  )

# Support Data:
sd_lis_icb <- df_lis_icb |>
  dplyr::select(
    `Financial Year` = FINANCIAL_YEAR,
    `Country` = COUNTRY,
    `ICB` = ICB_NAME,
    `HC2 certificates issued` = HC2,
    `HC3 certificates issued` = HC3,
    `Total certificates issued` = OVR,
    `Population` = BASE_POPULATION,
    `HC2 Certificates issued per 10,000 population` = HC2_PER_10000_POP,
    `HC3 Certificates issued per 10,000 population` = HC3_PER_10000_POP,
    `Certificates issued per 10,000 population` = OVR_PER_10000_POP
  )


# 3.2 Aggregation and analysis: Maternity Exemption (MATEX) ---------------


# 3.2.1 MATEX Applications ------------------------------------------------
# Applications will include any application to the scheme regardless of current status or outcome

# Chart:
ch_mat_fy_app <- get_hes_application_data(con, 'HES_FACT', 'MAT', config$min_trend_ym_mat, config$max_trend_ym_mat, c('APPLICATION_FY')) |>
  dplyr::arrange(APPLICATION_FY) |> 
  dplyr::mutate(APPLICATIONS_SF = signif(APPLICATIONS,3)) |> 
  nhsbsaVis::basic_chart_hc(
    x = APPLICATION_FY,
    y = APPLICATIONS_SF,
    type = "line",
    xLab = "Financial Year",
    yLab = "Number of applications received",
    seriesName = "Applications received",
    title = "",
    dlOn = FALSE
  ) |> 
  highcharter::hc_tooltip(
    enabled = T,
    shared = T,
    sort = T
  )

# Chart Data Download:
dl_mat_fy_app <- get_hes_application_data(con, 'HES_FACT', 'MAT', config$min_trend_ym_mat, config$max_trend_ym_mat, c('APPLICATION_FY')) |>
  dplyr::arrange(APPLICATION_FY) |> 
  dplyr::select(
    `Financial Year` = APPLICATION_FY,
    `Number of applications for maternity exemption certificates` = APPLICATIONS
  )

# Support Data:
sd_mat_fy_app <- get_hes_application_data(con, 'HES_FACT', 'MAT', config$min_trend_ym_mat, config$max_trend_ym_mat, c('APPLICATION_FY')) |>
  dplyr::arrange(APPLICATION_FY) |>
  dplyr::select(
    `Financial Year` = APPLICATION_FY,
    `Number of applications` = APPLICATIONS
  )



# 3.3 Aggregation and analysis: Maternity Exemption (MEDEX) ---------------


# 3.3.1 MEDEX Applications ------------------------------------------------
# Applications will include any application to the scheme regardless of current status or outcome

# Chart:
ch_med_fy_app <- get_hes_application_data(con, 'HES_FACT', 'MED', config$min_trend_ym_med, config$max_trend_ym_med, c('APPLICATION_FY')) |>
  dplyr::arrange(APPLICATION_FY) |> 
  dplyr::mutate(APPLICATIONS_SF = signif(APPLICATIONS,3)) |> 
  nhsbsaVis::basic_chart_hc(
    x = APPLICATION_FY,
    y = APPLICATIONS_SF,
    type = "line",
    xLab = "Financial Year",
    yLab = "Number of applications received",
    seriesName = "Applications received",
    title = "",
    dlOn = FALSE
  ) |> 
  highcharter::hc_tooltip(
    enabled = T,
    shared = T,
    sort = T
  )

# Chart Data Download:
dl_med_fy_app <- get_hes_application_data(con, 'HES_FACT', 'MED', config$min_trend_ym_med, config$max_trend_ym_med, c('APPLICATION_FY')) |>
  dplyr::arrange(APPLICATION_FY) |> 
  dplyr::select(
    `Financial Year` = APPLICATION_FY,
    `Number of applications for maternity exemption certificates` = APPLICATIONS
  )

# Support Data:
sd_med_fy_app <- get_hes_application_data(con, 'HES_FACT', 'MED', config$min_trend_ym_med, config$max_trend_ym_med, c('APPLICATION_FY')) |>
  dplyr::arrange(APPLICATION_FY) |> 
  dplyr::select(
    `Financial Year` = APPLICATION_FY,
    `Number of applications` = APPLICATIONS
  )


# 3.4 Aggregation and analysis: Maternity Exemption (PPC) ---------------


# 3.4.1 PPC Applications ------------------------------------------------
# Applications will include any application to the scheme regardless of current status or outcome

# Chart:
ch_ppc_fy_app <- get_hes_application_data(con, 'HES_FACT', 'PPC', config$min_trend_ym_ppc, config$max_trend_ym_ppc, c('CERTIFICATE_SUBTYPE', 'APPLICATION_FY')) |>
  dplyr::filter(CERTIFICATE_SUBTYPE %in% c('3-month','12-month')) |> 
  dplyr::arrange(APPLICATION_FY, CERTIFICATE_SUBTYPE) |> 
  dplyr::mutate(APPLICATIONS_SF = signif(APPLICATIONS,3)) |> 
  nhsbsaVis::group_chart_hc(
    x = APPLICATION_FY,
    y = APPLICATIONS_SF,
    type = "line",
    group = "CERTIFICATE_SUBTYPE",
    xLab = "Financial Year",
    yLab = "Number of applications received",
    title = "",
    dlOn = FALSE
  ) |> 
  highcharter::hc_tooltip(
    enabled = T,
    shared = T,
    sort = T
  ) |> 
  highcharter::hc_yAxis(labels = list(formatter = htmlwidgets::JS( 
    "function() {
        return (this.value/1000000)+'m'; /* all labels to absolute values */
    }")))

# Chart Data Download:
dl_ppc_fy_app <- get_hes_application_data(con, 'HES_FACT', 'PPC', config$min_trend_ym_ppc, config$max_trend_ym_ppc, c('CERTIFICATE_TYPE','CERTIFICATE_SUBTYPE', 'APPLICATION_FY')) |>
  dplyr::arrange(APPLICATION_FY, CERTIFICATE_SUBTYPE) |>
  dplyr::select(
    `Financial Year` = APPLICATION_FY,
    `Certificate Type` = CERTIFICATE_TYPE,
    `Certificate Duration` = CERTIFICATE_SUBTYPE,
    `Number of applications for maternity exemption certificates` = APPLICATIONS
  )

# Support Data:
sd_ppc_fy_app <- get_hes_application_data(con, 'HES_FACT', 'PPC', config$min_trend_ym_ppc, config$max_trend_ym_ppc, c('CERTIFICATE_TYPE','CERTIFICATE_SUBTYPE', 'APPLICATION_FY')) |>
  dplyr::arrange(APPLICATION_FY, CERTIFICATE_SUBTYPE) |>
  dplyr::select(
    `Financial Year` = APPLICATION_FY,
    `Certificate Type` = CERTIFICATE_TYPE,
    `Certificate Duration` = CERTIFICATE_SUBTYPE,
    `Number of applications` = APPLICATIONS
  )


# 3.2-------------------------------------------------
# Data:
# Chart Data:
# Chart:
# Chart Data Download:
# Support Data:


# 3.x-------------------------------------------------
# Data:
# Chart Data:
# Chart:
# Chart Data Download:
# Support Data:

# 3.X Close ---------------------------------------------------------------



# close connection to database
DBI::dbDisconnect(con)

# 4. Data tables ----------------------------------------------------------

# data tables for spreadsheet outputs
# formatted according to accessibility standards
# user may need to update file name to write outputs to in future releases


# 4.1 Define sheets and metadata ------------------------------------------

# list sheets to include in the output
sheetNames <- c(
  # "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" # 31 character limit
  "LIS_Applications",
  "LIS_Outcomes",
  "LIS_Active_Certificates",
  "LIS_Certificate_Duration",
  "LIS_Age_Breakdown",
  "LIS_Deprivation_Breakdown",
  "LIS_ICB_Breakdown",
  "MAT_Applications",
  "MED_Applications",
  "PPC_Applications"
)

wb <- accessibleTables::create_wb(sheetNames)

# create metadata tab (will need to open file and auto row heights once ran)
meta_fields <- c(
  "Financial Year",
  "Country",
  "Number of applications",
  "HC2 certificates issued",
  "HC3 certificates issued",
  "No certificate issued",
  "Total decisions issued",
  "Total certificates issued",
  "Active HC2 certificates",
  "Active HC3 certificates",
  "Total active certificates",
  "Certificate Duration",
  "Age Band",
  "IMD Quintile",
  "ICB",
  "Population",
  "HC2 Certificates issued per 10,000 population",
  "HC3 Certificates issued per 10,000 population",
  "Certificates issued per 10,000 population"
)

meta_descs <-
  c(
    "Financial Year",
    "Country",
    "Number of applications",
    "HC2 certificates issued",
    "HC3 certificates issued",
    "No certificate issued",
    "Total decisions issued",
    "Total certificates issued",
    "Active HC2 certificates",
    "Active HC3 certificates",
    "Total active certificates",
    "Certificate Duration",
    "Age Band",
    "IMD Quintile",
    "ICB",
    "Population",
    "HC2 Certificates issued per 10,000 population",
    "HC3 Certificates issued per 10,000 population",
    "Certificates issued per 10,000 population"
  )

accessibleTables::create_metadata(wb,
                                  meta_fields,
                                  meta_descs)


# 4.2 Applications --------------------------------------------------------


# 4.2.1 Applications: Low Income Scheme -----------------------------------

# create the sheet
accessibleTables::write_sheet(
  wb,
  "LIS_Applications",
  paste0(
    config$publication_table_title,
    " - Number of applications to NHS Low Income Scheme split by financial year and country"
  ),
  c(
    "A country classification of 'Other' will represent applications with a postcode for a country other than England.",
    "A country classification of 'Unknown' will represent applications where the postcode could not be assigned to any country."
  ),
  sd_lis_fy_app,
  30
)

# apply formatting
# left align columns A to B
accessibleTables::format_data(wb,
                              "LIS_Applications",
                              c("A", "B"),
                              "left",
                              "")

# right align columns
accessibleTables::format_data(
  wb,
  "LIS_Applications",
  c(
    "C"
  ),
  "right",
  "0"
)


# 4.2.2 Applications: MATEX ---------------------------------------------

# create the sheet
accessibleTables::write_sheet(
  wb,
  "MAT_Applications",
  paste0(
    config$publication_table_title,
    " - Number of applications for maternity exemption certificates split by financial year."
  ),
  c(
    "n/a."
  ),
  sd_mat_fy_app,
  30
)

# apply formatting
# left align columns
accessibleTables::format_data(wb,
                              "MAT_Applications",
                              "A",
                              "left",
                              "")

# right align columns
accessibleTables::format_data(
  wb,
  "MAT_Applications",
  "B",
  "right",
  "0"
)

# 4.2.3 Applications: MEDEX ---------------------------------------------

# create the sheet
accessibleTables::write_sheet(
  wb,
  "MED_Applications",
  paste0(
    config$publication_table_title,
    " - Number of applications for medical exemption certificates split by financial year."
  ),
  c(
    "n/a."
  ),
  sd_med_fy_app,
  30
)

# apply formatting
# left align columns A to B
accessibleTables::format_data(wb,
                              "MED_Applications",
                              "A",
                              "left",
                              "")

# right align columns
accessibleTables::format_data(
  wb,
  "MED_Applications",
  "B",
  "right",
  "0"
)


# 4.2.4 Applications: PPC ---------------------------------------------

# create the sheet
accessibleTables::write_sheet(
  wb,
  "PPC_Applications",
  paste0(
    config$publication_table_title,
    " - Number of applications for prescription prepayment certificates split by financial year and certificate duration"
  ),
  c(
    "A certificate duration of 'unknown' has been used where the certificate duration cannot be identified as 3 or 12 months from the available application details."
  ),
  sd_ppc_fy_app,
  30
)

# apply formatting
# left align columns A to B
accessibleTables::format_data(wb,
                              "PPC_Applications",
                              c("A", "B", "C"),
                              "left",
                              "")

# right align columns
accessibleTables::format_data(
  wb,
  "PPC_Applications",
  c(
    "D"
  ),
  "right",
  "0"
)


# 4.3 Issued/Outcome ------------------------------------------------------

# create the sheet
accessibleTables::write_sheet(
  wb,
  "LIS_Outcomes",
  paste0(
    config$publication_table_title,
    " - Number of NHS Low Income Scheme assessment outcome decisions issued, split by financial year and country"
  ),
  c(
    "Results limited to cases where the application has been fully completed, assessed and a decision issued to the applicant.",
    "Where no certificate is issued, the financial year is based on the date of application in lieu of no available date of certificate being issued."
  ),
  sd_lis_fy_issue,
  30
)

# apply formatting
# left align columns A to B
accessibleTables::format_data(wb,
                              "LIS_Outcomes",
                              c("A", "B"),
                              "left",
                              "")

# right align columns
accessibleTables::format_data(
  wb,
  "LIS_Outcomes",
  c(
    "C",
    "D",
    "E",
    "F"
  ),
  "right",
  "0"
)


# 4.4 Active --------------------------------------------------------------

# create the sheet
accessibleTables::write_sheet(
  wb,
  "LIS_Active_Certificates",
  paste0(
    config$publication_table_title,
    " - Number of active NHS Low Income Scheme HC2/HC3 certificates split by financial year and country"
  ),
  c(
    "Certificates can be valid for upto 5 years and therefore some certificates may be represented in figures for multiple years."
  ),
  sd_lis_fy_act,
  30
)

# apply formatting
# left align columns A to B
accessibleTables::format_data(wb,
                              "LIS_Active_Certificates",
                              c("A", "B"),
                              "left",
                              "")

# right align columns
accessibleTables::format_data(
  wb,
  "LIS_Active_Certificates",
  c(
    "C",
    "D",
    "E"
  ),
  "right",
  "0"
)

# 4.5 Duration --------------------------------------------------------------

# create the sheet
accessibleTables::write_sheet(
  wb,
  "LIS_Certificate_Duration",
  paste0(
    config$publication_table_title,
    " - Number of issued NHS Low Income Scheme HC2/HC3 certificates, split by financial year, country and certificate duration"
  ),
  c(
    "Certificate duration has been grouped into categories based on certificate duration rounded to nearest number of months."
  ),
  sd_lis_duration,
  30
)

# apply formatting
# left align columns A to B
accessibleTables::format_data(wb,
                              "LIS_Certificate_Duration",
                              c("A", "B","C"),
                              "left",
                              "")

# right align columns
accessibleTables::format_data(
  wb,
  "LIS_Certificate_Duration",
  c(
    "D",
    "E",
    "F"
  ),
  "right",
  "0"
)

# 4.6 Age --------------------------------------------------------------

# create the sheet
accessibleTables::write_sheet(
  wb,
  "LIS_Age_Breakdown",
  paste0(
    config$publication_table_title,
    " - Number of issued NHS Low Income Scheme HC2/HC3 certificates, split by financial year, country and age of applicant"
  ),
  c(
    "Age is calculated at the point of application, with applicants aged 65 and over grouped as 65+.",
    "An age band of 'unknown' has been used where the applicants age could not be confidently assigned from the available data."
  ),
  sd_lis_age,
  30
)

# apply formatting
# left align columns A to B
accessibleTables::format_data(wb,
                              "LIS_Age_Breakdown",
                              c("A", "B","C"),
                              "left",
                              "")

# right align columns
accessibleTables::format_data(
  wb,
  "LIS_Age_Breakdown",
  c(
    "D",
    "E",
    "F"
  ),
  "right",
  "0"
)

# 4.7 Deprivation --------------------------------------------------------------

# create the sheet
accessibleTables::write_sheet(
  wb,
  "LIS_Deprivation_Breakdown",
  paste0(
    config$publication_table_title,
    " - Number of issued NHS Low Income Scheme HC2/HC3 certificates, split by financial year, country and IMD quintile"
  ),
  c(
    "The reported IMD quintile, where 1 is the most deprived and 5 the least deprived, is derived from the postcode held for an applicant.",
    "IMD quintiles cannot be assigned to applicants that can not be associated to an English postcode.",
    "IMD quintiles are calculated by ranking census lower-layer super output areas (LSOAs) from most deprived to least deprived and dividing them into equal groups.",
    "Quintiles range from the most deprived 20% (quintile 1) of small areas nationally to the least deprived 20% (quintile 5) of small areas nationally."
  ),
  sd_lis_imd,
  30
)

# apply formatting
# left align columns A to B
accessibleTables::format_data(wb,
                              "LIS_Deprivation_Breakdown",
                              c("A", "B","C"),
                              "left",
                              "")

# right align columns
accessibleTables::format_data(
  wb,
  "LIS_Deprivation_Breakdown",
  c(
    "D",
    "E",
    "F"
  ),
  "right",
  "0"
)

# 4.8 ICB --------------------------------------------------------------

# create the sheet
accessibleTables::write_sheet(
  wb,
  "LIS_ICB_Breakdown",
  paste0(
    config$publication_table_title,
    " - Number of issued NHS Low Income Scheme HC2/HC3 certificates, split by financial year, country and ICB"
  ),
  c(
    "The reported ICB is derived from the postcode held for an applicant.",
    "ICBs cannot be assigned to applicants that can not be associated to an English postcode.",
    "Population figures are based on mid-year estimates published by ONS for the population aged 16+ to align with age groups who could benefit from the NHS Low Income Scheme."
  ),
  sd_lis_icb,
  50
)

# apply formatting
# left align columns A to B
accessibleTables::format_data(wb,
                              "LIS_ICB_Breakdown",
                              c("A", "B","C"),
                              "left",
                              "")

# right align columns
accessibleTables::format_data(
  wb,
  "LIS_ICB_Breakdown",
  c(
    "D",
    "E",
    "F",
    "G",
    "H",
    "I",
    "J"
  ),
  "right",
  "0"
)

# 4.X Cover Sheet ---------------------------------------------------------

# create cover sheet
accessibleTables::makeCoverSheet(
  config$publication_top_name,
  config$publication_sub_name,
  paste0("Publication date: ", config$publication_date),
  wb,
  sheetNames,
  c(
    "Metadata",
    "Table 1: LIS Applications",
    "Table 2: LIS Outcomes",
    "Table 3: LIS Active Certificates",
    "Table 4: LIS Certificate Duration",
    "Table 5: LIS Age Breakdown",
    "Table 6: LIS Deprivation Breakdown",
    "Table 7: LIS ICB Breakdown",
    "Table 8: Maternity exemption certificate - Applications",
    "Table 9: Medical exemption certificate - Applications"
  ),
  c("Metadata", sheetNames)
)

# save file into outputs folder
openxlsx::saveWorkbook(wb,
                       "outputs/hwhc_tables.xlsx",
                       overwrite = TRUE)


# 5. Charts and figures ---------------------------------------------------


# 6. Render outputs ------------------------------------------------------------

rmarkdown::render(
  "hwhc-markdown.Rmd",
  output_format = "html_document",
  output_file = "outputs/help-with-health-costs.html"
)

rmarkdown::render(
  "hwhc-methodology.Rmd",
  output_format = "html_document",
  output_file = "outputs/help-with-health-costs-methodology.html"
)

logr::log_close()
