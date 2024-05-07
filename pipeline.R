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

# 2.2 Data Import: HES (MATEX, MEDEX, PPC & Tax Credits) ----------------------------------

# create the base dataset for HES areas (PPC, MATEX, MEDEX and Tax Credit)
# data at individual case level (ID) for all applications/certificates
if(config$rebuild_base_data == TRUE){
  create_dataset_from_sql(
    db_connection = con,
    path_to_sql_file = "./SQL/HES_FACT.sql",
    db_table_name = "HES_FACT",
    ls_variables = list(
      var = c("p_extract_date"),
      val = c(config$extract_date_hrt)
    )
  )
}

# 2.3 Data Import: HRT PPC ----------------------------------

# create the base dataset for HRT PPC
# data at individual case level (ID) for all applications/certificates
if(config$rebuild_base_data == TRUE){
  create_dataset_from_sql(
    db_connection = con,
    path_to_sql_file = "./SQL/HRTPPC_FACT.sql",
    db_table_name = "HRTPPC_FACT",
    ls_variables = list(
      var = c("p_extract_date"),
      val = c(config$extract_date_hes)
    )
  )
}

# 2.4 Data Import: PX PATIENT COUNTS ----------------------------------

# create the base dataset for prescription patient counts
# data at aggregated level showing counts of patients from the NHS prescription data
if(config$rebuild_base_data == TRUE){
  create_dataset_from_sql(
    db_connection = con,
    path_to_sql_file = "./SQL/PX_PAT_FACT.sql",
    db_table_name = "PX_PAT_FACT",
    ls_variables = list(
      var = c("p_min_ym","p_max_ym","p_age_date"),
      val = c(config$extract_px_min_ym, config$extract_px_max_ym, config$extract_px_age_dt)
    )
  )
}


# 3. Aggregations and analysis --------------------------------------------

# 3.1 Aggregation and analysis: NHS Low Income Scheme (LIS) ---------------------

# 3.1.1 LIS: Applications received (trend) --------------------------------------------------
# Applications will include any application to the scheme regardless of current status or outcome
# The dataset is already limited to HC1 applications excluding HC5 (refund only) applications

lis_application_objs <- create_hes_application_objects(
  db_connection = con,
  db_table_name = 'LIS_FACT',
  service_area = 'LIS',
  min_ym = config$min_trend_ym_lis,
  max_ym = config$max_trend_ym_lis,
  subtype_split = FALSE
)

# 3.1.2 LIS: Outcome type (trend)-------------------------------------------------
# An "outcome/decision" will only be reached when the application is completed and assessed
# If applicants qualify for support they will be issued a HC2/HC3 certificate
# All other outcomes have simply been captured under the group "No certificate issued"
# Where no certificate is issued the ISSUE date will actually represent the APPLICATION date
# Not all applications will reach the "outcome/decision" stage as applicants may drop out during the application/assessment process

lis_issued_objs <- create_hes_issued_objects(
  db_connection = con,
  db_table_name = 'LIS_FACT',
  service_area = 'LIS',
  min_ym = config$min_trend_ym_lis,
  max_ym = config$max_trend_ym_lis,
  subtype_split = TRUE
)

# 3.1.3 LIS: Active certificates (trend)-------------------------------------------
# Only issued HC2/HC3 certificates should be considered in "active" counts
# Certificates could be active for multiple years and therefore should be included in counts for each applicable year
# The validity period for a HC2/HC3 is determined when the certificate is issued so will not change if the holders circumstances change during this period

lis_active_objs <- create_hes_active_objects(
  db_connection = con,
  db_table_name = 'LIS_FACT',
  service_area = 'LIS',
  min_ym = config$min_trend_active_ym_lis,
  max_ym = config$max_trend_ym_lis,
  subtype_split = TRUE
)

# 3.1.4 LIS: Duration (latest year)-------------------------------------------------
# The duration of HC2/HC3 certificates will vary based on applicants circumstances
# This reporting is only applicable for issued HC2/HC3 certificates
# CERTIFICATE_DURATION in the data uses some grouping categories to bundle certificates into common categories

lis_duration_objs <- create_hes_duration_objects(
  db_connection = con,
  db_table_name = 'LIS_FACT',
  service_area = 'LIS',
  min_ym = config$min_focus_ym_lis,
  max_ym = config$max_focus_ym_lis,
  subtype_split = TRUE
)

# 3.1.5 LIS: Age profile (latest year)-------------------------------------------------
# Age is available for the lead applicant only
# Some processing has been performed to group by set age bands and reclassify potential errors 

lis_age_objs <- create_hes_age_objects(
  db_connection = con,
  db_table_name = 'LIS_FACT',
  service_area = 'LIS',
  min_ym = config$min_focus_ym_lis,
  max_ym = config$max_focus_ym_lis,
  subtype_split = TRUE
)

# 3.1.6 LIS: Deprivation profile (latest year)-------------------------------------------------
# IMD may not be available if the postcode cannot be mapped to NSPL

lis_imd_objs <- create_hes_imd_objects(
  db_connection = con,
  db_table_name = 'LIS_FACT',
  service_area = 'LIS',
  min_ym = config$min_focus_ym_lis,
  max_ym = config$max_focus_ym_lis,
  subtype_split = TRUE
)

# 3.1.7 LIS: ICB profile (latest year)-------------------------------------------------
# ICBs can vary in size and therefore are not appropriate for direct comparison
# Figures should be standardised by a population denominator
# ONS only publish mid-year estimates and at a delayed schedule
# latest available population year should be defined in the config file

lis_icb_objs <- create_hes_icb_objects(
  db_connection = con,
  db_table_name = 'LIS_FACT',
  service_area = 'LIS',
  min_ym = config$min_focus_ym_lis,
  max_ym = config$max_focus_ym_lis,
  subtype_split = TRUE,
  base_population_source = "ONS",
  population_min_age = config$lis_min_pop_age, 
  population_max_age = config$lis_max_pop_age,
  ons_population_year = config$ons_pop_year, 
  ons_population_gender = "T"
)

# 3.2 Aggregation and analysis: Maternity Exemption (MATEX) ---------------


# 3.2.1 MATEX: Applications received (trend)------------------------------------------------
# Applications will include any application to the scheme regardless of current status or outcome

mat_application_objs <- create_hes_application_objects(
  db_connection = con,
  db_table_name = 'HES_FACT',
  service_area = 'MAT',
  min_ym = config$min_trend_ym_mat,
  max_ym = config$max_trend_ym_mat,
  subtype_split = FALSE
)

# 3.2.2 MATEX: Certificates issued (trend)------------------------------------------------
# Issued certificates will only include cases where a certificate was issued to the customer

mat_issued_objs <- create_hes_issued_objects(
  db_connection = con,
  db_table_name = 'HES_FACT',
  service_area = 'MAT',
  min_ym = config$min_trend_ym_mat,
  max_ym = config$max_trend_ym_mat,
  subtype_split = FALSE
)

# 3.2.3 MATEX: Certificates active (trend)------------------------------------------------
# Active certificates will only include cases where a certificate was issued to the customer
# Certificates will be included if active for one or more days in the financial year and could be assigned to multiple years

mat_active_objs <- create_hes_active_objects(
  db_connection = con,
  db_table_name = 'HES_FACT',
  service_area = 'MAT',
  min_ym = config$min_trend_active_ym_mat,
  max_ym = config$max_trend_ym_mat,
  subtype_split = FALSE
)

# 3.2.4 MATEX: Duration (latest year)------------------------------------------------
# Looking at certificates issued in the latest FY
# Split by time between due date and certificate issue

# Chart:
ch_mat_duration <- get_matex_duration_data(con, 'HES_FACT', config$min_focus_ym_mat, config$max_focus_ym_mat) |>
  nhsbsaVis::basic_chart_hc(
    x = MONTHS_BETWEEN_DUE_DATE_AND_ISSUE,
    y = ROLLING_PROPORTION,
    type = "line",
    xLab = "Number of months between due date and certificate issue date",
    yLab = "Proportion of certificates issued (%)",
    seriesName = "Proportion of certificates issued (%)",
    title = "",
    dlOn = FALSE
  ) |> 
  highcharter::hc_tooltip(
    enabled = T,
    shared = T,
    sort = T
  ) |> 
  highcharter::hc_yAxis(max = 100) |> 
  highcharter::hc_xAxis(plotLines = list(
    list(
      label = list(
        text = "Expected due date",
        verticalAlign = "bottom",
        textAlign = "left",
        rotation = 270,
        x = -5,
        y = -5
      ),
      color = "#000000",
      width = 2,
      value = 0
    )
  ))

# Chart Data Download:
dl_mat_duration <- get_matex_duration_data(con, 'HES_FACT', config$min_focus_ym_mat, config$max_focus_ym_mat) |>
  dplyr::select(
    `Financial Year` = ISSUE_FY,
    `Number of months between due date and certificate issue date` = MONTHS_BETWEEN_DUE_DATE_AND_ISSUE,
    `Number of certificates issued` = ISSUED_CERTS,
    `Number of certificates issued (cumulative)` = ROLLING_ISSUED_CERTS,
    `Proportion of certificates issued (cumulative %)` = ROLLING_PROPORTION
  )

# Support Data:
sd_mat_duration <- get_matex_duration_data(con, 'HES_FACT', config$min_focus_ym_mat, config$max_focus_ym_mat) |>
  dplyr::mutate(COUNTRY = "n/a") |> 
  dplyr::select(
    `Financial Year` = ISSUE_FY,
    `Country` = COUNTRY,
    `Number of months between due date and certificate issue date` = MONTHS_BETWEEN_DUE_DATE_AND_ISSUE,
    `Number of certificates issued` = ISSUED_CERTS,
    `Number of certificates issued (cumulative)` = ROLLING_ISSUED_CERTS,
    `Proportion of certificates issued (cumulative %)` = ROLLING_PROPORTION
  )

# 3.2.5 MATEX: Age profile (latest year)------------------------------------------------
# Issued certificates will only include cases where a certificate was issued to the customer
# Some processing has been performed to group by set age bands and reclassify potential errors 

mat_age_objs <- create_hes_age_objects(
  db_connection = con,
  db_table_name = 'HES_FACT',
  service_area = 'MAT',
  min_ym = config$min_focus_ym_mat,
  max_ym = config$max_focus_ym_mat,
  subtype_split = FALSE
)

# for additional context identify the number of live births published by ONS
df_births_age <- get_ons_live_birth_geo_data(con, config$ons_birth_geo_table, "COUNTRY", config$ons_births_year)

# create chart for ONS data
ch_births_age <- df_births_age |>
  dplyr::arrange(AGE_BAND) |> 
  dplyr::mutate(LIVE_BIRTHS_SF = signif(LIVE_BIRTHS,3)) |> 
  nhsbsaVis::basic_chart_hc(
    x = AGE_BAND,
    y = LIVE_BIRTHS_SF,
    type = "column",
    xLab = "Age band",
    yLab = "Number of live births",
    seriesName = "Live births",
    title = "", # assign custom title in markdown
    dlOn = FALSE
  ) |> 
  highcharter::hc_tooltip(
    enabled = T,
    shared = T,
    sort = T
  )

# add ONS data to chart and supporting data
mat_age_objs$chart_data <- mat_age_objs$chart_data |> 
  dplyr::left_join(
    y = df_births_age,
    by = c(`Age Band` = "AGE_BAND")
  ) |> 
  dplyr::rename(!!paste0("ONS Live Births (", config$ons_births_year, ")") := LIVE_BIRTHS)

mat_age_objs$support_data <- mat_age_objs$support_data |> 
  dplyr::left_join(
    y = df_births_age,
    by = c(`Age Band` = "AGE_BAND")
  ) |> 
  dplyr::rename(!!paste0("ONS Live Births (", config$ons_births_year, ")") := LIVE_BIRTHS)


# 3.2.6 MATEX: Deprivation profile (latest year)------------------------------------------------
# IMD may not be available if the postcode cannot be mapped to NSPL

mat_imd_objs <- create_hes_imd_objects(
  db_connection = con,
  db_table_name = 'HES_FACT',
  service_area = 'MAT',
  min_ym = config$min_focus_ym_mat,
  max_ym = config$max_focus_ym_mat,
  subtype_split = FALSE
)

# for additional context identify the number of live births published by ONS
df_births_imd <- get_ons_live_birth_imd_data(con, config$ons_birth_imd_table, "IMD_QUINTILE", config$ons_births_year)

# create chart for ONS data
ch_births_imd  = df_births_imd |>
  dplyr::arrange(IMD_QUINTILE) |> 
  dplyr::mutate(LIVE_BIRTHS_SF = signif(LIVE_BIRTHS,3)) |> 
  nhsbsaVis::basic_chart_hc(
    x = IMD_QUINTILE,
    y = LIVE_BIRTHS_SF,
    type = "column",
    xLab = "IMD Quintile (1 = most deprived)",
    yLab = "Number of live births",
    seriesName = "Live births",
    title = "", # custom title applied in markdown
    dlOn = FALSE
  ) |> 
  highcharter::hc_tooltip(
    enabled = T,
    shared = T,
    sort = T
  )

# add ONS data to chart and supporting data
mat_imd_objs$chart_data <- mat_imd_objs$chart_data |> 
  dplyr::left_join(
    y = df_births_imd,
    by = c(`IMD Quintile` = "IMD_QUINTILE")
  ) |> 
  dplyr::rename(!!paste0("ONS Live Births (", config$ons_births_year, ")") := LIVE_BIRTHS)

mat_imd_objs$support_data <- mat_imd_objs$support_data |> 
  dplyr::left_join(
    y = df_births_imd,
    by = c(`IMD Quintile` = "IMD_QUINTILE")
  ) |> 
  dplyr::rename(!!paste0("ONS Live Births (", config$ons_births_year, ")") := LIVE_BIRTHS)

# 3.2.7 MATEX: ICB profile (latest year)-------------------------------------------------
# ICBs can vary in size and therefore are not appropriate for direct comparison
# Figures should be standardised by a population denominator
# ONS only publish mid-year estimates and at a delayed schedule
# latest available population year should be defined in the config file

mat_icb_objs <- create_hes_icb_objects(
  db_connection = con,
  db_table_name = 'HES_FACT',
  service_area = 'MAT',
  min_ym = config$min_focus_ym_mat,
  max_ym = config$max_focus_ym_mat,
  subtype_split = FALSE,
  base_population_source = "ONS",
  population_min_age = config$mat_min_pop_age, 
  population_max_age = config$mat_max_pop_age,
  ons_population_year = config$ons_pop_year, 
  ons_population_gender = "F"
)

# 3.3 Aggregation and analysis: Medical Exemption (MEDEX) ---------------


# 3.3.1 MEDEX: Applications received (trend)------------------------------------------------
# Applications will include any application to the scheme regardless of current status or outcome

med_application_objs <- create_hes_application_objects(
  db_connection = con,
  db_table_name = 'HES_FACT',
  service_area = 'MED',
  min_ym = config$min_trend_ym_med,
  max_ym = config$max_trend_ym_med,
  subtype_split = FALSE
)

# 3.3.2 MEDEX: Certificates issued (trend)------------------------------------------------
# Issued certificates will only include cases where a certificate was issued to the customer

med_issued_objs <- create_hes_issued_objects(
  db_connection = con,
  db_table_name = 'HES_FACT',
  service_area = 'MED',
  min_ym = config$min_trend_ym_med,
  max_ym = config$max_trend_ym_med,
  subtype_split = FALSE
)

# 3.3.3 MEDEX: Certificates active (trend)------------------------------------------------
# Active certificates will only include cases where a certificate was issued to the customer
# Certificates will be included if active for one or more days in the financial year and could be assigned to multiple years

med_active_objs <- create_hes_active_objects(
  db_connection = con,
  db_table_name = 'HES_FACT',
  service_area = 'MED',
  min_ym = config$min_trend_active_ym_med,
  max_ym = config$max_trend_ym_med,
  subtype_split = FALSE
)

# 3.3.4 MEDEX: Age profile (latest year)------------------------------------------------
# Some processing has been performed to group by set age bands and reclassify potential errors

med_age_objs <- create_hes_age_objects(
  db_connection = con,
  db_table_name = 'HES_FACT',
  service_area = 'MED',
  min_ym = config$min_focus_ym_med,
  max_ym = config$max_focus_ym_med,
  subtype_split = FALSE
)


# 3.3.5 MEDEX: Deprivation profile (latest year)------------------------------------------------
# IMD may not be available if the postcode cannot be mapped to NSPL

med_imd_objs <- create_hes_imd_objects(
  db_connection = con,
  db_table_name = 'HES_FACT',
  service_area = 'MED',
  min_ym = config$min_focus_ym_med,
  max_ym = config$max_focus_ym_med,
  subtype_split = FALSE
)

# 3.3.6 MEDEX: ICB profile (latest year)-------------------------------------------------
# ICBs can vary in size and therefore are not appropriate for direct comparison
# Figures should be standardised by a population denominator


# ICB by ONS population denominator
# ONS only publish mid-year estimates and at a delayed schedule
# latest available population year should be defined in the config file

# med_icb_objs <- create_hes_icb_objects(
#   db_connection = con,
#   db_table_name = 'HES_FACT',
#   service_area = 'MED',
#   min_ym = config$min_focus_ym_med,
#   max_ym = config$max_focus_ym_med,
#   subtype_split = FALSE,
#   base_population_source = 'ONS',
#   population_min_age = config$med_min_pop_age, 
#   population_max_age = config$med_max_pop_age,
#   ons_population_year = config$ons_pop_year, 
#   ons_population_gender = "T"
# )

# ICB by NHS prescription patient count
# Limited to EPS where LSOA is captured
# Based on all patients in relevant year receiving any prescribing
med_icb_objs <- create_hes_icb_objects(
  db_connection = con,
  db_table_name = 'HES_FACT',
  service_area = 'MED',
  min_ym = config$min_focus_ym_med,
  max_ym = config$max_focus_ym_med,
  subtype_split = FALSE,
  base_population_source = 'PX',
  population_min_age = config$med_min_pop_age, 
  population_max_age = config$med_max_pop_age,
  db_px_patient_table = 'PX_PAT_FACT',
  px_population_type = 'PATIENT_COUNT'
)


# 3.4 Aggregation and analysis: Prescription Prepayment Certificate (PPC) ---------------


# 3.4.1 PPC: Applications received (trend) ------------------------------------------------
# Applications will include any application to the scheme regardless of current status or outcome
# Applications will be for a specific certificate type and therefore can be split

ppc_application_objs <- create_hes_application_objects(
  db_connection = con,
  db_table_name = 'HES_FACT',
  service_area = 'PPC',
  min_ym = config$min_trend_ym_ppc,
  max_ym = config$max_trend_ym_ppc,
  subtype_split = TRUE
)

# 3.4.2 PPC: Certificates issued (trend)------------------------------------------------
# Issued certificates will only include cases where a certificate was issued to the customer

ppc_issued_objs <- create_hes_issued_objects(
  db_connection = con,
  db_table_name = 'HES_FACT',
  service_area = 'PPC',
  min_ym = config$min_trend_ym_ppc,
  max_ym = config$max_trend_ym_ppc,
  subtype_split = TRUE
)

# 3.4.3 PPC: Age profile (latest year)------------------------------------------------
# Some processing applied to base dataset to reclassify ages that are outside of expected ranges as these may be errors

ppc_age_objs <- create_hes_age_objects(
  db_connection = con,
  db_table_name = 'HES_FACT',
  service_area = 'PPC',
  min_ym = config$min_focus_ym_ppc,
  max_ym = config$max_focus_ym_ppc,
  subtype_split = TRUE
)

# 3.4.4 PPC: Deprivation profile (latest year)------------------------------------------------
# IMD may not be available if the postcode cannot be mapped to NSPL

ppc_imd_objs <- create_hes_imd_objects(
  db_connection = con,
  db_table_name = 'HES_FACT',
  service_area = 'PPC',
  min_ym = config$min_focus_ym_ppc,
  max_ym = config$max_focus_ym_ppc,
  subtype_split = TRUE
)

# 3.4.5 PPC: ICB profile (latest year)-------------------------------------------------
# ICBs can vary in size and therefore are not appropriate for direct comparison
# Figures should be standardised by a population denominator


# ONS only publish mid-year estimates and at a delayed schedule
# latest available population year should be defined in the config file
# ppc_icb_objs <- create_hes_icb_objects(
#   db_connection = con,
#   db_table_name = 'HES_FACT',
#   service_area = 'PPC',
#   min_ym = config$min_focus_ym_ppc,
#   max_ym = config$max_focus_ym_ppc,
#   subtype_split = TRUE,
#   base_population_source = 'ONS',
#   population_min_age = config$ppc_min_pop_age, 
#   population_max_age = config$ppc_max_pop_age,
#   ons_population_year = config$ons_pop_year, 
#   ons_population_gender = "T"
# )

# ICB by NHS prescription patient count
# Limited to EPS where LSOA is captured
# Based on all patients in relevant year receiving any prescribing
ppc_icb_objs <- create_hes_icb_objects(
  db_connection = con,
  db_table_name = 'HES_FACT',
  service_area = 'PPC',
  min_ym = config$min_focus_ym_ppc,
  max_ym = config$max_focus_ym_ppc,
  subtype_split = TRUE,
  base_population_source = 'PX',
  population_min_age = config$ppc_min_pop_age, 
  population_max_age = config$ppc_max_pop_age,
  db_px_patient_table = 'PX_PAT_FACT',
  px_population_type = 'PATIENT_COUNT'
)

# 3.5 Aggregation and analysis: HRT PPC (HRTPPC) ---------------------

# 3.5.1 HRTPPC: Applications received (trend) --------------------------------------------------
# Applications will include any application to the scheme regardless of current status or outcome

hrt_application_objs <- create_hes_application_month_objects(
  db_connection = con,
  db_table_name = 'HRTPPC_FACT',
  service_area = 'HRTPPC',
  min_ym = config$min_focus_ym_hrt,
  max_ym = config$max_focus_ym_hrt,
  subtype_split = FALSE
)

# 3.5.2 HRTPPC: Certificates issued (trend) --------------------------------------------------
# Issued certificates will only include cases where a certificate was issued to the customer

hrt_issued_objs <- create_hes_issued_month_objects(
  db_connection = con,
  db_table_name = 'HRTPPC_FACT',
  service_area = 'HRTPPC',
  min_ym = config$min_focus_ym_hrt,
  max_ym = config$max_focus_ym_hrt,
  subtype_split = FALSE
)

# 3.5.3 HRTPPC: Age profile (latest year)------------------------------------------------
# Some processing applied to base dataset to reclassify ages that are outside of expected ranges as these may be errors

hrt_age_objs <- create_hes_age_objects(
  db_connection = con,
  db_table_name = 'HRTPPC_FACT',
  service_area = 'HRTPPC',
  min_ym = config$min_focus_ym_hrt,
  max_ym = config$max_focus_ym_hrt,
  subtype_split = FALSE
)

hrt_age_base_pop_objs <- create_px_patient_age_objects(
  db_connection = con,
  db_table_name = 'PX_PAT_FACT',
  
  patient_group = 'HRT_PATIENT_COUNT'
)

hrt_age_objs$chart_data <- hrt_age_objs$chart_data |> 
  dplyr::left_join(
    y = hrt_age_base_pop_objs$chart_data,
    by = c('Age Band')
  ) |> 
  dplyr::rename(`Estimated patients receiving HRT PPC qualifying medication (aged 16-59)` = BASE_POPULATION)
  


# 3.4.4 HRTPPC: Deprivation profile (latest year)------------------------------------------------
# IMD may not be available if the postcode cannot be mapped to NSPL

hrt_imd_objs <- create_hes_imd_objects(
  db_connection = con,
  db_table_name = 'HRTPPC_FACT',
  service_area = 'HRTPPC',
  min_ym = config$min_focus_ym_hrt,
  max_ym = config$max_focus_ym_hrt,
  subtype_split = FALSE
)

hrt_imd_base_pop_objs <- create_px_patient_imd_objects(
  db_connection = con,
  db_table_name = 'PX_PAT_FACT',
  patient_group = 'HRT_PATIENT_COUNT',
  min_age = config$hrt_min_pop_age,
  max_age = config$hrt_max_pop_age
)

hrt_imd_objs$chart_data <- hrt_imd_objs$chart_data |> 
  dplyr::left_join(
    y = hrt_imd_base_pop_objs$chart_data,
    by = c('IMD Quintile')
  ) |> 
  dplyr::rename(`Estimated patients receiving HRT PPC qualifying medication (aged 16-59)` = BASE_POPULATION)

# 3.4.5 HRTPPC: ICB profile (latest year)-------------------------------------------------
# ICBs can vary in size and therefore are not appropriate for direct comparison
# Figures should be standardised by a population denominator

# ONS only publish mid-year estimates and at a delayed schedule
# latest available population year should be defined in the config file
# hrt_icb_objs <- create_hes_icb_objects(
#   db_connection = con,
#   db_table_name = 'HRTPPC_FACT',
#   service_area = 'HRTPPC',
#   min_ym = config$min_focus_ym_hrt,
#   max_ym = config$max_focus_ym_hrt,
#   subtype_split = FALSE,
#   base_population_source = 'ONS',
#   population_min_age = config$hrt_min_pop_age, 
#   population_max_age = config$hrt_max_pop_age,
#   ons_population_year = config$ons_pop_year, 
#   ons_population_gender = "F"
# )

# ICB by NHS prescription patient count
# Limited to EPS where LSOA is captured
# Based on all patients in relevant year receiving any prescribing of HRT PPC medicines
hrt_icb_objs <- create_hes_icb_objects(
  db_connection = con,
  db_table_name = 'HRTPPC_FACT',
  service_area = 'HRTPPC',
  min_ym = config$min_focus_ym_hrt,
  max_ym = config$max_focus_ym_hrt,
  subtype_split = FALSE,
  base_population_source = 'PX',
  population_min_age = config$hrt_min_pop_age, 
  population_max_age = config$hrt_max_pop_age,
  db_px_patient_table = 'PX_PAT_FACT',
  px_population_type = 'HRT_PATIENT_COUNT'
)


# 3.6 Aggregation and analysis: Tax Credit (TAX) ---------------

# 3.6.1 TAX: Certificates issued (trend) ------------------------------------------------
# Issued certificates will only include cases where a certificate was issued to the customer

tax_issued_objs <- create_hes_issued_objects(
  db_connection = con,
  db_table_name = 'HES_FACT',
  service_area = 'TAX',
  min_ym = config$min_trend_ym_tax,
  max_ym = config$max_trend_ym_tax,
  subtype_split = FALSE
)


# 3.6.2 TAX: Age profile (latest year)------------------------------------------------
# Some processing has been performed to group by set age bands and reclassify potential errors 

tax_age_objs <- create_hes_age_objects(
  db_connection = con,
  db_table_name = 'HES_FACT',
  service_area = 'TAX',
  min_ym = config$min_focus_ym_tax,
  max_ym = config$max_focus_ym_tax,
  subtype_split = FALSE
)

# create dataframe to look at age group profile
df_tax_age_trend <- get_hes_issue_data(con, 'HES_FACT', 'TAX', config$min_trend_ym_tax, config$max_trend_ym_tax, c('SERVICE_AREA_NAME','ISSUE_FY','CUSTOM_AGE_BAND')) |>
  dplyr::mutate(AGE_GROUP = dplyr::case_when(
    CUSTOM_AGE_BAND == 'N/A' ~ 'N/A',
    CUSTOM_AGE_BAND %in% c('15-19','20-24','25-29','30-34','35-39') ~ 'Under 40',
    TRUE ~ '40 and over'
  )) |> 
  dplyr::group_by(SERVICE_AREA_NAME, ISSUE_FY, AGE_GROUP) |> 
  dplyr::summarise(ISSUED_CERTS = sum(ISSUED_CERTS), .groups = "keep") |> 
  dplyr::mutate(SORT_ORDER = dplyr::case_when(
    AGE_GROUP == 'Under 40' ~ 1,
    AGE_GROUP == '40 and over' ~ 2,
    AGE_GROUP == 'N/A' ~ 3
  ))

# produce stacked chart
ch_tax_age_trend <- df_tax_age_trend |> 
  dplyr::filter(AGE_GROUP != 'N/A') |> 
  dplyr::arrange(desc(SORT_ORDER)) |> 
  dplyr::mutate(ISSUED_CERTS_SF = signif(ISSUED_CERTS,3)) |> 
  nhsbsaVis::group_chart_hc(
    x = ISSUE_FY,
    y = ISSUED_CERTS_SF,
    type = "column",
    group = "AGE_GROUP",
    xLab = "Financial Year",
    yLab = "Number of certificates issued",
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
    }"))) |> 
  highcharter::hc_plotOptions(series = list(stacking = "normal"))

# data download
dl_tax_age_trend <- df_tax_age_trend |> 
  dplyr::arrange(ISSUE_FY, SORT_ORDER) |> 
  dplyr::select(-SORT_ORDER) |> 
  rename_df_fields()

# supporting data
sd_tax_age_trend <- get_hes_issue_data(con, 'HES_FACT', 'TAX', config$min_trend_ym_tax, config$max_trend_ym_tax, c('SERVICE_AREA_NAME','COUNTRY','ISSUE_FY','CUSTOM_AGE_BAND')) |>
  dplyr::mutate(AGE_GROUP = dplyr::case_when(
    CUSTOM_AGE_BAND == 'N/A' ~ 'N/A',
    CUSTOM_AGE_BAND %in% c('15-19','20-24','25-29','30-34','35-39') ~ 'Under 40',
    TRUE ~ '40 and over'
  )) |> 
  dplyr::group_by(SERVICE_AREA_NAME, COUNTRY, ISSUE_FY, AGE_GROUP) |> 
  dplyr::summarise(ISSUED_CERTS = sum(ISSUED_CERTS), .groups = "keep") |> 
  dplyr::mutate(SORT_ORDER = dplyr::case_when(
    AGE_GROUP == 'Under 40' ~ 1,
    AGE_GROUP == '40 and over' ~ 2,
    AGE_GROUP == 'N/A' ~ 3
  )) |> 
  dplyr::arrange(ISSUE_FY, COUNTRY, SORT_ORDER) |> 
  dplyr::select(-SORT_ORDER) |> 
  rename_df_fields()

# 3.6.3 TAX: Deprivation profile (latest year)------------------------------------------------
# IMD may not be available if the postcode cannot be mapped to NSPL

tax_imd_objs <- create_hes_imd_objects(
  db_connection = con,
  db_table_name = 'HES_FACT',
  service_area = 'TAX',
  min_ym = config$min_focus_ym_tax,
  max_ym = config$max_focus_ym_tax,
  subtype_split = FALSE
)

# 3.6.4 TAX: ICB profile (latest year)-------------------------------------------------
# ICBs can vary in size and therefore are not appropriate for direct comparison
# Figures should be standardised by a population denominator
# ONS only publish mid-year estimates and at a delayed schedule
# latest available population year should be defined in the config file

tax_icb_objs <- create_hes_icb_objects(
  db_connection = con,
  db_table_name = 'HES_FACT',
  service_area = 'TAX',
  min_ym = config$min_focus_ym_tax,
  max_ym = config$max_focus_ym_tax,
  subtype_split = FALSE,
  base_population_source = 'ONS',
  population_min_age = config$tax_min_pop_age, 
  population_max_age = config$tax_max_pop_age,
  ons_population_year = config$ons_pop_year, 
  ons_population_gender = "T"
)


# 3.7 Close database connection---------------------------------------------------------------

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
  "MAT_Outcomes",
  "MAT_Active_Certificates",
  "MAT_Certificate_Duration",
  "MED_Applications",
  "MED_Outcomes",
  "MED_Active_Certificates",
  "PPC_Applications",
  "PPC_Outcomes",
  "TAX_Outcomes"
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
  lis_application_objs$support_data,
  30
)

# apply formatting
# left align columns A to B
accessibleTables::format_data(wb,
                              "LIS_Applications",
                              c("A", "B","C"),
                              "left",
                              "")

# right align columns
accessibleTables::format_data(
  wb,
  "LIS_Applications",
  c(
    "D"
  ),
  "right",
  "#,###"
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
    "Maternity exemption certificates are only available for people living in England."
  ),
  mat_application_objs$support_data,
  30
)

# apply formatting
# left align columns
accessibleTables::format_data(wb,
                              "MAT_Applications",
                              c("A","B"),
                              "left",
                              "")

# right align columns
accessibleTables::format_data(
  wb,
  "MAT_Applications",
  "C",
  "right",
  "#,###"
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
    "Medical exemption certificates are only available for people living in England."
  ),
  med_application_objs$support_data,
  30
)

# apply formatting
# left align columns A to B
accessibleTables::format_data(wb,
                              "MED_Applications",
                              c("A","B"),
                              "left",
                              "")

# right align columns
accessibleTables::format_data(
  wb,
  "MED_Applications",
  "C",
  "right",
  "#,###"
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
    "Prescription prepayment certificates are only available for people living in England.",
    "A certificate duration of 'unknown' has been used where the certificate duration cannot be identified as 3 or 12 months from the available application details."
  ),
  ppc_application_objs$support_data,
  30
)

# apply formatting
# left align columns A to B
accessibleTables::format_data(wb,
                              "PPC_Applications",
                              c("A", "B", "C","D"),
                              "left",
                              "")

# right align columns
accessibleTables::format_data(
  wb,
  "PPC_Applications",
  c(
    "E"
  ),
  "right",
  "#,###"
)


# 4.3 Issued/Outcome ------------------------------------------------------

# 4.3.1 Issued: Low Income Scheme -----------------------------------------

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
  lis_issued_objs$support_data,
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
  "#,###"
)

# 4.3.2 Issued: MATEX -----------------------------------------------------

# create the sheet
accessibleTables::write_sheet(
  wb,
  "MAT_Outcomes",
  paste0(
    config$publication_table_title,
    " - Number of maternity exemption certificates issued, split by financial year and country"
  ),
  c(
    "Results limited to cases where the application has been fully processed, and a certificate issued to the applicant.",
    "Maternity exemption certificates are only available for people living in England."
  ),
  mat_issued_objs$support_data,
  30
)

# apply formatting
# left align columns A to B
accessibleTables::format_data(wb,
                              "MAT_Outcomes",
                              c("A", "B"),
                              "left",
                              "")

# right align columns
accessibleTables::format_data(
  wb,
  "MAT_Outcomes",
  c(
    "C"
  ),
  "right",
  "#,###"
)

# 4.3.2 Issued: MEDEX -----------------------------------------------------

# create the sheet
accessibleTables::write_sheet(
  wb,
  "MED_Outcomes",
  paste0(
    config$publication_table_title,
    " - Number of medical exemption certificates issued, split by financial year and country"
  ),
  c(
    "Results limited to cases where the application has been fully processed, and a certificate issued to the applicant.",
    "Medical exemption certificates are only available for people living in England."
  ),
  med_issued_objs$support_data,
  30
)

# apply formatting
# left align columns A to B
accessibleTables::format_data(wb,
                              "MED_Outcomes",
                              c("A", "B"),
                              "left",
                              "")

# right align columns
accessibleTables::format_data(
  wb,
  "MED_Outcomes",
  c(
    "C"
  ),
  "right",
  "#,###"
)

# 4.2.4 Issued: PPC ---------------------------------------------

# create the sheet
accessibleTables::write_sheet(
  wb,
  "PPC_Outcomes",
  paste0(
    config$publication_table_title,
    " - Number of prescription prepayment certificates issued, split by financial year and certificate duration"
  ),
  c(
    "Prescription prepayment certificates are only available for people living in England.",
    "A certificate duration of 'unknown' has been used where the certificate duration cannot be identified as 3 or 12 months from the available application details.",
    "Includes the number of certificates that were issued to customers, regardless if the certificate was ever cancelled or revoked."
  ),
  ppc_issued_objs$support_data,
  30
)

# apply formatting
# left align columns A to B
accessibleTables::format_data(wb,
                              "PPC_Outcomes",
                              c("A", "B", "C","D"),
                              "left",
                              "")

# right align columns
accessibleTables::format_data(
  wb,
  "PPC_Outcomes",
  c(
    "E"
  ),
  "right",
  "#,###"
)

# 4.2.5 Issued: Tax Credit ---------------------------------------------

# create the sheet
accessibleTables::write_sheet(
  wb,
  "TAX_Outcomes",
  paste0(
    config$publication_table_title,
    " - Number of Tax Credit Exemption Certificates issued, split by financial year and country"
  ),
  c(
    "Certificates issued to residents of Scotland, Wales and Northern Ireland have been reported as 'Other'.",
    "Country is reported as 'Unknown' if the postcode for the certificate holder cannot be mapped to the National Statistics Postcode Lookup (NSPL)."
  ),
  tax_issued_objs$support_data,
  30
)

# apply formatting
# left align columns A to B
accessibleTables::format_data(wb,
                              "TAX_Outcomes",
                              c("A", "B"),
                              "left",
                              "")

# right align columns
accessibleTables::format_data(
  wb,
  "TAX_Outcomes",
  c(
    "C"
  ),
  "right",
  "#,###"
)

# 4.4 Active --------------------------------------------------------------

# 4.4.1 Active: LIS -------------------------------------------------------

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
  lis_active_objs$support_data,
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
  "#,###"
)

# 4.4.2 Active: MAT -------------------------------------------------------

# create the sheet
accessibleTables::write_sheet(
  wb,
  "MAT_Active_Certificates",
  paste0(
    config$publication_table_title,
    " - Number of active maternity exemption certificates split by financial year"
  ),
  c(
    "Certificates can be valid during pregnancy and upto one year following birth. Therefore some certificates may be represented in figures for multiple years.",
    "Maternity exemption certificates are only available for people living in England."
  ),
  mat_active_objs$support_data,
  30
)

# apply formatting
# left align columns A to B
accessibleTables::format_data(wb,
                              "MAT_Active_Certificates",
                              c("A", "B"),
                              "left",
                              "")

# right align columns
accessibleTables::format_data(
  wb,
  "MAT_Active_Certificates",
  c(
    "C"
  ),
  "right",
  "#,###"
)

# 4.4.3 Active: MED -------------------------------------------------------

# create the sheet
accessibleTables::write_sheet(
  wb,
  "MED_Active_Certificates",
  paste0(
    config$publication_table_title,
    " - Number of active medical exemption certificates split by financial year"
  ),
  c(
    "Certificates are usually valid for five years and therefore certificates may be represented in figures for multiple years.",
    "Medical exemption certificates are only available for people living in England."
  ),
  med_active_objs$support_data,
  30
)

# apply formatting
# left align columns A to B
accessibleTables::format_data(wb,
                              "MED_Active_Certificates",
                              c("A", "B"),
                              "left",
                              "")

# right align columns
accessibleTables::format_data(
  wb,
  "MED_Active_Certificates",
  c(
    "C"
  ),
  "right",
  "#,###"
)

# 4.5 Duration --------------------------------------------------------------

# 4.5.1 Duration: LIS -----------------------------------------------------

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
  lis_duration_objs$support_data,
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
  "#,###"
)

# 4.5.2 Duration: MAT -----------------------------------------------------

# create the sheet
accessibleTables::write_sheet(
  wb,
  "MAT_Certificate_Duration",
  paste0(
    config$publication_table_title,
    " - proportion of maternity exemption certificates issued relative to expected due date, split by financial year and country"
  ),
  c(
    "Results limited to cases where the application has been fully processed, and a certificate issued to the applicant.",
    "Number of months based on difference between due date and date of certificate issue, rounded to the nearest number of months.",
    "Due date will be supplied during application and therefore may not exactly match delivery date.",
    "Number of months reported as 'NA' where captured due date would produce a value outside of the expected range." 
  ),
  sd_mat_duration,
  30
)

# apply formatting
# left align columns A to B
accessibleTables::format_data(wb,
                              "MAT_Certificate_Duration",
                              c("A", "B"),
                              "left",
                              "")

# right align columns
accessibleTables::format_data(
  wb,
  "MAT_Certificate_Duration",
  c(
    "C",
    "D",
    "E"
  ),
  "right",
  "#,###"
)

# right align columns
accessibleTables::format_data(
  wb,
  "MAT_Certificate_Duration",
  c(
    "F"
  ),
  "right",
  "#,###.0"
)


# 4.6 Age --------------------------------------------------------------

# 4.6.1 Age: LIS ----------------------------------------------------------

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
  lis_age_objs$support_data,
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
  "#,###"
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
  lis_imd_objs$support_data,
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
  "#,###"
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
  lis_icb_objs$support_data,
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
  "#,###"
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
    "Table 9: Maternity exemption certificate - Issued certificates",
    "Table 10: Maternity exemption certificate - Certificate duration",
    "Table 10: Medical exemption certificate - Applications",
    "Table 11: Medical exemption certificate - Issued certificates",
    "Table 12: PPC - Applications"
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
