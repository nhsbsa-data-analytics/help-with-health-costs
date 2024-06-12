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
if(config$rebuild_base_lis_data == TRUE){
  create_dataset_from_sql(
    db_connection = con,
    path_to_sql_file = "./SQL/HWHC_LIS_FACT.sql",
    db_table_name = "HWHC_LIS_FACT",
    ls_variables = list(
      var = c("p_extract_date"),
      val = c(config$extract_date_lis)
    )
  )
}

# 2.2 Data Import: HES (MATEX, MEDEX, PPC & Tax Credits) ----------------------------------

# create the base dataset for HES areas (PPC, MATEX, MEDEX and Tax Credit)
# data at individual case level (ID) for all applications/certificates
if(config$rebuild_base_hes_data == TRUE){
  create_dataset_from_sql(
    db_connection = con,
    path_to_sql_file = "./SQL/HWHC_HES_FACT.sql",
    db_table_name = "HWHC_HES_FACT",
    ls_variables = list(
      var = c("p_extract_date"),
      val = c(config$extract_date_hes)
    )
  )
}

# 2.3 Data Import: HRT PPC ----------------------------------

# create the base dataset for HRT PPC
# data at individual case level (ID) for all applications/certificates
if(config$rebuild_base_hrt_data == TRUE){
  create_dataset_from_sql(
    db_connection = con,
    path_to_sql_file = "./SQL/HWHC_HRTPPC_FACT.sql",
    db_table_name = "HWHC_HRTPPC_FACT",
    ls_variables = list(
      var = c("p_extract_date"),
      val = c(config$extract_date_hrt)
    )
  )
}

# 2.4 Data Import: PX PATIENT COUNTS ----------------------------------

# create the base dataset for prescription patient counts
# data at aggregated level showing counts of patients from the NHS prescription data
if(config$rebuild_base_px_data == TRUE){
  create_dataset_from_sql(
    db_connection = con,
    path_to_sql_file = "./SQL/HWHC_PX_PAT_FACT.sql",
    db_table_name = "HWHC_PX_PAT_FACT",
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

# not required for narrative and only required for supporting data

lis_application_objs <- create_hes_application_objects(
  db_connection = con,
  db_table_name = 'HWHC_LIS_FACT',
  service_area = 'LIS',
  min_ym = config$min_trend_ym_lis,
  max_ym = config$max_trend_ym_lis,
  subtype_split = FALSE
)


# 3.1.2 LIS: Certificates issued (trend)-------------------------------------------------
# A certificate will only be issued when the application is completed and assessed
# If applicants qualify for support they will be issued a HC2/HC3 certificate
# Not all applications will reach the "outcome/decision" stage as applicants may drop out during the application/assessment process
# For HC3 this will include cases where a HBD11 was issued to the applicant

lis_issued_objs <- create_hes_issued_objects(
  db_connection = con,
  db_table_name = 'HWHC_LIS_FACT',
  service_area = 'LIS',
  min_ym = config$min_trend_ym_lis,
  max_ym = config$max_trend_ym_lis,
  subtype_split = TRUE
)

# 3.1.3 LIS: Duration (latest year)-------------------------------------------------
# The duration of HC2/HC3 certificates will vary based on applicants circumstances
# This reporting is only applicable for issued HC2/HC3 certificates
# CERTIFICATE_DURATION in the data uses some grouping categories to bundle certificates into common categories

# not required for narrative and only required for supporting data

lis_duration_objs <- create_hes_duration_objects(
  db_connection = con,
  db_table_name = 'HWHC_LIS_FACT',
  service_area = 'LIS',
  min_ym = config$min_focus_ym_lis,
  max_ym = config$max_focus_ym_lis,
  subtype_split = TRUE
)

# 3.1.4 LIS: Age profile (latest year)-------------------------------------------------
# Age is available for the lead applicant only
# Some processing has been performed to group by set age bands and reclassify potential errors 

lis_age_objs <- create_hes_age_objects(
  db_connection = con,
  db_table_name = 'HWHC_LIS_FACT',
  service_area = 'LIS',
  min_ym = config$min_focus_ym_lis,
  max_ym = config$max_focus_ym_lis,
  subtype_split = TRUE
)

# 3.1.5 LIS: Deprivation profile (latest year)-------------------------------------------------
# IMD may not be available if the postcode cannot be mapped to NSPL

lis_imd_objs <- create_hes_imd_objects(
  db_connection = con,
  db_table_name = 'HWHC_LIS_FACT',
  service_area = 'LIS',
  min_ym = config$min_focus_ym_lis,
  max_ym = config$max_focus_ym_lis,
  subtype_split = TRUE
)

# 3.1.6 LIS: ICB profile (latest year)-------------------------------------------------
# ICBs can vary in size and therefore are not appropriate for direct comparison
# Figures should be standardised by a population denominator
# ONS only publish mid-year estimates and at a delayed schedule
# latest available population year should be defined in the config file

lis_icb_objs <- create_hes_icb_objects(
  db_connection = con,
  db_table_name = 'HWHC_LIS_FACT',
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

# not required for narrative and only required for supporting data

mat_application_objs <- create_hes_application_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'MAT',
  min_ym = config$min_trend_ym_mat,
  max_ym = config$max_trend_ym_mat,
  subtype_split = FALSE
)

# 3.2.2 MATEX: Certificates issued (trend)------------------------------------------------
# Issued certificates will only include cases where a certificate was issued to the customer

mat_issued_objs <- create_hes_issued_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'MAT',
  min_ym = config$min_trend_ym_mat,
  max_ym = config$max_trend_ym_mat,
  subtype_split = FALSE
)

# 3.2.3 MATEX: Duration (latest year)------------------------------------------------
# Looking at certificates issued in the latest FY
# Split by time between due date and certificate issue

# not required for narrative and only required for supporting data

mat_duration_objs <- create_matex_duration_objects(
  db_connection = con,
  min_ym = config$min_focus_ym_mat,
  max_ym = config$max_focus_ym_mat
)

# 3.2.4 MATEX: Age profile (latest year)------------------------------------------------
# Issued certificates will only include cases where a certificate was issued to the customer
# Some processing has been performed to group by set age bands and reclassify potential errors 

mat_age_objs <- create_hes_age_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
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

# add ONS data to table, chart data and supporting data

# kable table needs rebuilding
mat_age_objs$table <- mat_age_objs$chart_data |> 
  dplyr::select(-"HwHC Service",-"Financial Year") |> 
  dplyr::inner_join(
    y = df_births_age,
    by = c(`Age Band` = "AGE_BAND")
  ) |> 
  dplyr::rename(!!paste0("ONS Live Births (", config$ons_births_year, ")") := LIVE_BIRTHS) |> 
  knitr::kable(
    align = "lrr",
    format.args = list(big.mark = ",")
  )

# update the chart_data
mat_age_objs$chart_data <- mat_age_objs$chart_data |> 
  dplyr::left_join(
    y = df_births_age,
    by = c(`Age Band` = "AGE_BAND")
  ) |> 
  dplyr::rename(!!paste0("ONS Live Births (", config$ons_births_year, ")") := LIVE_BIRTHS)

# update the support_data
mat_age_objs$support_data <- mat_age_objs$support_data |> 
  dplyr::left_join(
    y = df_births_age,
    by = c(`Age Band` = "AGE_BAND")
  ) |> 
  dplyr::rename(!!paste0("ONS Live Births (", config$ons_births_year, ")") := LIVE_BIRTHS)


# 3.2.5 MATEX: Deprivation profile (latest year)------------------------------------------------
# IMD may not be available if the postcode cannot be mapped to NSPL

mat_imd_objs <- create_hes_imd_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'MAT',
  min_ym = config$min_focus_ym_mat,
  max_ym = config$max_focus_ym_mat,
  subtype_split = FALSE
)

# for additional context identify the number of live births published by ONS
df_births_imd <- get_ons_live_birth_imd_data(con, config$ons_birth_imd_table, "IMD_QUINTILE", config$ons_births_year) |> 
  dplyr::mutate(IMD_QUINTILE)

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

# add ONS data to table, chart data and supporting data

# kable table needs rebuilding
mat_imd_objs$table <- mat_imd_objs$chart_data |> 
  dplyr::select(-"HwHC Service",-"Financial Year") |> 
  dplyr::inner_join(
    y = df_births_imd |> dplyr::mutate(IMD_QUINTILE = as.character(IMD_QUINTILE)),
    by = c(`IMD Quintile` = "IMD_QUINTILE")
  ) |> 
  dplyr::rename(!!paste0("ONS Live Births (", config$ons_births_year, ")") := LIVE_BIRTHS) |> 
  knitr::kable(
    align = "lrr",
    format.args = list(big.mark = ",")
  )

# update the chart_data
mat_imd_objs$chart_data <- mat_imd_objs$chart_data |> 
  dplyr::left_join(
    y = df_births_imd |> dplyr::mutate(IMD_QUINTILE = as.character(IMD_QUINTILE)),
    by = c(`IMD Quintile` = "IMD_QUINTILE")
  ) |> 
  dplyr::rename(!!paste0("ONS Live Births (", config$ons_births_year, ")") := LIVE_BIRTHS)

# update the support_data
mat_imd_objs$support_data <- mat_imd_objs$support_data |> 
  dplyr::left_join(
    y = df_births_imd |> dplyr::mutate(IMD_QUINTILE = as.character(IMD_QUINTILE)),
    by = c(`IMD Quintile` = "IMD_QUINTILE")
  ) |> 
  dplyr::rename(!!paste0("ONS Live Births (", config$ons_births_year, ")") := LIVE_BIRTHS)

# 3.2.6 MATEX: ICB profile (latest year)-------------------------------------------------
# ICBs can vary in size and therefore are not appropriate for direct comparison
# Figures should be standardised by a population denominator
# ONS only publish mid-year estimates and at a delayed schedule
# latest available population year should be defined in the config file

mat_icb_objs <- create_hes_icb_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
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

# not required for narrative and only required for supporting data

med_application_objs <- create_hes_application_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'MED',
  min_ym = config$min_trend_ym_med,
  max_ym = config$max_trend_ym_med,
  subtype_split = FALSE
)

# 3.3.2 MEDEX: Certificates issued (trend)------------------------------------------------
# Issued certificates will only include cases where a certificate was issued to the customer

med_issued_objs <- create_hes_issued_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'MED',
  min_ym = config$min_trend_ym_med,
  max_ym = config$max_trend_ym_med,
  subtype_split = FALSE
)

# 3.3.3 MEDEX: Age profile (latest year)------------------------------------------------
# Some processing has been performed to group by set age bands and reclassify potential errors

med_age_objs <- create_hes_age_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'MED',
  min_ym = config$min_focus_ym_med,
  max_ym = config$max_focus_ym_med,
  subtype_split = FALSE
)

# 3.3.4 MEDEX: Deprivation profile (latest year)------------------------------------------------
# IMD may not be available if the postcode cannot be mapped to NSPL

med_imd_objs <- create_hes_imd_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'MED',
  min_ym = config$min_focus_ym_med,
  max_ym = config$max_focus_ym_med,
  subtype_split = FALSE
)

# 3.3.5 MEDEX: ICB profile (latest year)-------------------------------------------------
# ICBs can vary in size and therefore are not appropriate for direct comparison
# Figures should be standardised by a population denominator


# ICB by ONS population denominator
# ONS only publish mid-year estimates and at a delayed schedule
# latest available population year should be defined in the config file

# med_icb_objs <- create_hes_icb_objects(
#   db_connection = con,
#   db_table_name = 'HWHC_HES_FACT',
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
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'MED',
  min_ym = config$min_focus_ym_med,
  max_ym = config$max_focus_ym_med,
  subtype_split = FALSE,
  base_population_source = 'PX',
  population_min_age = config$med_min_pop_age, 
  population_max_age = config$med_max_pop_age,
  db_px_patient_table = 'HWHC_PX_PAT_FACT',
  px_population_type = 'PATIENT_COUNT'
)


# 3.4 Aggregation and analysis: Prescription Prepayment Certificate (PPC) ---------------


# 3.4.1 PPC: Applications received (trend) ------------------------------------------------
# Applications will include any application to the scheme regardless of current status or outcome
# Applications will be for a specific certificate type and therefore can be split

# not required for narrative and only required for supporting data

ppc_application_objs <- create_hes_application_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'PPC',
  min_ym = config$min_trend_ym_ppc,
  max_ym = config$max_trend_ym_ppc,
  subtype_split = TRUE
)

# 3.4.2 PPC: Certificates issued (trend)------------------------------------------------
# Issued certificates will only include cases where a certificate was issued to the customer

ppc_issued_objs <- create_hes_issued_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'PPC',
  min_ym = config$min_trend_ym_ppc,
  max_ym = config$max_trend_ym_ppc,
  subtype_split = TRUE
)

# 3.4.3 PPC: Age profile (latest year)------------------------------------------------
# Some processing applied to base dataset to reclassify ages that are outside of expected ranges as these may be errors

ppc_age_objs <- create_hes_age_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'PPC',
  min_ym = config$min_focus_ym_ppc,
  max_ym = config$max_focus_ym_ppc,
  subtype_split = TRUE
)

# 3.4.4 PPC: Deprivation profile (latest year)------------------------------------------------
# IMD may not be available if the postcode cannot be mapped to NSPL

ppc_imd_objs <- create_hes_imd_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
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
#   db_table_name = 'HWHC_HES_FACT',
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
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'PPC',
  min_ym = config$min_focus_ym_ppc,
  max_ym = config$max_focus_ym_ppc,
  subtype_split = TRUE,
  base_population_source = 'PX',
  population_min_age = config$ppc_min_pop_age, 
  population_max_age = config$ppc_max_pop_age,
  db_px_patient_table = 'HWHC_PX_PAT_FACT',
  px_population_type = 'PATIENT_COUNT'
)

# 3.5 Aggregation and analysis: HRT PPC (HRTPPC) ---------------------

# 3.5.1 HRTPPC: Applications received (trend) --------------------------------------------------
# Applications will include any application to the scheme regardless of current status or outcome

# not required for narrative and only required for supporting data

hrt_application_objs <- create_hes_application_month_objects(
  db_connection = con,
  db_table_name = 'HWHC_HRTPPC_FACT',
  service_area = 'HRTPPC',
  min_ym = config$min_focus_ym_hrt,
  max_ym = config$max_focus_ym_hrt,
  subtype_split = FALSE
)

# 3.5.2 HRTPPC: Certificates issued (trend) --------------------------------------------------
# Issued certificates will only include cases where a certificate was issued to the customer

hrt_issued_objs <- create_hes_issued_month_objects(
  db_connection = con,
  db_table_name = 'HWHC_HRTPPC_FACT',
  service_area = 'HRTPPC',
  min_ym = config$min_focus_ym_hrt,
  max_ym = config$max_focus_ym_hrt,
  subtype_split = FALSE
)

# identify the proportion of issued certificates post-dated to the next month
df_hrt_issued_postdate <- dplyr::tbl(
  con, 
  from = dbplyr::in_schema(toupper(con@info$username), 'HWHC_HRTPPC_FACT')
) |> 
  # filter to service area and time periods
  dplyr::filter(
    SERVICE_AREA == toupper('HRTPPC'),
    ISSUE_YM >= config$min_focus_ym_hrt,
    ISSUE_YM <= config$max_focus_ym_hrt,
    CERTIFICATE_ISSUED_FLAG == 1
  ) |> 
  dplyr::group_by(ISSUE_YM) |> 
  dplyr::summarise(
    ISSUED_CERTS = n(),
    POST_DATE_CERTS = sum(FLAG_START_FOLLOWING_MONTH)
  ) |> 
  dplyr::ungroup() |> 
  dplyr::mutate(PROP_CERTS_POSTDATE = round(POST_DATE_CERTS/ISSUED_CERTS*100,1)) |> 
  dplyr::collect() |> 
  dplyr::arrange(ISSUE_YM) |> 
  dplyr::mutate(ISSUE_YM = format(as.Date(paste0(ISSUE_YM,'01'), '%Y%m%d'), '%b-%y'))

# create a column chart
ch_hrt_issued_postdate <- df_hrt_issued_postdate |> 
  nhsbsaVis::basic_chart_hc(
    x = ISSUE_YM,
    y = PROP_CERTS_POSTDATE,
    type = "column",
    xLab = "Month",
    yLab = "Certificates post-dated to start the following month (%)",
    seriesName = "Proportion of issued certificates",
    title = "",
    dlOn = FALSE
  ) |> 
  highcharter::hc_tooltip(
    enabled = T,
    shared = T,
    sort = T
  ) |>
  highcharter::hc_yAxis(labels = list(enabled = TRUE))

# update the table object
hrt_issued_objs$table <- df_hrt_issued_postdate |>
  dplyr::select(-POST_DATE_CERTS) |> 
  rename_df_fields() |> 
  knitr::kable(
    align = "lrr",
    format.args = list(big.mark = ",")
  )

# update the chart data object
hrt_issued_objs$chart_data <- hrt_issued_objs$chart_data |> 
  dplyr::left_join(
    y = df_hrt_issued_postdate |> dplyr::select(-ISSUED_CERTS),
    by = c("Month" = "ISSUE_YM")
  ) |> 
  rename_df_fields()

# update the support data object
hrt_issued_objs$support_data <- hrt_issued_objs$support_data |> 
  dplyr::left_join(
    y = df_hrt_issued_postdate |> dplyr::select(-ISSUED_CERTS),
    by = c("Month" = "ISSUE_YM")
  ) |> 
  rename_df_fields()


# 3.5.3 HRTPPC: Age profile (latest year)------------------------------------------------
# Some processing applied to base dataset to reclassify ages that are outside of expected ranges as these may be errors

# create the age objects
hrt_age_objs <- create_hes_age_objects(
  db_connection = con,
  db_table_name = 'HWHC_HRTPPC_FACT',
  service_area = 'HRTPPC',
  min_ym = config$min_focus_ym_hrt,
  max_ym = config$max_focus_ym_hrt,
  subtype_split = FALSE
)

# identify the base population figures
hrt_age_base_pop_objs <- create_px_patient_age_objects(
  db_connection = con,
  db_table_name = 'HWHC_PX_PAT_FACT',
  patient_group = 'HRT_PATIENT_COUNT'
)

# update the table (need to rebuild from chart data)
hrt_age_objs$table <- hrt_age_objs$chart_data |> 
  dplyr::select(-"HwHC Service", -"Financial Year") |> 
  dplyr::inner_join(
    y = hrt_age_base_pop_objs$chart_data,
    by = c('Age Band')
  ) |> 
  dplyr::rename(`Estimated patients receiving HRT PPC qualifying medication` = BASE_POPULATION) |> 
  knitr::kable(
    align = "lrr",
    format.args = list(big.mark = ",")
  )

# update the chart_data
hrt_age_objs$chart_data <- hrt_age_objs$chart_data |> 
  dplyr::left_join(
    y = hrt_age_base_pop_objs$chart_data,
    by = c('Age Band')
  ) |> 
  dplyr::rename(`Estimated patients receiving HRT PPC qualifying medication` = BASE_POPULATION)

# update the support_data
hrt_age_objs$support_data <- hrt_age_objs$support_data |> 
  dplyr::left_join(
    y = hrt_age_base_pop_objs$chart_data,
    by = c('Age Band')
  ) |> 
  dplyr::rename(`Estimated patients receiving HRT PPC qualifying medication (aged 16-59)` = BASE_POPULATION)

# 3.4.4 HRTPPC: Deprivation profile (latest year)------------------------------------------------
# IMD may not be available if the postcode cannot be mapped to NSPL

# create the IMD objects
hrt_imd_objs <- create_hes_imd_objects(
  db_connection = con,
  db_table_name = 'HWHC_HRTPPC_FACT',
  service_area = 'HRTPPC',
  min_ym = config$min_focus_ym_hrt,
  max_ym = config$max_focus_ym_hrt,
  subtype_split = FALSE
)

# identify the base population figures
hrt_imd_base_pop_objs <- create_px_patient_imd_objects(
  db_connection = con,
  db_table_name = 'HWHC_PX_PAT_FACT',
  patient_group = 'HRT_PATIENT_COUNT',
  min_age = config$hrt_min_pop_age,
  max_age = config$hrt_max_pop_age
)

# update the table (need to rebuild from chart data)
hrt_imd_objs$table <- hrt_imd_objs$chart_data |> 
  dplyr::select(-"HwHC Service", -"Financial Year") |> 
  dplyr::inner_join(
    y = hrt_imd_base_pop_objs$chart_data |> dplyr::mutate(`IMD Quintile` = as.character(`IMD Quintile`)),
    by = c('IMD Quintile')
  ) |> 
  dplyr::rename(`Estimated patients receiving HRT PPC qualifying medication (aged 16-59)` = BASE_POPULATION) |> 
  knitr::kable(
    align = "lrr",
    format.args = list(big.mark = ",")
  )

# update the chart_data
hrt_imd_objs$chart_data <- hrt_imd_objs$chart_data |> 
  dplyr::left_join(
    y = hrt_imd_base_pop_objs$chart_data |> dplyr::mutate(`IMD Quintile` = as.character(`IMD Quintile`)),
    by = c('IMD Quintile')
  ) |> 
  dplyr::rename(`Estimated patients receiving HRT PPC qualifying medication (aged 16-59)` = BASE_POPULATION)

# update the support_data
hrt_imd_objs$support_data <- hrt_imd_objs$support_data |> 
  dplyr::left_join(
    y = hrt_imd_base_pop_objs$chart_data |> dplyr::mutate(`IMD Quintile` = as.character(`IMD Quintile`)),
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
#   db_table_name = 'HWHC_HRTPPC_FACT',
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
  db_table_name = 'HWHC_HRTPPC_FACT',
  service_area = 'HRTPPC',
  min_ym = config$min_focus_ym_hrt,
  max_ym = config$max_focus_ym_hrt,
  subtype_split = FALSE,
  base_population_source = 'PX',
  population_min_age = config$hrt_min_pop_age, 
  population_max_age = config$hrt_max_pop_age,
  db_px_patient_table = 'HWHC_PX_PAT_FACT',
  px_population_type = 'HRT_PATIENT_COUNT'
)


# 3.6 Aggregation and analysis: Tax Credit (TAX) ---------------

# 3.6.1 TAX: Certificates issued (trend) ------------------------------------------------
# Issued certificates will only include cases where a certificate was issued to the customer

tax_issued_objs <- create_hes_issued_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'TAX',
  min_ym = config$min_trend_ym_tax,
  max_ym = config$max_trend_ym_tax,
  subtype_split = FALSE
)


# 3.6.2 TAX: Age profile (latest year)------------------------------------------------
# Some processing has been performed to group by set age bands and reclassify potential errors 

tax_age_objs <- create_hes_age_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'TAX',
  min_ym = config$min_focus_ym_tax,
  max_ym = config$max_focus_ym_tax,
  subtype_split = FALSE
)

# create dataframe to look at age group profile
df_tax_age_trend <- get_hes_issue_data(con, 'HWHC_HES_FACT', 'TAX', config$min_trend_ym_tax, config$max_trend_ym_tax, c('SERVICE_AREA_NAME','ISSUE_FY','CUSTOM_AGE_BAND')) |>
  dplyr::mutate(AGE_GROUP = dplyr::case_when(
    CUSTOM_AGE_BAND == 'Not Available' ~ 'Not Available',
    CUSTOM_AGE_BAND %in% c('15-19','20-24','25-29','30-34','35-39') ~ 'Under 40',
    TRUE ~ '40 and over'
  )) |> 
  dplyr::group_by(SERVICE_AREA_NAME, ISSUE_FY, AGE_GROUP) |> 
  dplyr::summarise(ISSUED_CERTS = sum(ISSUED_CERTS), .groups = "keep") |> 
  dplyr::mutate(SORT_ORDER = dplyr::case_when(
    AGE_GROUP == 'Under 40' ~ 1,
    AGE_GROUP == '40 and over' ~ 2,
    AGE_GROUP == 'Not Available' ~ 3
  ))

# produce stacked chart
ch_tax_age_trend <- df_tax_age_trend |> 
  dplyr::filter(AGE_GROUP != 'Not Available') |> 
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

# produce table
tbl_tax_age_trend <- df_tax_age_trend |> 
  dplyr::ungroup() |> 
  dplyr::filter(AGE_GROUP != 'Not Available') |> 
  dplyr::select(ISSUE_FY,AGE_GROUP, ISSUED_CERTS) |> 
  dplyr::arrange(AGE_GROUP, ISSUE_FY) |> 
  rename_df_fields() |> 
  knitr::kable(
    align = "llr",
    format.args = list(big.mark = ",")
  )

# data download
dl_tax_age_trend <- df_tax_age_trend |> 
  dplyr::arrange(ISSUE_FY, SORT_ORDER) |> 
  dplyr::select(-SORT_ORDER) |> 
  rename_df_fields()

# supporting data
sd_tax_age_trend <- get_hes_issue_data(con, 'HWHC_HES_FACT', 'TAX', config$min_trend_ym_tax, config$max_trend_ym_tax, c('SERVICE_AREA_NAME','COUNTRY','ISSUE_FY','CUSTOM_AGE_BAND')) |>
  dplyr::mutate(AGE_GROUP = dplyr::case_when(
    CUSTOM_AGE_BAND == 'Not Available' ~ 'Not Available',
    CUSTOM_AGE_BAND %in% c('15-19','20-24','25-29','30-34','35-39') ~ 'Under 40',
    TRUE ~ '40 and over'
  )) |> 
  dplyr::group_by(SERVICE_AREA_NAME, COUNTRY, ISSUE_FY, AGE_GROUP) |> 
  dplyr::summarise(ISSUED_CERTS = sum(ISSUED_CERTS), .groups = "keep") |> 
  dplyr::mutate(SORT_ORDER = dplyr::case_when(
    AGE_GROUP == 'Under 40' ~ 1,
    AGE_GROUP == '40 and over' ~ 2,
    AGE_GROUP == 'Not Available' ~ 3
  )) |> 
  dplyr::arrange(ISSUE_FY, COUNTRY, SORT_ORDER) |> 
  dplyr::select(-SORT_ORDER) |> 
  rename_df_fields()

# 3.6.3 TAX: Deprivation profile (latest year)------------------------------------------------
# IMD may not be available if the postcode cannot be mapped to NSPL

tax_imd_objs <- create_hes_imd_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
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
  db_table_name = 'HWHC_HES_FACT',
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
  "MATEX_Applications",
  "MATEX_Issued",
  "MATEX_Certificate_Duration",
  "MATEX_Age_Breakdown",
  "MATEX_Deprivation_Breakdown",
  "MATEX_ICB_Breakdown",
  "MEDEX_Applications",
  "MEDEX_Issued",
  "MEDEX_Age_Breakdown",
  "MEDEX_Deprivation_Breakdown",
  "MEDEX_ICB_Breakdown",
  "TAX_Issued",
  "TAX_Age_Breakdown",
  "TAX_Age_Profile",
  "TAX_Deprivation_Breakdown",
  "TAX_ICB_Breakdown",
  "LIS_Applications",
  "LIS_Issued",
  "LIS_Certificate_Duration",
  "LIS_Age_Breakdown",
  "LIS_Deprivation_Breakdown",
  "LIS_ICB_Breakdown",
  "PPC_Applications",
  "PPC_Issued",
  "PPC_Age_Breakdown",
  "PPC_Deprivation_Breakdown",
  "PPC_ICB_Breakdown",
  "HRTPPC_Applications",
  "HRTPPC_Issued",
  "HRTPPC_Age_Breakdown",
  "HRTPPC_Deprivation_Breakdown",
  "HRTPPC_ICB_Breakdown"
)

wb <- accessibleTables::create_wb(sheetNames)

# create metadata tab (will need to open file and auto row heights once ran)
meta_fields <- c(
  "HwHC Service",
  "Financial Year",
  "Country",
  "Country: England",
  "Country: Other",
  "Country: Unknown",
  "Certificate Type",
  "Certificate Type: HC2 (NHS Low Income Scheme)",
  "Certificate Type: HC3 (NHS Low Income Scheme)",
  "Certificate Type: No certificate issued (NHS Low Income Scheme)",
  "Certificate Type: 12-month (PPC)",
  "Certificate Type: 3-month (PPC)",
  "Certificate Duration",
  "Age Band",
  "IMD Quintile",
  "ICB Code",
  "ICB Name",
  "Number of applications received",
  "Number of certificates issued",
  "ONS population estimate (...)",
  "Population estimate (...)",
  "Number of issued certificates per 10,000 population",
  "Number of certificates issued (cumulative)",
  "Proportion of certificates issued (cumulative %)",
  "ONS Live Births (...)",
  "Number of issued certificates post-dated to start the following month",
  "Issued certificates post-dated to start the following month (%)",
  "Estimated patients receiving HRT PPC qualifying medication (aged 16-59)"
  
)

meta_descs <-
  c(
    "Name of NHS BSA administered service.",
    "Financial year the activity can be assigned to.",
    "Country classification based on the applicants residential address, using mapping via the National Statistics Postcode Lookup (NSPL). Not included for services typically available to English residents only.",
    "Where the applicants residential address can be aligned to an English postcode via the NSPL",
    "Where the applicants residential address can be assigned to a country other than England via the NSPL, the country will be recorded as 'Other'.",
    "Where the applicants residential address cannot be assigned to any country via the NSPL, the country will be recorded as 'Unknown'.",
    "Where distinct certificate types are available, this field will show which certificate was used.",
    "HC2 certificates provide full help with health costs, including free NHS prescriptions.",
    "HC3 certificates provide limited help with health costs. A HC3 certificate will show how much the holder has to pay towards health costs.",
    "If following the assessment, no support is available the applicant will recieve a confirmation letter rather than a certificate.",
    "A 12-month PPC will cover all of a patients NHS prescription charges for a period of 12-months for a set cost.",
    "A 3-month PPC will cover all of a patients NHS prescription charges for a period of 3-months for a set cost.",
    "The length of the certificate, rounded to nearest month based on the certificate start and end dates. For reporting purposes, duration will be grouped based on common scenarios for each service area.",
    "The age band of the applicant, based on the age at the time the application was received and processed. Age will be rounded to the nearest year and reported in 5 year age bands.",
    "The reported IMD quintile, where 1 is the most deprived and 5 the least deprived, is derived from the postcode held for an applicant. IMD quintiles are calculated by ranking census lower-layer super output areas (LSOAs) from most deprived to least deprived and dividing them into equal groups. Quintiles range from the most deprived 20% (quintile 1) of small areas nationally to the least deprived 20% (quintile 5) of small areas nationally.",
    "Three character code unique to an ICB.",
    "Full name for each ICB.",
    "Number of applications received by NHS BSA. Includes applications via any route. Includes all applications regardless of status or outcome.",
    "Number of certificates issued to applicants following processing of applications. Will exclude ongoing or incomplete applications.",
    "For ICB level reporting population baselines are included for context. Based on mid-year population estimates published by Office for National Statistics. Column header will include definition for which year and age range are included in population aggregations.",
    "For ICB level reporting population baselines are included for context. Based on the estimated number of patients receiving appicable NHS prescribing. Column header will include definition for the age range of patients included in the population aggregation.",
    "For ICB level reporting data is presented as rates per 10,000 population to prevent ICB size unfairly impacting results. Figures calculated using the number of issued certificates and the appropriate population denominator, with rates reported per 10,000 population.",
    "Maternity exemption only. Cumulative total number of maternity certificates issued by certificate duration. Certificate duration has been reported as a cumulutive figure to show the number of certificates issued based on the potential timeframe. The smaller the duration of the certificate, the later in pregnancy the application will have been received. A durations less than 12 months would represent a certificate only issued after the birth of the child.",
    "Maternity exemption only. Cumulative proportion of maternity certificates issued by certificate duration. Certificate duration has been reported as a cumulutive figure to show the number of certificates issued based on the potential timeframe. The smaller the duration of the certificate, the later in pregnancy the application will have been received. A durations less than 12 months would represent a certificate only issued after the birth of the child.",
    "Maternity exemption only. Figures sourced from Office for National Statistics to provide additional context. The time period for the live birth figures will be included in the column header.",
    "HRT PPC only. Showing the number of certificates where the applicant requested the certificate to start the following month. Applicants can specify when they would like their certificate to start, up to one month after the application date. This can help applicants apply for a new certificate to start as soon as their existing certificate expires.",
    "HRT PPC only. Showing the proportion of certificates issed where the applicant requested the certificate to start the following month. Applicants can specify when they would like their certificate to start, up to one month after the application date. This can help applicants apply for a new certificate to start as soon as their existing certificate expires.",
    "HRT PPC only. Showing the estimated number of patients who could be identified receiving NHS prescriptions for medication that is eligilbe for support via a HRT PPC."
  )

accessibleTables::create_metadata(wb,
                                  meta_fields,
                                  meta_descs)


# 4.2 Data Tables: NHS Low Income Scheme ----------------------------------

# 4.2.1 LIS: Applications -------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "LIS_Applications",
  title = paste0(config$publication_table_title, " - Number of applications to NHS Low Income Scheme split by financial year and country"),
  notes = c(config$caveat_country_other, config$caveat_country_unknown),
  dataset = lis_application_objs$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "LIS_Applications", c("A","B","C"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "LIS_Applications", c("D"), "right", "#,###")

# 4.2.2 LIS: Issued -----------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "LIS_Issued",
  title = paste0(config$publication_table_title, " - Number of NHS Low Income Scheme HC2/HC3 certificates issued, split by financial year, country and certificate type"),
  notes = c(config$caveat_lis_issued, config$caveat_country_other, config$caveat_country_unknown),
  dataset = lis_issued_objs$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "LIS_Issued", c("A","B","C","D"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "LIS_Issued", c("E"), "right", "#,###")

# 4.2.3 LIS: Duration -----------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "LIS_Certificate_Duration",
  title = paste0(config$publication_table_title, " - Number of issued NHS Low Income Scheme HC2/HC3 certificates, split by financial year, country and certificate duration"),
  notes = c(config$caveat_lis_issued, config$caveat_lis_duration_group, config$caveat_lis_duration_notes, config$caveat_country_other, config$caveat_country_unknown),
  dataset = lis_duration_objs$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "LIS_Certificate_Duration", c("A","B","C","D","E"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "LIS_Certificate_Duration", c("F"), "right", "#,###")

# 4.2.4 LIS: Age ----------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "LIS_Age_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued NHS Low Income Scheme HC2/HC3 certificates, split by financial year, country and age of applicant"),
  notes = c(config$caveat_lis_issued, config$caveat_lis_age_group, config$caveat_age_restriction, config$caveat_country_other, config$caveat_country_unknown),
  dataset = lis_age_objs$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "LIS_Age_Breakdown", c("A","B","C","D","E"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "LIS_Age_Breakdown", c("F"), "right", "#,###")

# 4.2.5 LIS: Deprivation --------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "LIS_Deprivation_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued NHS Low Income Scheme HC2/HC3 certificates, split by financial year, country and IMD quintile"),
  notes = c(config$caveat_lis_issued, config$caveat_imd_restriction, config$caveat_country_other, config$caveat_country_unknown),
  dataset = lis_imd_objs$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "LIS_Deprivation_Breakdown", c("A","B","C","D","E"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "LIS_Deprivation_Breakdown", c("F"), "right", "#,###")

# 4.2.6 LIS: ICB ----------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "LIS_ICB_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued NHS Low Income Scheme HC2/HC3 certificates, split by financial year and ICB"),
  notes = c(config$caveat_lis_issued, config$caveat_icb_method, config$caveat_icb_restriction, config$caveat_lis_base_population, config$caveat_country_other, config$caveat_country_unknown),
  dataset = lis_icb_objs$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "LIS_ICB_Breakdown", c("A","B","C","D","E"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "LIS_ICB_Breakdown", c("F","G","H","I","J"), "right", "#,###")


# 4.3 Data Tables: Maternity exemption certificate ----------------------------------

# 4.3.1 MATEX: Applications -------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "MATEX_Applications",
  title = paste0(config$publication_table_title, " - Number of applications for maternity exemption certificates split by financial year"),
  notes = c(config$caveat_mat_country),
  dataset = mat_application_objs$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "MATEX_Applications", c("A","B"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "MATEX_Applications", c("C"), "right", "#,###")

# 4.3.2 MATEX: Issued -----------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "MATEX_Issued",
  title = paste0(config$publication_table_title, " - Number of maternity exemption certificates issued, split by financial year"),
  notes = c(config$caveat_cert_issued, config$caveat_mat_country),
  dataset = mat_issued_objs$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "MATEX_Issued", c("A","B"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "MATEX_Issued", c("C"), "right", "#,###")

# 4.3.3 MATEX: Duration -----------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "MATEX_Certificate_Duration",
  title = paste0(config$publication_table_title, " - Proportion of maternity exemption certificates issued relative to expected due date"),
  notes = c(config$caveat_cert_issued, config$caveat_mat_duration_method, config$caveat_mat_duration_restriction, config$caveat_mat_country),
  dataset = mat_duration_objs$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "MATEX_Certificate_Duration", c("A","B"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "MATEX_Certificate_Duration", c("C","D","E"), "right", "#,###")
# Proportion: right align * 1 decimal place
accessibleTables::format_data(wb, "MATEX_Certificate_Duration", c("F"), "right", "#,###.0")

# 4.3.4 MATEX: Age ----------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "MATEX_Age_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued maternity exemption certificates, split by age of applicant"),
  notes = c(config$caveat_cert_issued, config$caveat_mat_age_group, config$caveat_age_restriction, config$caveat_mat_live_births, config$caveat_mat_country),
  dataset = mat_age_objs$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "MATEX_Age_Breakdown", c("A","B","C"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "MATEX_Age_Breakdown", c("D","E"), "right", "#,###")

# 4.3.5 MATEX: Deprivation --------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "MATEX_Deprivation_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued maternity exemption certificates, split by IMD quintile"),
  notes = c(config$caveat_cert_issued, config$caveat_imd_restriction, config$caveat_mat_live_births, config$caveat_mat_country),
  dataset = mat_imd_objs$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "MATEX_Deprivation_Breakdown", c("A","B","C"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "MATEX_Deprivation_Breakdown", c("D","E"), "right", "#,###")

# 4.3.6 MATEX: ICB ----------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "MATEX_ICB_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued maternity exemption certificates, split by ICB"),
  notes = c(config$caveat_cert_issued, config$caveat_icb_method, config$caveat_icb_restriction, config$caveat_mat_base_population),
  dataset = mat_icb_objs$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "MATEX_ICB_Breakdown", c("A","B","C","D"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "MATEX_ICB_Breakdown", c("E","F","G"), "right", "#,###")

# 4.4 Data Tables: Medical exemption certificate ----------------------------------

# 4.4.1 MEDEX: Applications -------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "MEDEX_Applications",
  title = paste0(config$publication_table_title, " - Number of applications for medical exemption certificates split by financial year"),
  notes = c(config$caveat_med_country),
  dataset = med_application_objs$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "MEDEX_Applications", c("A","B"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "MEDEX_Applications", c("C"), "right", "#,###")

# 4.4.2 MEDEX: Issued -----------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "MEDEX_Issued",
  title = paste0(config$publication_table_title, " - Number of medical exemption certificates issued, split by financial year"),
  notes = c(config$caveat_cert_issued, config$caveat_med_country),
  dataset = med_issued_objs$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "MEDEX_Issued", c("A","B"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "MEDEX_Issued", c("C"), "right", "#,###")

# 4.4.3 MEDEX: Age ----------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "MEDEX_Age_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued medical exemption certificates, split by age of applicant"),
  notes = c(config$caveat_cert_issued, config$caveat_med_age_group, config$caveat_age_restriction, config$caveat_med_country),
  dataset = med_age_objs$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "MEDEX_Age_Breakdown", c("A","B","C"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "MEDEX_Age_Breakdown", c("D"), "right", "#,###")

# 4.4.4 MEDEX: Deprivation --------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "MEDEX_Deprivation_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued medical exemption certificates, split by IMD quintile"),
  notes = c(config$caveat_cert_issued, config$caveat_imd_restriction, config$caveat_med_country),
  dataset = med_imd_objs$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "MEDEX_Deprivation_Breakdown", c("A","B","C"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "MEDEX_Deprivation_Breakdown", c("D"), "right", "#,###")

# 4.4.5 MEDEX: ICB ----------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "MEDEX_ICB_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued maternity exemption certificates, split by ICB"),
  notes = c(config$caveat_cert_issued, config$caveat_icb_method, config$caveat_icb_restriction, config$caveat_med_base_population),
  dataset = med_icb_objs$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "MEDEX_ICB_Breakdown", c("A","B","C","D"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "MEDEX_ICB_Breakdown", c("E","F","G"), "right", "#,###")

# 4.5 Data Tables: PPC certificate ----------------------------------

# 4.5.1 PPC: Applications -------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "PPC_Applications",
  title = paste0(config$publication_table_title, " - Number of applications for prescription prepayment certificates split by financial year and certificate type"),
  notes = c(config$caveat_ppc_type_unknown, config$caveat_ppc_country),
  dataset = ppc_application_objs$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "PPC_Applications", c("A","B","C"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "PPC_Applications", c("D"), "right", "#,###")

# 4.5.2 PPC: Issued -----------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "PPC_Issued",
  title = paste0(config$publication_table_title, " - Number of prescription prepayment certificates issued, split by financial year and certificate type"),
  notes = c(config$caveat_cert_issued, config$caveat_ppc_type_unknown, config$caveat_ppc_country),
  dataset = ppc_issued_objs$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "PPC_Issued", c("A","B","C"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "PPC_Issued", c("D"), "right", "#,###")

# 4.5.3 PPC: Age ----------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "PPC_Age_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued prescription prepayment certificates, split by certificate type and age of applicant"),
  notes = c(config$caveat_cert_issued, config$caveat_ppc_age_group, config$caveat_age_restriction, config$caveat_ppc_country),
  dataset = ppc_age_objs$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "PPC_Age_Breakdown", c("A","B","C","D"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "PPC_Age_Breakdown", c("E"), "right", "#,###")

# 4.5.4 PPC: Deprivation --------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "PPC_Deprivation_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued prescription prepayment certificates, split by certificate type and IMD quintile"),
  notes = c(config$caveat_cert_issued, config$caveat_imd_restriction, config$caveat_ppc_country),
  dataset = ppc_imd_objs$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "PPC_Deprivation_Breakdown", c("A","B","C","D"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "PPC_Deprivation_Breakdown", c("E"), "right", "#,###")

# 4.5.5 PPC: ICB ----------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "PPC_ICB_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued prescription prepayment certificates, split by ICB"),
  notes = c(config$caveat_cert_issued, config$caveat_icb_method, config$caveat_icb_restriction, config$caveat_ppc_base_population),
  dataset = ppc_icb_objs$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "PPC_ICB_Breakdown", c("A","B","C","D"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "PPC_ICB_Breakdown", c("E","F","G","H","I"), "right", "#,###")

# 4.6 Data Tables: HRT PPC certificate ----------------------------------

# 4.6.1 HRT PPC: Applications -------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "HRTPPC_Applications",
  title = paste0(config$publication_table_title, " - Number of applications for NHS Hormone Replacement Therapy Prescription Prepayment Certificate (HRT PPC) split by month"),
  notes = c(),
  dataset = hrt_application_objs$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "HRTPPC_Applications", c("A","B"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "HRTPPC_Applications", c("C"), "right", "#,###")

# 4.6.2 HRT PPC: Issued -----------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "HRTPPC_Issued",
  title = paste0(config$publication_table_title, " - Number of NHS Hormone Replacement Therapy Prescription Prepayment Certificate (HRT PPC) issued, split by month"),
  notes = c(config$caveat_hrtppc_start, config$caveat_cert_issued, config$caveat_hrtppc_postdate, config$caveat_hrtppc_country),
  dataset = hrt_issued_objs$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "HRTPPC_Issued", c("A","B"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "HRTPPC_Issued", c("C","D"), "right", "#,###")
# Proportion: right align * thousand seperator to 1 decimal place
accessibleTables::format_data(wb, "HRTPPC_Issued", c("E"), "right", "#,###.0")

# 4.6.3 HRT PPC: Age ----------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "HRTPPC_Age_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued NHS Hormone Replacement Therapy Prescription Prepayment Certificate (HRT PPC), split by age of applicant"),
  notes = c(config$caveat_cert_issued, config$caveat_hrtppc_age_group, config$caveat_age_restriction, config$caveat_hrtppc_px_data, config$caveat_hrtppc_country),
  dataset = hrt_age_objs$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "HRTPPC_Age_Breakdown", c("A","B","C"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "HRTPPC_Age_Breakdown", c("D","E"), "right", "#,###")

# 4.6.4 HRT PPC: Deprivation --------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "HRTPPC_Deprivation_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued NHS Hormone Replacement Therapy Prescription Prepayment Certificate (HRT PPC), split by IMD quintile"),
  notes = c(config$caveat_cert_issued, config$caveat_imd_restriction, config$caveat_hrtppc_px_data, config$caveat_hrtppc_country),
  dataset = hrt_imd_objs$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "HRTPPC_Deprivation_Breakdown", c("A","B","C"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "HRTPPC_Deprivation_Breakdown", c("D","E"), "right", "#,###")

# 4.6.5 HRT PPC: ICB ----------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "HRTPPC_ICB_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued NHS Hormone Replacement Therapy Prescription Prepayment Certificate (HRT PPC), split by ICB"),
  notes = c(config$caveat_cert_issued, config$caveat_icb_method, config$caveat_icb_restriction, config$caveat_hrtppc_base_population),
  dataset = hrt_icb_objs$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "HRTPPC_ICB_Breakdown", c("A","B","C","D"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "HRTPPC_ICB_Breakdown", c("E","F","G"), "right", "#,###")

# 4.7 Data Tables: NHS tax credit exemption certificates ----------------------------------

# 4.7.1 TAX: Issued -----------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "TAX_Issued",
  title = paste0(config$publication_table_title, " - Number of NHS tax credit exemption certificates issued, split by financial year and country"),
  notes = c(config$caveat_tax_issued, config$caveat_country_other, config$caveat_country_unknown),
  dataset = tax_issued_objs$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "TAX_Issued", c("A","B","C"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "TAX_Issued", c("D"), "right", "#,###")

# 4.7.2 TAX: Age ----------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "TAX_Age_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued NHS tax credit exemption certificates, split by country and age of applicant"),
  notes = c(config$caveat_tax_age_band, config$caveat_age_restriction, config$caveat_country_other, config$caveat_country_unknown),
  dataset = tax_age_objs$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "TAX_Age_Breakdown", c("A","B","C","D"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "TAX_Age_Breakdown", c("E"), "right", "#,###")

# 4.7.3 TAX: Age Profile ----------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "TAX_Age_Profile",
  title = paste0(config$publication_table_title, " - Number of issued NHS tax credit exemption certificates, split by country, financial year and age group"),
  notes = c(config$caveat_tax_age_group, config$caveat_age_restriction, config$caveat_country_other, config$caveat_country_unknown),
  dataset = sd_tax_age_trend,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "TAX_Age_Profile", c("A","B","C","D"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "TAX_Age_Profile", c("E"), "right", "#,###")

# 4.7.4 TAX: Deprivation --------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "TAX_Deprivation_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued NHS tax credit exemption certificates, split by country and IMD quintile"),
  notes = c(config$caveat_imd_restriction, config$caveat_country_other, config$caveat_country_unknown),
  dataset = tax_imd_objs$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "TAX_Deprivation_Breakdown", c("A","B","C","D"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "TAX_Deprivation_Breakdown", c("E"), "right", "#,###")

# 4.7.5 TAX: ICB ----------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "TAX_ICB_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued NHS tax credit exemption certificates, split by ICB"),
  notes = c(config$caveat_icb_method, config$caveat_icb_restriction, config$caveat_tax_base_population),
  dataset = tax_icb_objs$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "TAX_ICB_Breakdown", c("A","B","C","D","E"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "TAX_ICB_Breakdown", c("F","G","H"), "right", "#,###")

# 4.8 Cover Sheet ---------------------------------------------------------

# assign a table name to each of the sheets in the workbook
# identify all the tables included (based on sheetNames)
table_list <- sheetNames
# append a sequential table number
for(s in 1:length(table_list)){
  table_list[s] <- paste0("Table ",s,": ",table_list[s])
}
# add the Metadata reference to the top of the list
table_list <- c("Metadata",table_list)


# create cover sheet
accessibleTables::makeCoverSheet(
  config$publication_top_name,
  config$publication_sub_name,
  paste0("Publication date: ", config$publication_date),
  wb,
  sheetNames,
  table_list,
  c("Metadata", sheetNames)
)

# save file into outputs folder
openxlsx::saveWorkbook(wb,
                       "outputs/hwhc_tables.xlsx",
                       overwrite = TRUE)

# 5. Render outputs ------------------------------------------------------------

rmarkdown::render(
  "hwhc-markdown.Rmd",
  output_format = config$output_type,
  output_file = paste0(
    "outputs/", 
    config$publication_title, 
    switch(config$output_type,
           "html_document" = ".html",
           "word_document" = ".docx"
           )
  )
)

rmarkdown::render(
  "hwhc-methodology.Rmd",
  output_format = config$output_type,
  output_file = paste0(
    "outputs/", 
    config$methodology_title, 
    switch(config$output_type,
           "html_document" = ".html",
           "word_document" = ".docx"
    )
  )
)

# 6. Build PowerBI support tables------------------------------------------

# establish connection to database
con <- nhsbsaR::con_nhsbsa(
  dsn = "FBS_8192k",
  driver = "Oracle in OraClient19Home1",
  "DWCP"
)


# 6.1 Summarise service area data -----------------------------------------
# data from each dataset will be aggregate to only the required groupings
# individual SQL scripts will create intermediate tables that can be dropped once combined

if(config$rebuild_powerbi_data == TRUE){

  # LIS data
  create_dataset_from_sql(
    db_connection = con,
    path_to_sql_file = "./SQL/HWHC_BI_LIS.sql",
    db_table_name = "HWHC_BI_LIS",
    ls_variables = list(
      var = c("p_min_ym", "p_max_ym"),
      val = c(config$powerbi_min_ym_lis, config$powerbi_max_ym_lis)
    )
  )
  
  # HES data
  create_dataset_from_sql(
    db_connection = con,
    path_to_sql_file = "./SQL/HWHC_BI_HES.sql",
    db_table_name = "HWHC_BI_HES",
    ls_variables = list(
      var = c("p_min_ym", "p_max_ym"),
      val = c(config$powerbi_min_ym_hes, config$powerbi_max_ym_hes)
    )
  )
  
  # HRT PPC data
  create_dataset_from_sql(
    db_connection = con,
    path_to_sql_file = "./SQL/HWHC_BI_HRTPPC.sql",
    db_table_name = "HWHC_BI_HRTPPC",
    ls_variables = list(
      var = c("p_min_ym", "p_max_ym"),
      val = c(config$powerbi_min_ym_hrtppc, config$powerbi_max_ym_hrtppc)
    )
  )
  
  # Combine datasets
  create_dataset_from_sql(
    db_connection = con,
    path_to_sql_file = "./SQL/HWHC_BI_OUTPUT.sql",
    db_table_name = "HWHC_BI_OUTPUT"
  )
  
  # remove the intermediate tables
  DBI::dbRemoveTable(conn = con, name = DBI::Id(schema = toupper(con@info$username), table = "HWHC_BI_LIS"))
  DBI::dbRemoveTable(conn = con, name = DBI::Id(schema = toupper(con@info$username), table = "HWHC_BI_HES"))
  DBI::dbRemoveTable(conn = con, name = DBI::Id(schema = toupper(con@info$username), table = "HWHC_BI_HRTPPC"))
  
# 6.2 Create base population datasets -------------------------------------
# for population figures based on patient estimates the script struggles to run for multiple years
# to improve performance the script can be run for individual years with results combined
# intermediate tables can be dropped following execution
# this is still slow and should only be required if there have been changes to the code as prescription data does not change
  
  if(config$rebuild_prescription_population_data == TRUE){
    
    # create a list of periods to loop for prescription data
    # values will have been supplied as comma seperated lists in the pipeline
    px_fy_list <- list(
      fy = unlist(strsplit(config$px_pop_data_fy_list,",")),
      min_ym = unlist(strsplit(config$px_pop_data_fy_min_ym,",")),
      max_ym = unlist(strsplit(config$px_pop_data_fy_max_ym,","))
    )
    
    num_var <- length(px_fy_list$fy)
    
    if(num_var > 0){
      
      # build the intermediate tables
      for(v in 1:num_var){
        
        db_table_name = paste0("HWHC_PX_PAT_FY_ICB_",px_fy_list$fy[v])
        
        # update statement to combine intermediate tables
        if(v==1){
          sql_stmt = paste0("create table HWHC_PX_PAT_FY_ICB as select * from ", db_table_name)
        } else {
          sql_stmt = paste0(sql_stmt, " union all select * from ", db_table_name)
        }
        create_dataset_from_sql(
          db_connection = con,
          path_to_sql_file = "./SQL/HWHC_PX_PAT_FY_ICB.sql",
          db_table_name = db_table_name,
          ls_variables = list(
            var = c("p_min_ym","p_max_ym"),
            val = c(px_fy_list$min_ym[v], px_fy_list$max_ym[v])
          )
        )
        
      }
      
      # remove existing version of the table
      if(
        DBI::dbExistsTable(
          conn = con,
          name = DBI::Id(schema = toupper(con@info$username), table = "HWHC_PX_PAT_FY_ICB")
        ) == T
      ){
        DBI::dbRemoveTable(
          conn = con,
          name = DBI::Id(schema = toupper(con@info$username), table = "HWHC_PX_PAT_FY_ICB")
        )
      }
      
      # create the combined data
      # execute script to create database table
      DBI::dbExecute(conn = con, statement = sql_stmt)
      
      # delete the intermediate tables
      for(v in 1:num_var){
        DBI::dbRemoveTable(conn = con, name = DBI::Id(schema = toupper(con@info$username), table = paste0("HWHC_PX_PAT_FY_ICB_",px_fy_list$fy[v])))
      }
      
    }
    
    # create the ICB population reference table
    create_dataset_from_sql(
      db_connection = con,
      path_to_sql_file = "./SQL/HWHC_BI_ICB_POPULATION.sql",
      db_table_name = "HWHC_BI_ICB_POPULATION",
      ls_variables = list(
        var = c("p_min_ym","p_max_ym"),
        val = c(config$powerbi_min_ym_lis, config$powerbi_max_ym_lis)
      )
    )
    
  }
  

# 6.3 Create aggregated summary tables ------------------------------------
# these aggregated tables will be the base for the analyses
# data is pre-aggregated to the required levels with disclosure controls applied
# after creating table a csv extract should be produced to load to PowerBI
  

# 6.3.1 Financial year trend data -----------------------------------------

  # Combine datasets
  create_dataset_from_sql(
    db_connection = con,
    path_to_sql_file = "./SQL/HWHC_BI_AGG_FY_SERVICE_GEO_CERT.sql",
    db_table_name = "HWHC_BI_AGG_FY_SERVICE_GEO_CERT"
  )
  # export to csv
  export_db_table_to_csv(
    db_connection = con,
    db_table_name = "HWHC_BI_AGG_FY_SERVICE_GEO_CERT",
    output_path = "outputs/HWHC_BI_AGG_FY_SERVICE_GEO_CERT.csv"
  )

# 6.3.2 ICB summary data --------------------------------------------------

  # Combine datasets
  create_dataset_from_sql(
    db_connection = con,
    path_to_sql_file = "./SQL/HWHC_BI_AGG_FY_SERVICE_ICB_CERT.sql",
    db_table_name = "HWHC_BI_AGG_FY_SERVICE_ICB_CERT"
  )
  # export to csv
  export_db_table_to_csv(
    db_connection = con,
    db_table_name = "HWHC_BI_AGG_FY_SERVICE_ICB_CERT",
    output_path = "outputs/HWHC_BI_AGG_FY_SERVICE_ICB_CERT.csv"
  )

# 6.3.3 Age summary data --------------------------------------------------
  
  # Combine datasets
  create_dataset_from_sql(
    db_connection = con,
    path_to_sql_file = "./SQL/HWHC_BI_AGG_FY_SERVICE_GEO_CERT_AGE.sql",
    db_table_name = "HWHC_BI_AGG_FY_SERVICE_GEO_CERT_AGE"
  )
  # export to csv
  export_db_table_to_csv(
    db_connection = con,
    db_table_name = "HWHC_BI_AGG_FY_SERVICE_GEO_CERT_AGE",
    output_path = "outputs/HWHC_BI_AGG_FY_SERVICE_GEO_CERT_AGE.csv"
  )

# 6.3.4 IMD summary data --------------------------------------------------
  
  # Combine datasets
  create_dataset_from_sql(
    db_connection = con,
    path_to_sql_file = "./SQL/HWHC_BI_AGG_FY_SERVICE_GEO_CERT_IMD.sql",
    db_table_name = "HWHC_BI_AGG_FY_SERVICE_GEO_CERT_IMD"
  )
  # export to csv
  export_db_table_to_csv(
    db_connection = con,
    db_table_name = "HWHC_BI_AGG_FY_SERVICE_GEO_CERT_IMD",
    output_path = "outputs/HWHC_BI_AGG_FY_SERVICE_GEO_CERT_IMD.csv"
  )

# 6.3.5 Duration summary data ---------------------------------------------
  
  # Combine datasets
  create_dataset_from_sql(
    db_connection = con,
    path_to_sql_file = "./SQL/HWHC_BI_AGG_FY_SERVICE_GEO_CERT_DURATION.sql",
    db_table_name = "HWHC_BI_AGG_FY_SERVICE_GEO_CERT_DURATION"
  )
  # export to csv
  export_db_table_to_csv(
    db_connection = con,
    db_table_name = "HWHC_BI_AGG_FY_SERVICE_GEO_CERT_DURATION",
    output_path = "outputs/HWHC_BI_AGG_FY_SERVICE_GEO_CERT_DURATION.csv"
  )

# 6.3.6 Month trend data -----------------------------------------
  
  # Combine datasets
  create_dataset_from_sql(
    db_connection = con,
    path_to_sql_file = "./SQL/HWHC_BI_AGG_MONTH_SERVICE_GEO_CERT.sql",
    db_table_name = "HWHC_BI_AGG_MONTH_SERVICE_GEO_CERT"
  )
  # export to csv
  export_db_table_to_csv(
    db_connection = con,
    db_table_name = "HWHC_BI_AGG_MONTH_SERVICE_GEO_CERT",
    output_path = "outputs/HWHC_BI_AGG_MONTH_SERVICE_GEO_CERT.csv"
  )
  


# 6.4 Create dimension tables ---------------------------------------------
  
  # Dimension table : Service Area list
  create_dataset_from_sql(
    db_connection = con,
    path_to_sql_file = "./SQL/HWHC_BI_DIM_SERVICE_AREA_NAME.sql",
    db_table_name = "HWHC_BI_DIM_SERVICE_AREA_NAME"
  )
  # export to csv
  export_db_table_to_csv(
    db_connection = con,
    db_table_name = "HWHC_BI_DIM_SERVICE_AREA_NAME",
    output_path = "outputs/HWHC_BI_DIM_SERVICE_AREA_NAME.csv"
  )
  
  # Dimension table : Geographic Location list
  create_dataset_from_sql(
    db_connection = con,
    path_to_sql_file = "./SQL/HWHC_BI_DIM_GEO_CLASSIFICATION.sql",
    db_table_name = "HWHC_BI_DIM_GEO_CLASSIFICATION"
  )
  # export to csv
  export_db_table_to_csv(
    db_connection = con,
    db_table_name = "HWHC_BI_DIM_GEO_CLASSIFICATION",
    output_path = "outputs/HWHC_BI_DIM_GEO_CLASSIFICATION.csv"
  )
  
  # Dimension table : Financial Year list
  create_dataset_from_sql(
    db_connection = con,
    path_to_sql_file = "./SQL/HWHC_BI_DIM_FY.sql",
    db_table_name = "HWHC_BI_DIM_FY"
  )
  # export to csv
  export_db_table_to_csv(
    db_connection = con,
    db_table_name = "HWHC_BI_DIM_FY",
    output_path = "outputs/HWHC_BI_DIM_FY.csv"
  )
  
  # Dimension table : Certificate Type list
  create_dataset_from_sql(
    db_connection = con,
    path_to_sql_file = "./SQL/HWHC_BI_DIM_CERT_TYPE.sql",
    db_table_name = "HWHC_BI_DIM_CERT_TYPE"
  )
  # export to csv
  export_db_table_to_csv(
    db_connection = con,
    db_table_name = "HWHC_BI_DIM_CERT_TYPE",
    output_path = "outputs/HWHC_BI_DIM_CERT_TYPE.csv"
  )

}

# 6.5 Close database connection -------------------------------------------

# close connection to database
DBI::dbDisconnect(con)


logr::log_close()
