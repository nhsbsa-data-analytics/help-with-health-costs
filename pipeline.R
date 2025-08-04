# pipeline.R
# this script provides the code to run the reproducible analytical pipeline
# and produce the Help with Health Costs (HWHC) publication

# contains the following sections:
# 1. Setup and package installation
# 2. Data import
# 3. Build PowerBI support tables
# 4. Aggregations and analysis
# 5. Data tables - financial year
# 6. Data tables - calendar year
# 7. Render outputs

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
    "Y:/Official Stats/HWHC/hwhc_log",
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

# 2. Data import ---------------------------------------------------------------

# establish connection to database
con <- nhsbsaR::con_nhsbsa(
  dsn = "FBS_8192k",
  driver = "Oracle in OraClient19Home1",
  "DWCP"
)

# 2.1 Data Import: NHS Low Income Scheme ---------------------------------------

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

# 2.2 Data Import: HES (MATEX, MEDEX, PPC & Tax Credits) -----------------------

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

# 2.3 Data Import: HRT PPC -----------------------------------------------------

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

# 2.4 Data Import: PX PATIENT COUNTS -------------------------------------------

# Financial year
# create the base dataset for prescription patient counts
# data at aggregated level showing counts of patients from the NHS prescription data
# patient age calc date is 30th September for financial year

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

#Calendar year
# create the base dataset for prescription patient counts
# data at aggregated level showing counts of patients from the NHS prescription data
# patient age calc date is 30th June for calendar year

if(config$rebuild_base_px_data == TRUE){
  create_dataset_from_sql(
    db_connection = con,
    path_to_sql_file = "./SQL/HWHC_PX_PAT_FACT.sql",
    db_table_name = "HWHC_PX_PAT_FACT_CY",
    ls_variables = list(
      var = c("p_min_ym","p_max_ym","p_age_date"),
      val = c(config$extract_px_min_ym_cy, config$extract_px_max_ym_cy, config$extract_px_age_dt_cy)
    )
  )
}

# 3. Build PowerBI support tables-----------------------------------------------

# 3.1 Summarise service area data ----------------------------------------------
# data from each dataset will be aggregate to only the required groupings
# individual SQL scripts will create intermediate tables that can be dropped once combined
  
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
  
# 3.2 Create base population datasets ------------------------------------------
# for population figures based on patient estimates the script struggles to run for multiple years
# to improve performance the script can be run for individual years with results combined
# intermediate tables can be dropped following execution
# this is still slow and should only be required if there have been changes to the code as prescription data does not change
  
  if(config$rebuild_prescription_population_data == TRUE){
    
    # create a list of periods to loop for prescription data
    # values will have been supplied as comma separated lists in the pipeline
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
  
#calendar year dataset  
  
  if(config$rebuild_prescription_population_data == TRUE){
    
    # create a list of periods to loop for prescription data
    # values will have been supplied as comma separated lists in the pipeline
    px_cy_list <- list(
      cy = unlist(strsplit(config$px_pop_data_cy_list,",")),
      min_ym = unlist(strsplit(config$px_pop_data_cy_min_ym,",")),
      max_ym = unlist(strsplit(config$px_pop_data_cy_max_ym,","))
    )
    
    num_var <- length(px_cy_list$cy)
    
    if(num_var > 0){
      
      # build the intermediate tables
      for(v in 1:num_var){
        
        db_table_name = paste0("HWHC_PX_PAT_CY_ICB_",px_cy_list$cy[v])
        
        # update statement to combine intermediate tables
        if(v==1){
          sql_stmt = paste0("create table HWHC_PX_PAT_CY_ICB as select * from ", db_table_name)
        } else {
          sql_stmt = paste0(sql_stmt, " union all select * from ", db_table_name)
        }
        create_dataset_from_sql(
          db_connection = con,
          path_to_sql_file = "./SQL/HWHC_PX_PAT_CY_ICB.sql",
          db_table_name = db_table_name,
          ls_variables = list(
            var = c("p_min_ym","p_max_ym"),
            val = c(px_cy_list$min_ym[v], px_cy_list$max_ym[v])
          )
        )
        
      }
      
      # remove existing version of the table
      if(
        DBI::dbExistsTable(
          conn = con,
          name = DBI::Id(schema = toupper(con@info$username), table = "HWHC_PX_PAT_CY_ICB")
        ) == T
      ){
        DBI::dbRemoveTable(
          conn = con,
          name = DBI::Id(schema = toupper(con@info$username), table = "HWHC_PX_PAT_CY_ICB")
        )
      }
      
      # create the combined data
      # execute script to create database table
      DBI::dbExecute(conn = con, statement = sql_stmt)
      
      # delete the intermediate tables
      for(v in 1:num_var){
        DBI::dbRemoveTable(conn = con, name = DBI::Id(schema = toupper(con@info$username), table = paste0("HWHC_PX_PAT_CY_ICB_",px_cy_list$cy[v])))
      }
      
    }
    
    # create the ICB population reference table
    create_dataset_from_sql(
      db_connection = con,
      path_to_sql_file = "./SQL/HWHC_BI_ICB_POPULATION_CY.sql",
      db_table_name = "HWHC_BI_ICB_POPULATION_CY",
      ls_variables = list(
        var = c("p_min_ym","p_max_ym"),
        val = c(config$powerbi_min_ym_lis_cy, config$powerbi_max_ym_lis_cy)
      )
    )
    
  }
  
# 3.3 Create aggregated summary tables -----------------------------------------
# these aggregated tables will be the base for the analyses
# data is pre-aggregated to the required levels with disclosure controls applied
# after creating table a csv extract should be produced to load to PowerBI
  
  
# 3.3.1 Financial year trend data ----------------------------------------------
  
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
  
# 3.3.2 ICB summary data -------------------------------------------------------
  
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
  
# 3.3.3 Age summary data -------------------------------------------------------
  
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
  
# 3.3.4 IMD summary data -------------------------------------------------------
  
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
  
# 3.3.5 Duration summary data --------------------------------------------------
  
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
  
# 3.3.6 Month trend data -------------------------------------------------------
  
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
  
# 3.4 Create dimension tables --------------------------------------------------
  
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


# 4. Aggregations and analysis -------------------------------------------------

# 4.1 Aggregation and analysis: NHS Low Income Scheme (LIS) --------------------

# 4.1.1 LIS: Applications received (trend) -------------------------------------
# Applications will include any application to the scheme regardless of current status or outcome
# The dataset is already limited to HC1 applications excluding HC5 (refund only) applications

# not required for narrative and only required for supporting data

#financial year
lis_application_objs <- create_hes_application_objects(
  db_connection = con,
  db_table_name = 'HWHC_LIS_FACT',
  service_area = 'LIS',
  min_ym = config$min_trend_ym_lis,
  max_ym = config$max_trend_ym_lis,
  subtype_split = FALSE
)

#calendar year  
lis_application_objs_cy <- create_hes_application_objects_cy(
  db_connection = con,
  db_table_name = 'HWHC_LIS_FACT',
  service_area = 'LIS',
  min_ym = config$min_trend_ym_lis_cy,
  max_ym = config$max_trend_ym_lis_cy,
  subtype_split = FALSE
) 

#rename calendar year column
lis_application_objs_cy$support_data <- lis_application_objs_cy$support_data |>
  rename_df_fields()

# 4.1.2 LIS: Certificates issued (trend)----------------------------------------
# A certificate will only be issued when the application is completed and assessed
# If applicants qualify for support they will be issued a HC2/HC3 certificate
# Not all applications will reach the "outcome/decision" stage as applicants may drop out during the application/assessment process
# For HC3 this will include cases where a HBD11 was issued to the applicant

#financial year
lis_issued_objs <- create_hes_issued_objects(
  db_connection = con,
  db_table_name = 'HWHC_LIS_FACT',
  service_area = 'LIS',
  min_ym = config$min_trend_ym_lis,
  max_ym = config$max_trend_ym_lis,
  subtype_split = TRUE
)

#calendar year
lis_issued_objs_cy <- create_hes_issued_objects_cy(
  db_connection = con,
  db_table_name = 'HWHC_LIS_FACT',
  service_area = 'LIS',
  min_ym = config$min_trend_ym_lis_cy,
  max_ym = config$max_trend_ym_lis_cy,
  subtype_split = TRUE
)

#rename calendar year column
lis_issued_objs_cy$support_data <- lis_issued_objs_cy$support_data |>
  rename_df_fields()

# 4.1.3 LIS: Duration (narrative = latest year only)----------------------------
# The duration of HC2/HC3 certificates will vary based on applicants circumstances
# This reporting is only applicable for issued HC2/HC3 certificates
# CERTIFICATE_DURATION in the data uses some grouping categories to bundle certificates into common categories

# not required for narrative and only required for supporting data

#financial year
lis_duration_objs <- create_hes_duration_objects(
  db_connection = con,
  db_table_name = 'HWHC_LIS_FACT',
  service_area = 'LIS',
  min_ym = config$min_trend_ym_lis,
  max_ym = config$max_trend_ym_lis,
  focus_fy = config$focus_fy_lis,
  subtype_split = TRUE
)

#calendar year
lis_duration_objs_cy <- create_hes_duration_objects_cy(
  db_connection = con,
  db_table_name = 'HWHC_LIS_FACT',
  service_area = 'LIS',
  min_ym = config$min_trend_ym_lis_cy,
  max_ym = config$max_trend_ym_lis_cy,
  subtype_split = TRUE
)

#rename calendar year column
lis_duration_objs_cy$support_data <- lis_duration_objs_cy$support_data |>
  rename_df_fields()

# 4.1.4 LIS: Age profile (narrative = latest year only)-------------------------
# Age is available for the lead applicant only
# Some processing has been performed to group by set age bands and reclassify potential errors 

#financial year
lis_age_objs <- create_hes_age_objects(
  db_connection = con,
  db_table_name = 'HWHC_LIS_FACT',
  service_area = 'LIS',
  min_ym = config$min_trend_ym_lis,
  max_ym = config$max_trend_ym_lis,
  focus_fy = config$focus_fy_lis,
  subtype_split = TRUE
)

#calendar year
lis_age_objs_cy <- create_hes_age_objects_cy(
  db_connection = con,
  db_table_name = 'HWHC_LIS_FACT',
  service_area = 'LIS',
  min_ym = config$min_trend_ym_lis_cy,
  max_ym = config$max_trend_ym_lis_cy,
  subtype_split = TRUE
)

#rename calendar year column
lis_age_objs_cy$support_data <- lis_age_objs_cy$support_data |>
  rename_df_fields()

# 4.1.5 LIS: Deprivation profile (narrative = latest year only)-----------------
# IMD may not be available if the postcode cannot be mapped to NSPL

#financial year
lis_imd_objs <- create_hes_imd_objects(
  db_connection = con,
  db_table_name = 'HWHC_LIS_FACT',
  service_area = 'LIS',
  min_ym = config$min_trend_ym_lis,
  max_ym = config$max_trend_ym_lis,
  focus_fy = config$focus_fy_lis,
  subtype_split = TRUE
)

#calendar year
lis_imd_objs_cy <- create_hes_imd_objects_cy(
  db_connection = con,
  db_table_name = 'HWHC_LIS_FACT',
  service_area = 'LIS',
  min_ym = config$min_trend_ym_lis_cy,
  max_ym = config$max_trend_ym_lis_cy,
  subtype_split = TRUE
)

#rename calendar year column
lis_imd_objs_cy$support_data <- lis_imd_objs_cy$support_data |>
  rename_df_fields()

# 4.1.6 LIS: ICB profile (narrative = latest year only) ------------------------
# ICBs can vary in size and therefore are not appropriate for direct comparison
# Figures should be standardised by a population denominator
# population denominators for each ICB have been defined and applied in the HWHC_BI_ICB_POPULATION database table

#financial year
lis_icb_objs <- create_hes_icb_objects(
  db_connection = con,
  db_table_name = 'HWHC_LIS_FACT',
  service_area = 'LIS',
  min_ym = config$min_trend_ym_lis,
  max_ym = config$max_trend_ym_lis,
  focus_fy = config$focus_fy_lis,
  subtype_split = TRUE,
  population_db_table = "HWHC_BI_ICB_POPULATION"
)

#calendar year
lis_icb_objs_cy <- create_hes_icb_objects_cy(
  db_connection = con,
  db_table_name = 'HWHC_LIS_FACT',
  service_area = 'LIS',
  min_ym = config$min_trend_ym_lis_cy,
  max_ym = config$max_trend_ym_lis_cy,
  subtype_split = TRUE,
  population_db_table = "HWHC_BI_ICB_POPULATION_CY"
)

#rename calendar year column
lis_icb_objs_cy$support_data <- lis_icb_objs_cy$support_data |>
  rename_df_fields()

# 4.2 Aggregation and analysis: Maternity Exemption (MATEX) --------------------


# 4.2.1 MATEX: Applications received (trend) -----------------------------------
# Applications will include any application to the scheme regardless of current status or outcome

# not required for narrative and only required for supporting data

#financial year
mat_application_objs <- create_hes_application_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'MAT',
  min_ym = config$min_trend_ym_mat,
  max_ym = config$max_trend_ym_mat,
  subtype_split = FALSE
)

#calendar year
mat_application_objs_cy <- create_hes_application_objects_cy(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'MAT',
  min_ym = config$min_trend_ym_mat_cy,
  max_ym = config$max_trend_ym_mat_cy,
  subtype_split = FALSE
)

#rename calendar year column
mat_application_objs_cy$support_data <- mat_application_objs_cy$support_data |>
  rename_df_fields()

# 4.2.2 MATEX: Certificates issued (trend) -------------------------------------
# Issued certificates will only include cases where a certificate was issued to the customer

#financial year
mat_issued_objs <- create_hes_issued_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'MAT',
  min_ym = config$min_trend_ym_mat,
  max_ym = config$max_trend_ym_mat,
  subtype_split = FALSE
)

#calendar year
mat_issued_objs_cy <- create_hes_issued_objects_cy(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'MAT',
  min_ym = config$min_trend_ym_mat_cy,
  max_ym = config$max_trend_ym_mat_cy,
  subtype_split = FALSE
)

#rename calendar year column
mat_issued_objs_cy$support_data <- mat_issued_objs_cy$support_data |>
  rename_df_fields()

# 4.2.3 MATEX: Duration (narrative = latest year only) -------------------------
# Split by certificate duration

# not required for narrative and only required for supporting data

#financial year
mat_duration_objs <- create_hes_duration_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'MAT',
  min_ym = config$min_trend_ym_mat,
  max_ym = config$max_trend_ym_mat,
  focus_fy = config$focus_fy_mat,
  subtype_split = FALSE
)

#calendar year
mat_duration_objs_cy <- create_hes_duration_objects_cy(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'MAT',
  min_ym = config$min_trend_ym_mat_cy,
  max_ym = config$max_trend_ym_mat_cy,
  subtype_split = FALSE
)

#rename calendar year column
mat_duration_objs_cy$support_data <- mat_duration_objs_cy$support_data |>
  rename_df_fields()

# 4.2.4 MATEX: Age profile (narrative = latest year only) ----------------------
# Issued certificates will only include cases where a certificate was issued to the customer
# Some processing has been performed to group by set age bands and reclassify potential errors 

#financial year
mat_age_objs <- create_hes_age_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'MAT',
  min_ym = config$min_trend_ym_mat,
  max_ym = config$max_trend_ym_mat,
  focus_fy = config$focus_fy_mat,
  subtype_split = FALSE
)

#calendar year
mat_age_objs_cy <- create_hes_age_objects_cy(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'MAT',
  min_ym = config$min_trend_ym_mat_cy,
  max_ym = config$max_trend_ym_mat_cy,
  subtype_split = FALSE
)

#rename calendar year column
mat_age_objs_cy$support_data <- mat_age_objs_cy$support_data |>
  rename_df_fields()

# for additional context identify the number of live births published by ONS
#TO DO: identify how best to code difference in ONS under 20 age group vs MATEX 15-19 group
#as ONS under 20 data includes 10-14 ages
#consider adding MATEX 10-14 to make a MATEX under 20 group
df_births_age <- get_ons_live_birth_geo_data(con, config$ons_birth_geo_table, "COUNTRY", year = "2023") |>
  dplyr::mutate(AGE_BAND = ifelse(AGE_BAND == 'Under 20','15-19',AGE_BAND))

#dataset for chart data download with ONS under 20 group label

ch_births_age_data <- df_births_age |>
  dplyr::mutate(AGE_BAND = ifelse(AGE_BAND == '15-19','Under 20',AGE_BAND)) |>
  dplyr::arrange(factor(
    AGE_BAND,
    levels = c('Under 20', '20-24', '25-29', '30-34', '35-39', '40-44', '45+')
  ))

# create separate chart for ONS data (Figure 2 live births tab chart)
ch_births_age <- ch_births_age_data |>
  dplyr::mutate(LIVE_BIRTHS_SF = signif(LIVE_BIRTHS, 3)) |>
  nhsbsaVis::basic_chart_hc(
    x = AGE_BAND,
    y = LIVE_BIRTHS_SF,
    type = "column",
    xLab = "Age band",
    yLab = "Number of live births",
    seriesName = "Live births",
    title = "",
    # assign custom title in markdown
    dlOn = FALSE
  ) |>
  highcharter::hc_tooltip(enabled = T,
                          shared = T,
                          sort = T)

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

# create a separate support dataset for live birth data
# change code to reflect under 20 vs 15-19 age group if required
mat_age_live_birth_data <- df_births_age |>
  dplyr::arrange(AGE_BAND) |>
  dplyr::rename(
    !!paste0("ONS Live Births (", config$ons_births_year, ")") := LIVE_BIRTHS,
    "Age Band" = AGE_BAND
  )

# 4.2.5 MATEX: Deprivation profile (narrative = latest year only) --------------
# IMD may not be available if the postcode cannot be mapped to NSPL

#financial year
mat_imd_objs <- create_hes_imd_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'MAT',
  min_ym = config$min_trend_ym_mat,
  max_ym = config$max_trend_ym_mat,
  focus_fy = config$focus_fy_mat,
  subtype_split = FALSE
)

#calendar year
mat_imd_objs_cy <- create_hes_imd_objects_cy(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'MAT',
  min_ym = config$min_trend_ym_mat_cy,
  max_ym = config$max_trend_ym_mat_cy,
  subtype_split = FALSE
)

#rename calendar year column
mat_imd_objs_cy$support_data <- mat_imd_objs_cy$support_data |>
  rename_df_fields()

# for additional context identify the number of live births published by ONS
df_births_imd <- get_ons_live_birth_imd_data(con, config$ons_birth_imd_table, "IMD_QUINTILE", config$ons_births_year) |> 
  dplyr::mutate(IMD_QUINTILE) |> 
  dplyr::arrange(IMD_QUINTILE)

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

# create a separate support dataset for live birth data
mat_imd_live_birth_data <- df_births_imd |> 
  dplyr::mutate(IMD_QUINTILE = as.character(IMD_QUINTILE)) |> 
  dplyr::arrange(IMD_QUINTILE) |> 
  dplyr::rename(
    !!paste0("ONS Live Births (", config$ons_births_year, ")") := LIVE_BIRTHS,
    "IMD Quintile" = IMD_QUINTILE
  )

# 4.2.6 MATEX: ICB profile (narrative = latest year only) ----------------------
# ICBs can vary in size and therefore are not appropriate for direct comparison
# Figures should be standardised by a population denominator
# population denominators for each ICB have been defined and applied in the HWHC_BI_ICB_POPULATION database table

#financial year
mat_icb_objs <- create_hes_icb_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'MAT',
  min_ym = config$min_trend_ym_mat,
  max_ym = config$max_trend_ym_mat,
  focus_fy = config$focus_fy_mat,
  subtype_split = FALSE,
  population_db_table = "HWHC_BI_ICB_POPULATION"
)

#calendar year
mat_icb_objs_cy <- create_hes_icb_objects_cy(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'MAT',
  min_ym = config$min_trend_ym_mat_cy,
  max_ym = config$max_trend_ym_mat_cy,
  subtype_split = FALSE,
  population_db_table = "HWHC_BI_ICB_POPULATION_CY"
)

#rename calendar year column
mat_icb_objs_cy$support_data <- mat_icb_objs_cy$support_data |>
  rename_df_fields()

# 4.3 Aggregation and analysis: Medical Exemption (MEDEX) ----------------------

# 4.3.1 MEDEX: Applications received (trend) -----------------------------------
# Applications will include any application to the scheme regardless of current status or outcome

# not required for narrative and only required for supporting data

#financial year
med_application_objs <- create_hes_application_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'MED',
  min_ym = config$min_trend_ym_med,
  max_ym = config$max_trend_ym_med,
  subtype_split = FALSE
)

#calendar year
med_application_objs_cy <- create_hes_application_objects_cy(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'MED',
  min_ym = config$min_trend_ym_med_cy,
  max_ym = config$max_trend_ym_med_cy,
  subtype_split = FALSE
)

#rename calendar year column
med_application_objs_cy$support_data <- med_application_objs_cy$support_data |>
  rename_df_fields()

# 4.3.2 MEDEX: Certificates issued (trend) -------------------------------------
# Issued certificates will only include cases where a certificate was issued to the customer

#financial year
med_issued_objs <- create_hes_issued_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'MED',
  min_ym = config$min_trend_ym_med,
  max_ym = config$max_trend_ym_med,
  subtype_split = FALSE
)

#calendar year
med_issued_objs_cy <- create_hes_issued_objects_cy(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'MED',
  min_ym = config$min_trend_ym_med_cy,
  max_ym = config$max_trend_ym_med_cy,
  subtype_split = FALSE
)

#rename calendar year column
med_issued_objs_cy$support_data <- med_issued_objs_cy$support_data |>
  rename_df_fields()

# 4.3.3 MEDEX: Age profile (narrative = latest year only) ----------------------
# Some processing has been performed to group by set age bands and reclassify potential errors

#financial year
med_age_objs <- create_hes_age_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'MED',
  min_ym = config$min_trend_ym_mat,
  max_ym = config$max_trend_ym_med,
  focus_fy = config$focus_fy_med,
  subtype_split = FALSE
)

#calendar year
med_age_objs_cy <- create_hes_age_objects_cy(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'MED',
  min_ym = config$min_trend_ym_mat_cy,
  max_ym = config$max_trend_ym_med_cy,
  subtype_split = FALSE
)

#rename calendar year column
med_age_objs_cy$support_data <- med_age_objs_cy$support_data |>
  rename_df_fields()

# 4.3.4 MEDEX: Deprivation profile (narrative = latest year only) --------------
# IMD may not be available if the postcode cannot be mapped to NSPL

#financial year
med_imd_objs <- create_hes_imd_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'MED',
  min_ym = config$min_trend_ym_mat,
  max_ym = config$max_trend_ym_med,
  focus_fy = config$focus_fy_med,
  subtype_split = FALSE
)

#calendar year
med_imd_objs_cy <- create_hes_imd_objects_cy(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'MED',
  min_ym = config$min_trend_ym_mat_cy,
  max_ym = config$max_trend_ym_med_cy,
  subtype_split = FALSE
)
#rename calendar year column
med_imd_objs_cy$support_data <- med_imd_objs_cy$support_data |>
  rename_df_fields()

# 4.3.5 MEDEX: ICB profile (narrative = latest year only) ----------------------
# ICBs can vary in size and therefore are not appropriate for direct comparison
# Figures should be standardised by a population denominator
# population denominators for each ICB have been defined and applied in the HWHC_BI_ICB_POPULATION database table

#financial year
med_icb_objs <- create_hes_icb_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'MED',
  min_ym = config$min_trend_ym_med,
  max_ym = config$max_trend_ym_med,
  focus_fy = config$focus_fy_med,
  subtype_split = FALSE,
  population_db_table = "HWHC_BI_ICB_POPULATION"
)

#calendar year
med_icb_objs_cy <- create_hes_icb_objects_cy(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'MED',
  min_ym = config$min_trend_ym_med_cy,
  max_ym = config$max_trend_ym_med_cy,
  subtype_split = FALSE,
  population_db_table = "HWHC_BI_ICB_POPULATION_CY"
)

#rename calendar year column
med_icb_objs_cy$support_data <- med_icb_objs_cy$support_data |>
  rename_df_fields()

# 4.4 Aggregation and analysis: Prescription Prepayment Certificate (PPC) ------

# 4.4.1 PPC: Applications received (trend) -------------------------------------
# Applications will include any application to the scheme regardless of current status or outcome
# Applications will be for a specific certificate type and therefore can be split

# not required for narrative and only required for supporting data

#financial year
ppc_application_objs <- create_hes_application_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'PPC',
  min_ym = config$min_trend_ym_ppc,
  max_ym = config$max_trend_ym_ppc,
  subtype_split = TRUE
)

#calendar year
ppc_application_objs_cy <- create_hes_application_objects_cy(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'PPC',
  min_ym = config$min_trend_ym_ppc_cy,
  max_ym = config$max_trend_ym_ppc_cy,
  subtype_split = TRUE
)

#rename calendar year column
ppc_application_objs_cy$support_data <- ppc_application_objs_cy$support_data |>
  rename_df_fields()

# 4.4.2 PPC: Certificates issued (trend) ---------------------------------------
# Issued certificates will only include cases where a certificate was issued to the customer

#financial year
ppc_issued_objs <- create_hes_issued_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'PPC',
  min_ym = config$min_trend_ym_ppc,
  max_ym = config$max_trend_ym_ppc,
  subtype_split = TRUE
)

#calendar year
ppc_issued_objs_cy <- create_hes_issued_objects_cy(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'PPC',
  min_ym = config$min_trend_ym_ppc_cy,
  max_ym = config$max_trend_ym_ppc_cy,
  subtype_split = TRUE
)

#rename calendar year column
ppc_issued_objs_cy$support_data <- ppc_issued_objs_cy$support_data |>
  rename_df_fields()

# 4.4.3 PPC: Age profile (narrative = latest year only) ------------------------
# Some processing applied to base dataset to reclassify ages that are outside of expected ranges as these may be errors

#financial year
ppc_age_objs <- create_hes_age_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'PPC',
  min_ym = config$min_trend_ym_ppc,
  max_ym = config$max_trend_ym_ppc,
  focus_fy = config$focus_fy_ppc,
  subtype_split = TRUE
)

#calendar year
ppc_age_objs_cy <- create_hes_age_objects_cy(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'PPC',
  min_ym = config$min_trend_ym_ppc_cy,
  max_ym = config$max_trend_ym_ppc_cy,
  subtype_split = TRUE
)

#rename calendar year column
ppc_age_objs_cy$support_data <- ppc_age_objs_cy$support_data |>
  rename_df_fields()

# 4.4.4 PPC: Deprivation profile (narrative = latest year only) ----------------
# IMD may not be available if the postcode cannot be mapped to NSPL

#financial year
ppc_imd_objs <- create_hes_imd_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'PPC',
  min_ym = config$min_trend_ym_ppc,
  max_ym = config$max_trend_ym_ppc,
  focus_fy = config$focus_fy_ppc,
  subtype_split = TRUE
)

#calendar year
ppc_imd_objs_cy <- create_hes_imd_objects_cy(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'PPC',
  min_ym = config$min_trend_ym_ppc_cy,
  max_ym = config$max_trend_ym_ppc_cy,
  subtype_split = TRUE
)

#rename calendar year column
ppc_imd_objs_cy$support_data <- ppc_imd_objs_cy$support_data |>
  rename_df_fields()

# 4.4.5 PPC: ICB profile (narrative = latest year only) ------------------------
# ICBs can vary in size and therefore are not appropriate for direct comparison
# Figures should be standardised by a population denominator
# population denominators for each ICB have been defined and applied in the HWHC_BI_ICB_POPULATION database table

#financial year
ppc_icb_objs <- create_hes_icb_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'PPC',
  min_ym = config$min_trend_ym_ppc,
  max_ym = config$max_trend_ym_ppc,
  focus_fy = config$focus_fy_ppc,
  subtype_split = TRUE,
  population_db_table = "HWHC_BI_ICB_POPULATION"
)

#calendar year
ppc_icb_objs_cy <- create_hes_icb_objects_cy(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'PPC',
  min_ym = config$min_trend_ym_ppc_cy,
  max_ym = config$max_trend_ym_ppc_cy,
  subtype_split = TRUE,
  population_db_table = "HWHC_BI_ICB_POPULATION_CY"
)

#rename calendar year column
ppc_icb_objs_cy$support_data <- ppc_icb_objs_cy$support_data |>
  rename_df_fields()

# 4.5 Aggregation and analysis: HRT PPC (HRTPPC) -------------------------------

# 4.5.1 HRTPPC: Applications received (trend) ----------------------------------
# Applications will include any application to the scheme regardless of current status or outcome

# not required for narrative and only required for supporting data

#financial year
hrt_application_objs <- create_hes_application_month_objects(
  db_connection = con,
  db_table_name = 'HWHC_HRTPPC_FACT',
  service_area = 'HRTPPC',
  min_ym = config$min_trend_ym_hrt,
  max_ym = config$max_trend_ym_hrt,
  subtype_split = FALSE
)

#calendar year
hrt_application_objs_cy <- create_hes_application_month_objects_cy(
  db_connection = con,
  db_table_name = 'HWHC_HRTPPC_FACT',
  service_area = 'HRTPPC',
  min_ym = config$min_trend_ym_hrt_cy,
  max_ym = config$max_trend_ym_hrt_cy,
  subtype_split = FALSE
)

# remove any March 23 certificates as HRTPPC service started April 23
# extract fixed by changing `min_trend_ym_hrt_cy` in config
# hrt_application_objs_cy$support_data <-
#   hrt_application_objs_cy$support_data |> dplyr::filter(Month != "Mar-23")

# 4.5.2 HRTPPC: Certificates issued (trend) ------------------------------------
# Issued certificates will only include cases where a certificate was issued to the customer

#financial year
hrt_issued_objs <- create_hes_issued_month_objects(
  db_connection = con,
  db_table_name = 'HWHC_HRTPPC_FACT',
  service_area = 'HRTPPC',
  min_ym = config$min_trend_ym_hrt,
  max_ym = config$max_trend_ym_hrt,
  subtype_split = FALSE
)

#calendar year
hrt_issued_objs_cy <- create_hes_issued_month_objects_cy(
  db_connection = con,
  db_table_name = 'HWHC_HRTPPC_FACT',
  service_area = 'HRTPPC',
  min_ym = config$min_trend_ym_hrt_cy,
  max_ym = config$max_trend_ym_hrt_cy,
  subtype_split = FALSE
)

# identify the proportion of issued certificates post-dated to the next month
# financial year months
df_hrt_issued_postdate <- dplyr::tbl(
  con, 
  from = dbplyr::in_schema(toupper(con@info$username), 'HWHC_HRTPPC_FACT')
) |> 
  # filter to service area and time periods
  dplyr::filter(
    SERVICE_AREA == toupper('HRTPPC'),
    ISSUE_YM >= config$min_trend_ym_hrt,
    ISSUE_YM <= config$max_trend_ym_hrt,
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

#calendar year months
df_hrt_issued_postdate_cy <- dplyr::tbl(
  con, 
  from = dbplyr::in_schema(toupper(con@info$username), 'HWHC_HRTPPC_FACT')
) |> 
  # filter to service area and time periods
  dplyr::filter(
    SERVICE_AREA == toupper('HRTPPC'),
    ISSUE_YM >= config$min_trend_ym_hrt_cy,
    ISSUE_YM <= config$max_trend_ym_hrt_cy,
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
  #dplyr::filter(ISSUE_YM >= 'config$chart_focus_ym') |>
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

# update the financial year support data object
hrt_issued_objs$support_data <- hrt_issued_objs$support_data |> 
  dplyr::left_join(
    y = df_hrt_issued_postdate |> dplyr::select(-ISSUED_CERTS),
    by = c("Month" = "ISSUE_YM")
  ) |> 
  rename_df_fields()

# update the calendar year support data object
hrt_issued_objs_cy$support_data <- hrt_issued_objs_cy$support_data |> 
  dplyr::left_join(
    y = df_hrt_issued_postdate_cy |> dplyr::select(-ISSUED_CERTS),
    by = c("Month" = "ISSUE_YM")
  ) |> 
  rename_df_fields()

# 4.5.3 HRTPPC: Age profile (narrative = latest year only) ---------------------
# Some processing applied to base dataset to reclassify ages that are outside of expected ranges as these may be errors

# create the age objects

#financial year
hrt_age_objs <- create_hes_age_objects(
  db_connection = con,
  db_table_name = 'HWHC_HRTPPC_FACT',
  service_area = 'HRTPPC',
  min_ym = config$min_trend_ym_hrt,
  max_ym = config$max_trend_ym_hrt,
  focus_fy = config$focus_fy_hrt,
  subtype_split = FALSE
)

#calendar year
hrt_age_objs_cy <- create_hes_age_objects_cy(
  db_connection = con,
  db_table_name = 'HWHC_HRTPPC_FACT',
  service_area = 'HRTPPC',
  min_ym = config$min_trend_ym_hrt_cy,
  max_ym = config$max_trend_ym_hrt_cy,
  subtype_split = FALSE
)

# identify the base population figures
hrt_age_base_pop_objs <- create_px_patient_age_objects(
  db_connection = con,
  db_table_name = 'HWHC_PX_PAT_FACT',
  patient_group = 'HRT_PATIENT_COUNT'
)

hrt_age_base_pop_objs_cy <- create_px_patient_age_objects(
  db_connection = con,
  db_table_name = 'HWHC_PX_PAT_FACT_CY',
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

# update the financial year support_data
hrt_age_objs$support_data <- hrt_age_objs$support_data |> 
  dplyr::left_join(
    y = hrt_age_base_pop_objs$chart_data,
    by = c('Financial Year', 'Age Band')
  ) |> 
  dplyr::rename(`Estimated patients receiving HRT PPC qualifying medication (aged 16-59)` = BASE_POPULATION)

#update the calendar year support data
hrt_age_objs_cy$support_data <- hrt_age_objs_cy$support_data |> 
  dplyr::left_join(
    y = hrt_age_base_pop_objs_cy$chart_data,
    by = c('Financial Year', 'Age Band')
  ) |> 
  dplyr::rename(`Estimated patients receiving HRT PPC qualifying medication (aged 16-59)` = BASE_POPULATION)

# 4.5.4 HRTPPC: Deprivation profile (narrative = latest year only) -------------
# IMD may not be available if the postcode cannot be mapped to NSPL

# create the IMD objects

#financial year
hrt_imd_objs <- create_hes_imd_objects(
  db_connection = con,
  db_table_name = 'HWHC_HRTPPC_FACT',
  service_area = 'HRTPPC',
  min_ym = config$min_trend_ym_hrt,
  max_ym = config$max_trend_ym_hrt,
  focus_fy = config$focus_fy_hrt,
  subtype_split = FALSE
)

#calendar year
hrt_imd_objs_cy <- create_hes_imd_objects_cy(
  db_connection = con,
  db_table_name = 'HWHC_HRTPPC_FACT',
  service_area = 'HRTPPC',
  min_ym = config$min_trend_ym_hrt_cy,
  max_ym = config$max_trend_ym_hrt_cy,
  subtype_split = FALSE
)

# identify the base population figures

#financial year
#may need to use HWHC_PX_PAT_FACT_FY depending on how pat fact table is set up
hrt_imd_base_pop_objs <- create_px_patient_imd_objects(
  db_connection = con,
  db_table_name = 'HWHC_PX_PAT_FACT',
  patient_group = 'HRT_PATIENT_COUNT',
  focus_fy = "2024/2025",
  min_age = config$hrt_min_pop_age,
  max_age = config$hrt_max_pop_age
)

#calendar year
hrt_imd_base_pop_objs_cy <- create_px_patient_imd_objects(
  db_connection = con,
  db_table_name = 'HWHC_PX_PAT_FACT_CY',
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

# update the financial year support_data
hrt_imd_objs$support_data <- hrt_imd_objs$support_data |> 
  dplyr::left_join(
    y = hrt_imd_base_pop_objs$chart_data |> dplyr::mutate(`IMD Quintile` = as.character(`IMD Quintile`)),
    by = c('IMD Quintile')
  ) |> 
  dplyr::rename(`Estimated patients receiving HRT PPC qualifying medication (aged 16-59)` = BASE_POPULATION)

# update the calendar year support_data
hrt_imd_objs_cy$support_data <- hrt_imd_objs_cy$support_data |> 
  dplyr::left_join(
    y = hrt_imd_base_pop_objs_cy$chart_data |> dplyr::mutate(`IMD Quintile` = as.character(`IMD Quintile`)),
    by = c('IMD Quintile')
  ) |> 
  dplyr::rename(`Estimated patients receiving HRT PPC qualifying medication (aged 16-59)` = BASE_POPULATION)

# 4.5.5 HRTPPC: ICB profile (narrative = latest year only) ---------------------
# ICBs can vary in size and therefore are not appropriate for direct comparison
# Figures should be standardised by a population denominator
# population denominators for each ICB have been defined and applied in the HWHC_BI_ICB_POPULATION database table

#financial year
hrt_icb_objs <- create_hes_icb_objects(
  db_connection = con,
  db_table_name = 'HWHC_HRTPPC_FACT',
  service_area = 'HRTPPC',
  min_ym = config$min_trend_ym_hrt,
  max_ym = config$max_trend_ym_hrt,
  focus_fy = config$focus_fy_hrt,
  subtype_split = FALSE,
  population_db_table = "HWHC_BI_ICB_POPULATION"
)

#calendar year
hrt_icb_objs_cy <- create_hes_icb_objects_cy(
  db_connection = con,
  db_table_name = 'HWHC_HRTPPC_FACT',
  service_area = 'HRTPPC',
  min_ym = config$min_trend_ym_hrt_cy,
  max_ym = config$max_trend_ym_hrt_cy,
  subtype_split = FALSE,
  population_db_table = "HWHC_BI_ICB_POPULATION_CY"
)

#rename calendar year column
hrt_icb_objs_cy$support_data <- hrt_icb_objs_cy$support_data |>
  rename_df_fields()

# 4.6 Aggregation and analysis: Tax Credit (TAX) -------------------------------

# 4.6.1 TAX: Certificates issued (trend) ---------------------------------------
# Issued certificates will only include cases where a certificate was issued to the customer

#financial year
tax_issued_objs <- create_hes_issued_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'TAX',
  min_ym = config$min_trend_ym_tax,
  max_ym = config$max_trend_ym_tax,
  subtype_split = FALSE
)

#calendar year
tax_issued_objs_cy <- create_hes_issued_objects_cy(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'TAX',
  min_ym = config$min_trend_ym_tax_cy,
  max_ym = config$max_trend_ym_tax_cy,
  subtype_split = FALSE
)

#rename calendar year column
tax_issued_objs_cy$support_data <- tax_issued_objs_cy$support_data |>
  rename_df_fields()

# 4.6.2 TAX: Age profile (narrative = latest year only) ------------------------
# Some processing has been performed to group by set age bands and reclassify potential errors 

#financial year
tax_age_objs <- create_hes_age_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'TAX',
  min_ym = config$min_trend_ym_tax,
  max_ym = config$max_trend_ym_tax,
  focus_fy = config$focus_fy_tax,
  subtype_split = FALSE
)

#calendar year
tax_age_objs_cy <- create_hes_age_objects_cy(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'TAX',
  min_ym = config$min_trend_ym_tax_cy,
  max_ym = config$max_trend_ym_tax_cy,
  subtype_split = FALSE
)

#rename calendar year column
tax_age_objs_cy$support_data <- tax_age_objs_cy$support_data |>
  rename_df_fields()

# 4.6.3 TAX: Deprivation profile (narrative = latest year only) ----------------
# IMD may not be available if the postcode cannot be mapped to NSPL

#financial year
tax_imd_objs <- create_hes_imd_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'TAX',
  min_ym = config$min_trend_ym_tax,
  max_ym = config$max_trend_ym_tax,
  focus_fy = config$focus_fy_tax,
  subtype_split = FALSE
)

#calendar year
tax_imd_objs_cy <- create_hes_imd_objects_cy(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'TAX',
  min_ym = config$min_trend_ym_tax_cy,
  max_ym = config$max_trend_ym_tax_cy,
  subtype_split = FALSE
)

#rename calendar year column
tax_imd_objs_cy$support_data <- tax_imd_objs_cy$support_data |>
  rename_df_fields()

# 4.6.4 TAX: ICB profile (narrative = latest year only) ------------------------
# ICBs can vary in size and therefore are not appropriate for direct comparison
# Figures should be standardised by a population denominator
# population denominators for each ICB have been defined and applied in the HWHC_BI_ICB_POPULATION database table

#financial year
tax_icb_objs <- create_hes_icb_objects(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'TAX',
  min_ym = config$min_trend_ym_tax,
  max_ym = config$max_trend_ym_tax,
  focus_fy = config$focus_fy_tax,
  subtype_split = FALSE,
  population_db_table = "HWHC_BI_ICB_POPULATION"
)

#calendar year
tax_icb_objs_cy <- create_hes_icb_objects_cy(
  db_connection = con,
  db_table_name = 'HWHC_HES_FACT',
  service_area = 'TAX',
  min_ym = config$min_trend_ym_tax_cy,
  max_ym = config$max_trend_ym_tax_cy,
  subtype_split = FALSE,
  population_db_table = "HWHC_BI_ICB_POPULATION_CY"
)

#rename calendar year column
tax_icb_objs_cy$support_data <- tax_icb_objs_cy$support_data |>
  rename_df_fields()

# 4.7 Close database connection ------------------------------------------------

# close connection to database
DBI::dbDisconnect(con)


# 5. Data tables - Financial year ----------------------------------------------

# data tables for spreadsheet outputs
# formatted according to accessibility standards
# user may need to update file name to write outputs to in future releases


# 5.1 Define sheets and metadata -----------------------------------------------

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
  "Certificate Type: 12-month (PPC)",
  "Certificate Type: 3-month (PPC)",
  "Certificate Duration",
  "Age Band",
  "IMD Quintile",
  "Integrated care boards (ICBs)",
  "ICB Code",
  "ICB Name",
  "Number of applications received",
  "Number of certificates issued",
  "Base Population Classification",
  "Base Population",
  "Number of issued certificates per 10,000 population",
  "Number of issued certificates post-dated to start the following month",
  "Issued certificates post-dated to start the following month (%)",
  "Estimated patients receiving HRT PPC qualifying medication (aged 16-59)"
  
)

meta_descs <-
  c(
    "Name of NHSBSA administered NHS Help with Health Costs service. More information on these services can be found in the background and methodology document for this publication.",
    "Financial year the activity can be assigned to, which runs from April to March. For example, 2024/25 covers April 2024 to March 2025. For reporting of number of applications, this will represent the financial year the application was received. For reporting of issued certificates, this will represent the financial year in which the certificate was issued.",
    "Country classification based on the applicants residential address, using mapping via the National Statistics Postcode Lookup (NSPL) - August 2024. Not included for services typically available to English residents only.",
    "Where the applicant's residential address can be aligned to an English postcode via the National Statistics Postcode Lookup (NSPL) - August 2024",
    "Where the applicant's residential address can be assigned to a country other than England via the National Statistics Postcode Lookup (NSPL) - August 2024, the country will be recorded as 'Other'.",
    "Where the applicant's residential address cannot be assigned to any country via the National Statistics Postcode Lookup (NSPL) - August 2024, the country will be recorded as 'Unknown'.",
    "Where distinct certificate types are available, this field will show which certificate was applicable.",
    "HC2 certificates provide full help with health costs, including free NHS prescriptions.",
    "HC3 certificates provide limited help with health costs. A HC3 certificate will show how much the holder has to pay towards health costs.",
    "A 12-month PPC will cover all NHS prescription charges for a period of 12-months for a set cost.",
    "A 3-month PPC will cover all NHS prescription charges for a period of 3-months for a set cost.",
    "Certificate duration has been grouped into categories based on the number of months between the start and end date for the certificate, rounded to nearest number of months.",
    "The age band of the applicant, based on the age at the time the application was received and processed. Age will be rounded to the nearest year and reported in 5 year age bands.",
    "IMD quintiles are based on the English Indices of Deprivation 2019. IMD quintiles are calculated by ranking census lower-layer super output areas (LSOAs) from most deprived to least deprived and dividing them into equal groups. Quintiles range from the most deprived 20% (quintile 1) of small areas nationally to the least deprived 20% (quintile 5) of small areas nationally. People are aligned to an IMD quintile based on mapping their postcode to an LSOA (2011 classification) using the National Statistics Postcode Lookup (NSPL) - August 2024",
    "Integrated care boards (ICBs) are a statutory NHS organisation responsible for developing a plan in collaboration with NHS trusts/foundation trusts and other system partners for meeting the health needs of the population, managing the NHS budget and arranging for the provision of health services in the defined area. They took over the functions of Clinical Commissioning Groups (CCG) in July 2022. People are aligned to an ICB based on mappings using the National Statistics Postcode Lookup (NSPL) - August 2024",
    "Three character code unique to an ICB.",
    "Full name for each ICB.",
    "Number of applications received by NHSBSA. Includes applications via any route. Includes all applications regardless of status or outcome.",
    "Number of certificates issued to applicants following processing of applications. Will exclude ongoing or incomplete applications.",
    "For ICB Breakdown reporting this field will identify the base population group used to calculate rates.",
    "For ICB Breakdown reporting this field will number of people identified for the base population group.",
    "For ICB level reporting data is presented as rates per 10,000 population to prevent ICB size unfairly impacting results. Figures calculated using the number of issued certificates and the appropriate population denominator, with rates reported per 10,000 population.",
    "HRT PPC only. Showing the number of certificates where the applicant requested the certificate to start the following month. Applicants can specify when they would like their certificate to start, up to one month after the application date. This can help applicants apply for a new certificate to start as soon as their existing certificate expires.",
    "HRT PPC only. Showing the proportion of certificates issed where the applicant requested the certificate to start the following month. Applicants can specify when they would like their certificate to start, up to one month after the application date. This can help applicants apply for a new certificate to start as soon as their existing certificate expires.",
    "HRT PPC only. Showing the estimated number of patients who could be identified receiving NHS prescriptions for medication that is eligible for support via a HRT PPC. Patient counts are limited to patients receiving electronic prescribing where a date of birth was available and the patient could be assigned to a ICB based on their captured postcode. Prescribing limited to medications flagged as eligible for support via the HRT PPC at any point during the reporting period."
  )

accessibleTables::create_metadata(wb,
                                  meta_fields,
                                  meta_descs)


# 5.2 Data Tables: NHS Low Income Scheme ---------------------------------------

# 5.2.1 LIS: Applications ------------------------------------------------------

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
# Values: right align * thousand separator
accessibleTables::format_data(wb, "LIS_Applications", c("D"), "right", "#,###")

# 5.2.2 LIS: Issued ------------------------------------------------------------

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

# 5.2.3 LIS: Duration ----------------------------------------------------------

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

# 5.2.4 LIS: Age ---------------------------------------------------------------

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

# 5.2.5 LIS: Deprivation -------------------------------------------------------

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

# 5.2.6 LIS: ICB ---------------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "LIS_ICB_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued NHS Low Income Scheme HC2/HC3 certificates, split by financial year and ICB"),
  notes = c(config$caveat_lis_issued, config$caveat_icb_method, config$caveat_icb_restriction, config$caveat_icb_missing_data, config$caveat_lis_base_population, config$caveat_country_other, config$caveat_country_unknown),
  dataset = lis_icb_objs$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "LIS_ICB_Breakdown", c("A","B","C","D","E","G"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "LIS_ICB_Breakdown", c("F","H","I","J","K"), "right", "#,###")


# 5.3 Data Tables: Maternity exemption certificate -----------------------------

# 5.3.1 MATEX: Applications ----------------------------------------------------

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

# 5.3.2 MATEX: Issued ----------------------------------------------------------

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

# 5.3.3 MATEX: Duration --------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "MATEX_Certificate_Duration",
  title = paste0(config$publication_table_title, " - Number of issued maternity exemption certificates, split by financial year and certificate duration"),
  notes = c(config$caveat_cert_issued, config$caveat_mat_duration_method, config$caveat_mat_duration_notes, config$caveat_mat_duration_restriction, config$caveat_mat_country),
  dataset = mat_duration_objs$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "MATEX_Certificate_Duration", c("A","B"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "MATEX_Certificate_Duration", c("C","D"), "right", "#,###")

# 5.3.4 MATEX: Age -------------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "MATEX_Age_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued maternity exemption certificates, split by financial year and age of applicant"),
  notes = c(config$caveat_cert_issued, config$caveat_mat_age_group, config$caveat_age_restriction, config$caveat_mat_country),
  dataset = mat_age_objs$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "MATEX_Age_Breakdown", c("A","B","C"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "MATEX_Age_Breakdown", c("D"), "right", "#,###")

# 5.3.5 MATEX: Deprivation -----------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "MATEX_Deprivation_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued maternity exemption certificates, split by financial year and IMD quintile"),
  notes = c(config$caveat_cert_issued, config$caveat_imd_restriction, config$caveat_mat_country),
  dataset = mat_imd_objs$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "MATEX_Deprivation_Breakdown", c("A","B","C"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "MATEX_Deprivation_Breakdown", c("D"), "right", "#,###")

# 5.3.6 MATEX: ICB -------------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "MATEX_ICB_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued maternity exemption certificates, split by financial year and ICB"),
  notes = c(config$caveat_cert_issued, config$caveat_icb_method, config$caveat_icb_restriction, config$caveat_icb_missing_data, config$caveat_mat_base_population),
  dataset = mat_icb_objs$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "MATEX_ICB_Breakdown", c("A","B","C","D","F"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "MATEX_ICB_Breakdown", c("E","G","H"), "right", "#,###")

# 5.4 Data Tables: Medical exemption certificate --------------------------------

# 5.4.1 MEDEX: Applications ----------------------------------------------------

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

# 5.4.2 MEDEX: Issued ----------------------------------------------------------

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

# 5.4.3 MEDEX: Age -------------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "MEDEX_Age_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued medical exemption certificates, split by financial year and  age of applicant"),
  notes = c(config$caveat_cert_issued, config$caveat_med_age_group, config$caveat_age_restriction, config$caveat_med_country),
  dataset = med_age_objs$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "MEDEX_Age_Breakdown", c("A","B","C"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "MEDEX_Age_Breakdown", c("D"), "right", "#,###")

# 5.4.4 MEDEX: Deprivation -----------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "MEDEX_Deprivation_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued medical exemption certificates, split by financial year and IMD quintile"),
  notes = c(config$caveat_cert_issued, config$caveat_imd_restriction, config$caveat_med_country),
  dataset = med_imd_objs$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "MEDEX_Deprivation_Breakdown", c("A","B","C"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "MEDEX_Deprivation_Breakdown", c("D"), "right", "#,###")

# 5.4.5 MEDEX: ICB -------------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "MEDEX_ICB_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued medical exemption certificates, split by financial year and  ICB"),
  notes = c(config$caveat_cert_issued, config$caveat_icb_method, config$caveat_icb_restriction, config$caveat_icb_missing_data, config$caveat_med_base_population),
  dataset = med_icb_objs$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "MEDEX_ICB_Breakdown", c("A","B","C","D","F"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "MEDEX_ICB_Breakdown", c("E","G","H"), "right", "#,###")

# 5.5 Data Tables: PPC certificate ---------------------------------------------

# 5.5.1 PPC: Applications ------------------------------------------------------

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

# 5.5.2 PPC: Issued ------------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "PPC_Issued",
  title = paste0(config$publication_table_title, " - Number of prescription prepayment certificates issued, split by financial year and certificate type"),
  notes = c(config$caveat_cert_issued, config$caveat_ppc_country),
  dataset = ppc_issued_objs$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "PPC_Issued", c("A","B","C"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "PPC_Issued", c("D"), "right", "#,###")

# 5.5.3 PPC: Age ---------------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "PPC_Age_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued prescription prepayment certificates, split by financial year, certificate type and age of applicant"),
  notes = c(config$caveat_cert_issued, config$caveat_ppc_age_group, config$caveat_age_restriction, config$caveat_ppc_country),
  dataset = ppc_age_objs$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "PPC_Age_Breakdown", c("A","B","C","D"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "PPC_Age_Breakdown", c("E"), "right", "#,###")

# 5.5.4 PPC: Deprivation -------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "PPC_Deprivation_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued prescription prepayment certificates, split by financial year, certificate type and IMD quintile"),
  notes = c(config$caveat_cert_issued, config$caveat_imd_restriction, config$caveat_ppc_country),
  dataset = ppc_imd_objs$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "PPC_Deprivation_Breakdown", c("A","B","C","D"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "PPC_Deprivation_Breakdown", c("E"), "right", "#,###")

# 5.5.5 PPC: ICB ---------------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "PPC_ICB_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued prescription prepayment certificates, split by financial year and ICB"),
  notes = c(config$caveat_cert_issued, config$caveat_icb_method, config$caveat_icb_restriction, config$caveat_icb_missing_data, config$caveat_ppc_base_population),
  dataset = ppc_icb_objs$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "PPC_ICB_Breakdown", c("A","B","C","D","F"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "PPC_ICB_Breakdown", c("E","G","H","I","J","K"), "right", "#,###")


# 5.6 Data Tables: HRT PPC certificate -----------------------------------------

# 5.6.1 HRT PPC: Applications --------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "HRTPPC_Applications",
  title = paste0(config$publication_table_title, " - Number of applications for NHS Hormone Replacement Therapy Prescription Prepayment Certificate (HRT PPC) split by month"),
  notes = c(config$caveat_hrtppc_start, config$caveat_hrtppc_country),
  dataset = hrt_application_objs$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "HRTPPC_Applications", c("A","B"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "HRTPPC_Applications", c("C"), "right", "#,###")

# 5.6.2 HRT PPC: Issued --------------------------------------------------------

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

# 5.6.3 HRT PPC: Age -----------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "HRTPPC_Age_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued NHS Hormone Replacement Therapy Prescription Prepayment Certificate (HRT PPC), split by financial year and age of applicant"),
  notes = c(config$caveat_cert_issued, config$caveat_hrtppc_age_group, config$caveat_hrtppc_px_data, config$caveat_hrtppc_country),
  dataset = hrt_age_objs$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "HRTPPC_Age_Breakdown", c("A","B","C"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "HRTPPC_Age_Breakdown", c("D","E"), "right", "#,###")

# 5.6.4 HRT PPC: Deprivation ---------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "HRTPPC_Deprivation_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued NHS Hormone Replacement Therapy Prescription Prepayment Certificate (HRT PPC), split by financial year and IMD quintile"),
  notes = c(config$caveat_cert_issued, config$caveat_imd_restriction, config$caveat_hrtppc_px_data, config$caveat_hrtppc_country),
  dataset = hrt_imd_objs$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "HRTPPC_Deprivation_Breakdown", c("A","B","C"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "HRTPPC_Deprivation_Breakdown", c("D","E"), "right", "#,###")

# 5.6.5 HRT PPC: ICB -----------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "HRTPPC_ICB_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued NHS Hormone Replacement Therapy Prescription Prepayment Certificate (HRT PPC), split by financial year and ICB"),
  notes = c(config$caveat_cert_issued, config$caveat_icb_method, config$caveat_icb_restriction, config$caveat_icb_missing_data, config$caveat_hrtppc_base_population),
  dataset = hrt_icb_objs$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "HRTPPC_ICB_Breakdown", c("A","B","C","D","F"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "HRTPPC_ICB_Breakdown", c("E","G","H"), "right", "#,###")

# 5.7 Data Tables: NHS Tax Credit Exemption Certificates -----------------------

# 5.7.1 TAX: Issued ------------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "TAX_Issued",
  title = paste0(config$publication_table_title, " - Number of NHS Tax Credit Exemption Certificates issued, split by financial year and country"),
  notes = c(config$caveat_tax_issued, config$caveat_country_other, config$caveat_country_unknown),
  dataset = tax_issued_objs$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "TAX_Issued", c("A","B","C"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "TAX_Issued", c("D"), "right", "#,###")

# 5.7.2 TAX: Age ---------------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "TAX_Age_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued NHS Tax Credit Exemption Certificates, split by financial year, country and age of applicant"),
  notes = c(config$caveat_tax_age_band, config$caveat_age_restriction, config$caveat_country_other, config$caveat_country_unknown),
  dataset = tax_age_objs$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "TAX_Age_Breakdown", c("A","B","C","D"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "TAX_Age_Breakdown", c("E"), "right", "#,###")

# 5.7.4 TAX: Deprivation -------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "TAX_Deprivation_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued NHS Tax Credit Exemption Certificates, split by financial year, country and IMD quintile"),
  notes = c(config$caveat_imd_restriction, config$caveat_country_other, config$caveat_country_unknown),
  dataset = tax_imd_objs$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "TAX_Deprivation_Breakdown", c("A","B","C","D"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "TAX_Deprivation_Breakdown", c("E"), "right", "#,###")

# 5.7.5 TAX: ICB ---------------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "TAX_ICB_Breakdown",
  title = paste0(config$publication_table_title, " - Number of issued NHS Tax Credit Exemption Certificates, split by financial year and ICB"),
  notes = c(config$caveat_icb_method, config$caveat_icb_restriction, config$caveat_icb_missing_data, config$caveat_tax_base_population),
  dataset = tax_icb_objs$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "TAX_ICB_Breakdown", c("A","B","C","D","E","G"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "TAX_ICB_Breakdown", c("F","H","I"), "right", "#,###")

# 5.8 Cover Sheet --------------------------------------------------------------

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

# 6. Data tables - Calendar year -----------------------------------------------

# data tables for spreadsheet outputs
# formatted according to accessibility standards
# user may need to update file name to write outputs to in future releases

# 6.1 Define sheets and metadata -----------------------------------------------

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
  "Calendar Year",
  "Country",
  "Country: England",
  "Country: Other",
  "Country: Unknown",
  "Certificate Type",
  "Certificate Type: HC2 (NHS Low Income Scheme)",
  "Certificate Type: HC3 (NHS Low Income Scheme)",
  "Certificate Type: 12-month (PPC)",
  "Certificate Type: 3-month (PPC)",
  "Certificate Duration",
  "Age Band",
  "IMD Quintile",
  "Integrated care boards (ICBs)",
  "ICB Code",
  "ICB Name",
  "Number of applications received",
  "Number of certificates issued",
  "Base Population Classification",
  "Base Population",
  "Number of issued certificates per 10,000 population",
  "Number of issued certificates post-dated to start the following month",
  "Issued certificates post-dated to start the following month (%)",
  "Estimated patients receiving HRT PPC qualifying medication (aged 16-59)"
  
)

meta_descs <-
  c(
    "Name of NHSBSA administered NHS Help with Health Costs service. More information on these services can be found in the background and methodology document for this publication.",
    "Calendar year the activity can be assigned to. For reporting of number of applications, this will represent the calendar year the application was received. For reporting of issued certificates, this will represent the calendar year in which the certificate was issued.",
    "Country classification based on the applicants residential address, using mapping via the National Statistics Postcode Lookup (NSPL) - August 2024. Not included for services typically available to English residents only.",
    "Where the applicant's residential address can be aligned to an English postcode via the National Statistics Postcode Lookup (NSPL) - August 2024",
    "Where the applicant's residential address can be assigned to a country other than England via the National Statistics Postcode Lookup (NSPL) - August 2024, the country will be recorded as 'Other'.",
    "Where the applicant's residential address cannot be assigned to any country via the National Statistics Postcode Lookup (NSPL) - August 2024, the country will be recorded as 'Unknown'.",
    "Where distinct certificate types are available, this field will show which certificate was applicable.",
    "HC2 certificates provide full help with health costs, including free NHS prescriptions.",
    "HC3 certificates provide limited help with health costs. A HC3 certificate will show how much the holder has to pay towards health costs.",
    "A 12-month PPC will cover all NHS prescription charges for a period of 12-months for a set cost.",
    "A 3-month PPC will cover all NHS prescription charges for a period of 3-months for a set cost.",
    "Certificate duration has been grouped into categories based on the number of months between the start and end date for the certificate, rounded to nearest number of months.",
    "The age band of the applicant, based on the age at the time the application was received and processed. Age will be rounded to the nearest year and reported in 5 year age bands.",
    "IMD quintiles are based on the English Indices of Deprivation 2019. IMD quintiles are calculated by ranking census lower-layer super output areas (LSOAs) from most deprived to least deprived and dividing them into equal groups. Quintiles range from the most deprived 20% (quintile 1) of small areas nationally to the least deprived 20% (quintile 5) of small areas nationally. People are aligned to an IMD quintile based on mapping their postcode to an LSOA (2011 classification) using the National Statistics Postcode Lookup (NSPL) - August 2024",
    "Integrated care boards (ICBs) are a statutory NHS organisation responsible for developing a plan in collaboration with NHS trusts/foundation trusts and other system partners for meeting the health needs of the population, managing the NHS budget and arranging for the provision of health services in the defined area. They took over the functions of Clinical Commissioning Groups (CCG) in July 2022. People are aligned to an ICB based on mappings using the National Statistics Postcode Lookup (NSPL) - August 2024",
    "Three character code unique to an ICB.",
    "Full name for each ICB.",
    "Number of applications received by NHSBSA. Includes applications via any route. Includes all applications regardless of status or outcome.",
    "Number of certificates issued to applicants following processing of applications. Will exclude ongoing or incomplete applications.",
    "For ICB Breakdown reporting this field will identify the base population group used to calculate rates.",
    "For ICB Breakdown reporting this field will number of people identified for the base population group.",
    "For ICB level reporting data is presented as rates per 10,000 population to prevent ICB size unfairly impacting results. Figures calculated using the number of issued certificates and the appropriate population denominator, with rates reported per 10,000 population.",
    "HRT PPC only. Showing the number of certificates where the applicant requested the certificate to start the following month. Applicants can specify when they would like their certificate to start, up to one month after the application date. This can help applicants apply for a new certificate to start as soon as their existing certificate expires.",
    "HRT PPC only. Showing the proportion of certificates issed where the applicant requested the certificate to start the following month. Applicants can specify when they would like their certificate to start, up to one month after the application date. This can help applicants apply for a new certificate to start as soon as their existing certificate expires.",
    "HRT PPC only. Showing the estimated number of patients who could be identified receiving NHS prescriptions for medication that is eligible for support via a HRT PPC. Patient counts are limited to patients receiving electronic prescribing where a date of birth was available and the patient could be assigned to a ICB based on their captured postcode. Prescribing limited to medications flagged as eligible for support via the HRT PPC at any point during the reporting period."
  )

accessibleTables::create_metadata(wb,
                                  meta_fields,
                                  meta_descs)


# 6.2 Data Tables: NHS Low Income Scheme ---------------------------------------

# 6.2.1 LIS: Applications ------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "LIS_Applications",
  title = paste0(config$publication_table_title_cy, " - Number of applications to NHS Low Income Scheme split by calendar year and country"),
  notes = c(config$caveat_country_other, config$caveat_country_unknown),
  dataset = lis_application_objs_cy$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "LIS_Applications", c("A","B","C"), "left", "")
# Values: right align * thousand separator
accessibleTables::format_data(wb, "LIS_Applications", c("D"), "right", "#,###")

# 6.2.2 LIS: Issued ------------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "LIS_Issued",
  title = paste0(config$publication_table_title_cy, " - Number of NHS Low Income Scheme HC2/HC3 certificates issued, split by calendar year, country and certificate type"),
  notes = c(config$caveat_lis_issued, config$caveat_country_other, config$caveat_country_unknown),
  dataset = lis_issued_objs_cy$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "LIS_Issued", c("A","B","C","D"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "LIS_Issued", c("E"), "right", "#,###")

# 6.2.3 LIS: Duration ----------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "LIS_Certificate_Duration",
  title = paste0(config$publication_table_title_cy, " - Number of issued NHS Low Income Scheme HC2/HC3 certificates, split by calendar year, country and certificate duration"),
  notes = c(config$caveat_lis_issued, config$caveat_lis_duration_group, config$caveat_lis_duration_notes, config$caveat_country_other, config$caveat_country_unknown),
  dataset = lis_duration_objs_cy$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "LIS_Certificate_Duration", c("A","B","C","D","E"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "LIS_Certificate_Duration", c("F"), "right", "#,###")

# 6.2.4 LIS: Age ---------------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "LIS_Age_Breakdown",
  title = paste0(config$publication_table_title_cy, " - Number of issued NHS Low Income Scheme HC2/HC3 certificates, split by calendar year, country and age of applicant"),
  notes = c(config$caveat_lis_issued, config$caveat_lis_age_group, config$caveat_age_restriction, config$caveat_country_other, config$caveat_country_unknown),
  dataset = lis_age_objs_cy$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "LIS_Age_Breakdown", c("A","B","C","D","E"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "LIS_Age_Breakdown", c("F"), "right", "#,###")

# 6.2.5 LIS: Deprivation -------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "LIS_Deprivation_Breakdown",
  title = paste0(config$publication_table_title_cy, " - Number of issued NHS Low Income Scheme HC2/HC3 certificates, split by calendar year, country and IMD quintile"),
  notes = c(config$caveat_lis_issued, config$caveat_imd_restriction, config$caveat_country_other, config$caveat_country_unknown),
  dataset = lis_imd_objs_cy$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "LIS_Deprivation_Breakdown", c("A","B","C","D","E"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "LIS_Deprivation_Breakdown", c("F"), "right", "#,###")

# 6.2.6 LIS: ICB ---------------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "LIS_ICB_Breakdown",
  title = paste0(config$publication_table_title_cy, " - Number of issued NHS Low Income Scheme HC2/HC3 certificates, split by calendar year and ICB"),
  notes = c(config$caveat_lis_issued, config$caveat_icb_method, config$caveat_icb_restriction, config$caveat_icb_missing_data, config$caveat_lis_base_population, config$caveat_country_other, config$caveat_country_unknown),
  dataset = lis_icb_objs_cy$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "LIS_ICB_Breakdown", c("A","B","C","D","E","G"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "LIS_ICB_Breakdown", c("F","H","I","J","K"), "right", "#,###")


# 6.3 Data Tables: Maternity exemption certificate -----------------------------

# 6.3.1 MATEX: Applications ----------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "MATEX_Applications",
  title = paste0(config$publication_table_title_cy, " - Number of applications for maternity exemption certificates split by calendar year"),
  notes = c(config$caveat_mat_country),
  dataset = mat_application_objs_cy$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "MATEX_Applications", c("A","B"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "MATEX_Applications", c("C"), "right", "#,###")

# 6.3.2 MATEX: Issued ----------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "MATEX_Issued",
  title = paste0(config$publication_table_title_cy, " - Number of maternity exemption certificates issued, split by calendar year"),
  notes = c(config$caveat_cert_issued, config$caveat_mat_country),
  dataset = mat_issued_objs_cy$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "MATEX_Issued", c("A","B"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "MATEX_Issued", c("C"), "right", "#,###")

# 6.3.3 MATEX: Duration --------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "MATEX_Certificate_Duration",
  title = paste0(config$publication_table_title_cy, " - Number of issued maternity exemption certificates, split by calendar year and certificate duration"),
  notes = c(config$caveat_cert_issued, config$caveat_mat_duration_method, config$caveat_mat_duration_notes, config$caveat_mat_duration_restriction, config$caveat_mat_country),
  dataset = mat_duration_objs_cy$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "MATEX_Certificate_Duration", c("A","B"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "MATEX_Certificate_Duration", c("C","D"), "right", "#,###")

# 6.3.4 MATEX: Age -------------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "MATEX_Age_Breakdown",
  title = paste0(config$publication_table_title_cy, " - Number of issued maternity exemption certificates, split by calendar year and age of applicant"),
  notes = c(config$caveat_cert_issued, config$caveat_mat_age_group, config$caveat_age_restriction, config$caveat_mat_country),
  dataset = mat_age_objs_cy$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "MATEX_Age_Breakdown", c("A","B","C"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "MATEX_Age_Breakdown", c("D"), "right", "#,###")

# 6.3.5 MATEX: Deprivation -----------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "MATEX_Deprivation_Breakdown",
  title = paste0(config$publication_table_title_cy, " - Number of issued maternity exemption certificates, split by calendar year and IMD quintile"),
  notes = c(config$caveat_cert_issued, config$caveat_imd_restriction, config$caveat_mat_country),
  dataset = mat_imd_objs_cy$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "MATEX_Deprivation_Breakdown", c("A","B","C"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "MATEX_Deprivation_Breakdown", c("D"), "right", "#,###")

# 6.3.6 MATEX: ICB -------------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "MATEX_ICB_Breakdown",
  title = paste0(config$publication_table_title_cy, " - Number of issued maternity exemption certificates, split by calendar year and ICB"),
  notes = c(config$caveat_cert_issued, config$caveat_icb_method, config$caveat_icb_restriction, config$caveat_icb_missing_data, config$caveat_mat_base_population),
  dataset = mat_icb_objs_cy$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "MATEX_ICB_Breakdown", c("A","B","C","D","F"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "MATEX_ICB_Breakdown", c("E","G","H"), "right", "#,###")

# 6.4 Data Tables: Medical exemption certificate -------------------------------

# 6.4.1 MEDEX: Applications ----------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "MEDEX_Applications",
  title = paste0(config$publication_table_title_cy, " - Number of applications for medical exemption certificates split by calendar year"),
  notes = c(config$caveat_med_country),
  dataset = med_application_objs_cy$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "MEDEX_Applications", c("A","B"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "MEDEX_Applications", c("C"), "right", "#,###")

# 6.4.2 MEDEX: Issued ----------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "MEDEX_Issued",
  title = paste0(config$publication_table_title_cy, " - Number of medical exemption certificates issued, split by calendar year"),
  notes = c(config$caveat_cert_issued, config$caveat_med_country),
  dataset = med_issued_objs_cy$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "MEDEX_Issued", c("A","B"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "MEDEX_Issued", c("C"), "right", "#,###")

# 6.4.3 MEDEX: Age -------------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "MEDEX_Age_Breakdown",
  title = paste0(config$publication_table_title_cy, " - Number of issued medical exemption certificates, split by calendar year and  age of applicant"),
  notes = c(config$caveat_cert_issued, config$caveat_med_age_group, config$caveat_age_restriction, config$caveat_med_country),
  dataset = med_age_objs_cy$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "MEDEX_Age_Breakdown", c("A","B","C"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "MEDEX_Age_Breakdown", c("D"), "right", "#,###")

# 6.4.4 MEDEX: Deprivation -----------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "MEDEX_Deprivation_Breakdown",
  title = paste0(config$publication_table_title_cy, " - Number of issued medical exemption certificates, split by calendar year and IMD quintile"),
  notes = c(config$caveat_cert_issued, config$caveat_imd_restriction, config$caveat_med_country),
  dataset = med_imd_objs_cy$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "MEDEX_Deprivation_Breakdown", c("A","B","C"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "MEDEX_Deprivation_Breakdown", c("D"), "right", "#,###")

# 6.4.5 MEDEX: ICB -------------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "MEDEX_ICB_Breakdown",
  title = paste0(config$publication_table_title_cy, " - Number of issued medical exemption certificates, split by calendar year and  ICB"),
  notes = c(config$caveat_cert_issued, config$caveat_icb_method, config$caveat_icb_restriction, config$caveat_icb_missing_data, config$caveat_med_base_population),
  dataset = med_icb_objs_cy$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "MEDEX_ICB_Breakdown", c("A","B","C","D","F"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "MEDEX_ICB_Breakdown", c("E","G","H"), "right", "#,###")

# 6.5 Data Tables: PPC certificate ---------------------------------------------

# 6.5.1 PPC: Applications ------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "PPC_Applications",
  title = paste0(config$publication_table_title_cy, " - Number of applications for prescription prepayment certificates split by calendar year and certificate type"),
  notes = c(config$caveat_ppc_type_unknown, config$caveat_ppc_country),
  dataset = ppc_application_objs_cy$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "PPC_Applications", c("A","B","C"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "PPC_Applications", c("D"), "right", "#,###")

# 6.5.2 PPC: Issued ------------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "PPC_Issued",
  title = paste0(config$publication_table_title_cy, " - Number of prescription prepayment certificates issued, split by calendar year and certificate type"),
  notes = c(config$caveat_cert_issued, config$caveat_ppc_country),
  dataset = ppc_issued_objs_cy$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "PPC_Issued", c("A","B","C"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "PPC_Issued", c("D"), "right", "#,###")

# 6.5.3 PPC: Age ---------------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "PPC_Age_Breakdown",
  title = paste0(config$publication_table_title_cy, " - Number of issued prescription prepayment certificates, split by calendar year, certificate type and age of applicant"),
  notes = c(config$caveat_cert_issued, config$caveat_ppc_age_group, config$caveat_age_restriction, config$caveat_ppc_country),
  dataset = ppc_age_objs_cy$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "PPC_Age_Breakdown", c("A","B","C","D"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "PPC_Age_Breakdown", c("E"), "right", "#,###")

# 6.5.4 PPC: Deprivation -------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "PPC_Deprivation_Breakdown",
  title = paste0(config$publication_table_title_cy, " - Number of issued prescription prepayment certificates, split by calendar year, certificate type and IMD quintile"),
  notes = c(config$caveat_cert_issued, config$caveat_imd_restriction, config$caveat_ppc_country),
  dataset = ppc_imd_objs_cy$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "PPC_Deprivation_Breakdown", c("A","B","C","D"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "PPC_Deprivation_Breakdown", c("E"), "right", "#,###")

# 6.5.5 PPC: ICB ---------------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "PPC_ICB_Breakdown",
  title = paste0(config$publication_table_title_cy, " - Number of issued prescription prepayment certificates, split by calendar year and ICB"),
  notes = c(config$caveat_cert_issued, config$caveat_icb_method, config$caveat_icb_restriction, config$caveat_icb_missing_data, config$caveat_ppc_base_population),
  dataset = ppc_icb_objs_cy$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "PPC_ICB_Breakdown", c("A","B","C","D","F"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "PPC_ICB_Breakdown", c("E","G","H","I","J","K"), "right", "#,###")


# 6.6 Data Tables: HRT PPC certificate -----------------------------------------

# 6.6.1 HRT PPC: Applications --------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "HRTPPC_Applications",
  title = paste0(config$publication_table_title_cy, " - Number of applications for NHS Hormone Replacement Therapy Prescription Prepayment Certificate (HRT PPC) split by month"),
  notes = c(config$caveat_hrtppc_start, config$caveat_hrtppc_country),
  dataset = hrt_application_objs_cy$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "HRTPPC_Applications", c("A","B"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "HRTPPC_Applications", c("C"), "right", "#,###")

# 6.6.2 HRT PPC: Issued --------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "HRTPPC_Issued",
  title = paste0(config$publication_table_title_cy, " - Number of NHS Hormone Replacement Therapy Prescription Prepayment Certificate (HRT PPC) issued, split by month"),
  notes = c(config$caveat_hrtppc_start, config$caveat_cert_issued, config$caveat_hrtppc_postdate, config$caveat_hrtppc_country),
  dataset = hrt_issued_objs_cy$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "HRTPPC_Issued", c("A","B"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "HRTPPC_Issued", c("C","D"), "right", "#,###")
# Proportion: right align * thousand seperator to 1 decimal place
accessibleTables::format_data(wb, "HRTPPC_Issued", c("E"), "right", "#,###.0")

# 6.6.3 HRT PPC: Age -----------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "HRTPPC_Age_Breakdown",
  title = paste0(config$publication_table_title_cy, " - Number of issued NHS Hormone Replacement Therapy Prescription Prepayment Certificate (HRT PPC), split by calendar year and age of applicant"),
  notes = c(config$caveat_hrtppc_start, config$caveat_cert_issued, config$caveat_hrtppc_age_group, config$caveat_hrtppc_px_data, config$caveat_hrtppc_country),
  dataset = hrt_age_objs_cy$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "HRTPPC_Age_Breakdown", c("A","B","C"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "HRTPPC_Age_Breakdown", c("D","E"), "right", "#,###")

# 6.6.4 HRT PPC: Deprivation ---------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "HRTPPC_Deprivation_Breakdown",
  title = paste0(config$publication_table_title_cy, " - Number of issued NHS Hormone Replacement Therapy Prescription Prepayment Certificate (HRT PPC), split by calendar year and IMD quintile"),
  notes = c(config$caveat_hrtppc_start, config$caveat_cert_issued, config$caveat_imd_restriction, config$caveat_hrtppc_px_data, config$caveat_hrtppc_country),
  dataset = hrt_imd_objs_cy$support_data |> dplyr::select(-Country),
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "HRTPPC_Deprivation_Breakdown", c("A","B","C"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "HRTPPC_Deprivation_Breakdown", c("D","E"), "right", "#,###")

# 6.6.5 HRT PPC: ICB -----------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "HRTPPC_ICB_Breakdown",
  title = paste0(config$publication_table_title_cy, " - Number of issued NHS Hormone Replacement Therapy Prescription Prepayment Certificate (HRT PPC), split by calendar year and ICB"),
  notes = c(config$caveat_hrtppc_start, config$caveat_cert_issued, config$caveat_icb_method, config$caveat_icb_restriction, config$caveat_icb_missing_data, config$caveat_hrtppc_base_population),
  dataset = hrt_icb_objs_cy$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "HRTPPC_ICB_Breakdown", c("A","B","C","D","F"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "HRTPPC_ICB_Breakdown", c("E","G","H"), "right", "#,###")

# 6.7 Data Tables: NHS Tax Credit Exemption Certificates -----------------------

# 6.7.1 TAX: Issued ------------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "TAX_Issued",
  title = paste0(config$publication_table_title_cy, " - Number of NHS Tax Credit Exemption Certificates issued, split by calendar year and country"),
  notes = c(config$caveat_tax_issued, config$caveat_country_other, config$caveat_country_unknown),
  dataset = tax_issued_objs_cy$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "TAX_Issued", c("A","B","C"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "TAX_Issued", c("D"), "right", "#,###")

# 6.7.2 TAX: Age ---------------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "TAX_Age_Breakdown",
  title = paste0(config$publication_table_title_cy, " - Number of issued NHS Tax Credit Exemption Certificates, split by calendar year, country and age of applicant"),
  notes = c(config$caveat_tax_age_band, config$caveat_age_restriction, config$caveat_country_other, config$caveat_country_unknown),
  dataset = tax_age_objs_cy$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "TAX_Age_Breakdown", c("A","B","C","D"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "TAX_Age_Breakdown", c("E"), "right", "#,###")

# 6.7.4 TAX: Deprivation -------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "TAX_Deprivation_Breakdown",
  title = paste0(config$publication_table_title_cy, " - Number of issued NHS Tax Credit Exemption Certificates, split by calendar year, country and IMD quintile"),
  notes = c(config$caveat_imd_restriction, config$caveat_country_other, config$caveat_country_unknown),
  dataset = tax_imd_objs_cy$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "TAX_Deprivation_Breakdown", c("A","B","C","D"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "TAX_Deprivation_Breakdown", c("E"), "right", "#,###")

# 6.7.5 TAX: ICB ---------------------------------------------------------------

# Create the sheet
accessibleTables::write_sheet(
  workbook = wb,
  sheetname = "TAX_ICB_Breakdown",
  title = paste0(config$publication_table_title_cy, " - Number of issued NHS Tax Credit Exemption Certificates, split by calendar year and ICB"),
  notes = c(config$caveat_icb_method, config$caveat_icb_restriction, config$caveat_icb_missing_data, config$caveat_tax_base_population),
  dataset = tax_icb_objs_cy$support_data,
  column_a_width = 30
)
# Apply formatting
# Text: left align
accessibleTables::format_data(wb, "TAX_ICB_Breakdown", c("A","B","C","D","E","G"), "left", "")
# Values: right align * thousand seperator
accessibleTables::format_data(wb, "TAX_ICB_Breakdown", c("F","H","I"), "right", "#,###")

# 6.8 Cover Sheet --------------------------------------------------------------

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
                       "outputs/hwhc_tables_cy.xlsx",
                       overwrite = TRUE)

# 7. Render outputs ------------------------------------------------------------

#render narrative markdown as html for final output

rmarkdown::render(
  "hwhc-markdown.Rmd",
  output_format = "html_document",
  output_file = "hwhc_markdown.html"
)

#render narrative markdown as docx for quality review comments
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

#render background and methodology markdown as html for final output
rmarkdown::render(
  "hwhc-methodology.Rmd",
  output_format = "html_document",
  output_file = "hwhc_background_methodology.html"
)

#render background and methodology markdown as docx for quality review comments
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

# close the log
logr::log_close()
