#' create_base_lis_dataset
#'
#' Create the base dataset table for NHS Low Income Scheme data
#' Table will be used based on the following SQL script: ./sql/LIS_FACT.sql
#' Existing versions of the table will be dropped, or renamed if a backup is required
#' Parameters supplied to function will define the time period for which data will be produced
#'
#' @param dw_extract_date date to specify the extract data to collect data as of
#' @param archive_existing_table Boolean variable to identify if existing versions of the table should be archived (current data suffix appended)
#'
create_base_dataset_from_sql <- function(db_connection, path_to_sql_file, db_table_name, dw_extract_date) {
  
  # read SQl file from path
  sql_script <- readr::read_lines(path_to_sql_file)  |>  
    stringr::str_c(sep = " ", collapse = "\n")
  
  # file cleaning operations
  sql_script <- sql_script |>
    # remove all demarked /*  */ sql comments
    gsub(pattern = "/\\*.*?\\*/", replacement = " ") |>
    # remove all demarked -- comments
    gsub(pattern = "--[^\r\n]*", replacement = " ") |>
    # remove everything after the query-end semicolon
    gsub(pattern = ";.*", replacement = " ") |>
    # remove any line break, tab, etc.
    gsub(pattern = "[\r\n\t\f\v]", replacement = " ") |>
    # remove extra whitespace
    gsub(pattern = " +", replacement = " ")
  
  # replace extract date parameter in script
  sql_script <- sql_script |>
    gsub(pattern = "&&p_extract_date", replacement = dw_extract_date)
  
  # remove existing version of the table
  if(
    DBI::dbExistsTable(
      conn = db_connection,
      name = DBI::Id(schema = toupper(db_connection@info$username), table = db_table_name)
    ) == T
  ){
    DBI::dbRemoveTable(
      conn = db_connection,
      name = DBI::Id(schema = toupper(db_connection@info$username), table = db_table_name)
    )
  }
  
  # execute script to create database table
  DBI::dbExecute(
    conn = db_connection,
    statement = sql_script
  )
  
  # show update message
  print(paste0(Sys.time(),": Created database table ", db_table_name))
  
}
