#' create_dataset_from_sql
#'
#' Create a database table based on code in a SQL script
#' Path to SQL script will be passed as a parameter
#' Existing versions of the table will be dropped, or renamed if a backup is required
#'
#' @param db_connection active database connection
#' @param path_to_sql_file path to stored SQL script
#' @param db_table_name table name for created table (existing versions will be dropped)
#' @param ls_variables (default null) list containing details of variables to replace in code, should contain two fields var and val
#'
create_dataset_from_sql <- function(db_connection, path_to_sql_file, db_table_name, ls_variables = NULL) {
  
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
  
  # replace variables based on supplied details
  num_var = length(ls_variables)
  if(num_var > 0){
    for(v in 1:num_var){
      sql_script <- sql_script |>
        gsub(pattern = paste0("&&",ls_variables$var[v]), replacement = ls_variables$val[v])
    }
  }
  
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
