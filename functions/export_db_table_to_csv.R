#' export_db_table_to_csv
#'
#' Export a database table to csv
#' Table to export and filename will be supplied as paramaters
#'
#' @param db_connection active database connection
#' @param db_table_name table name for database table
#' @param output_path path and filename for output file
#'
export_db_table_to_csv <- function(db_connection, db_table_name, output_path) {
  
  # export to csv
  dplyr::tbl(
    db_connection, 
    from = dbplyr::in_schema(toupper(con@info$username), db_table_name)
  ) |> 
    dplyr::collect() |> 
    write.csv(
      file = output_path,
      row.names = FALSE
    )
  
  # show update message
  print(paste0(Sys.time(),": Exported table ", db_table_name, " to csv file ", output_path))
  
}
