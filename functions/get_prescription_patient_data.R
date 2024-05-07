#' get_prescription_patient_data
#'
#' collect summary patient counts from NHS prescription summary data
#' This dataset covers all patients that could be identified from the NHS prescription dataset
#' additional parameters will identify which fields to group results by
#'
#' @param db_connection active database connection
#' @param db_table_name database table containing application level data
#' @param min_age minimum age to consider in aggregation (-1 to 150)
#' @param max_age maximum age to consider in aggregation (-1 to 150)
#' @param group_list list of fields to group results by
#'
get_prescription_patient_data <- function(
    db_connection, 
    db_table_name, 
    min_age, 
    max_age, 
    group_list
) {
  
  # Test Parameters ---------------------------------------------------------
  
  # Test Parameter: db_table
  # abort if supplied table does not exist
  if(DBI::dbExistsTable(conn = db_connection, name = DBI::Id(schema = toupper(db_connection@info$username), table = db_table_name)) == FALSE){
    stop(paste0("Invalid parameter (db_table_name) supplied to get_prescription_patient_data: ", db_table_name, " does not exist!"), call. = FALSE)
  }
  
  # parameter test: min_age  
  if(!(min_age %in% seq(-1,150))){
    stop("Invalid parameter (min_age) supplied to get_prescription_patient_data: must be integer between -1 and 150", call. = FALSE)
  }
  
  # parameter test: max_age  
  if(!(max_age %in% seq(-1,150))){
    stop("Invalid parameter (max_age) supplied to get_prescription_patient_data: must be integer between -1 and 150", call. = FALSE)
  }
  
  # Extract Data ------------------------------------------------------------
  
  # create connection to database table
  df_patients <- dplyr::tbl(
    con, 
    from = dbplyr::in_schema(toupper(con@info$username), db_table_name)
  ) |> 
    dplyr::filter(AGE >= min_age) |> 
    dplyr::filter(AGE <= max_age) |> 
    dplyr::group_by(across(all_of(group_list))) |> 
    dplyr::summarise(
      PATIENT_COUNT = sum(PATIENT_COUNT),
      HRT_PATIENT_COUNT = sum(HRT_PATIENT_COUNT),
      .groups = "keep"
    ) |> 
    dplyr::ungroup() |> 
    dplyr::collect()
  
  # return output
  return(df_patients)
  
}