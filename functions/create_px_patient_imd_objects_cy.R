#' create_px_patient_imd_objects_cy
#'
#' Create objects to summarise the NHS prescription age data
#' Output will include a column chart by IMD quintile, supporting download data and data for supplementary datasets
#' 
#' Data will be based on patient counts using the get_prescription_patient_data function to extract data
#' The base dataset uses a IMD_QUINTILE column to assign people to relevant deprivation quintiles
#' 
#' Parameters supplied to function will define which subset of patients to summarise
#'
#' @param db_connection active database connection
#' @param db_table_name database table containing application level data
#' @param min_age minimum age to consider in aggregation (0 to 90)
#' @param max_age maximum age to consider in aggregation (0 to 90)
#' @param patient_group PATIENT_COUNT or HRT_PATIENT_COUNT
#'
create_px_patient_imd_objects_cy <- function(
    db_connection, 
    db_table_name,
    min_age,
    max_age,
    patient_group = 'PATIENT_COUNT'
){
  
  # Test Parameters ---------------------------------------------------------
  
  # parameter test: min_age  
  if(!(min_age %in% seq(0,90))){
    stop("Invalid parameter (min_age) supplied to create_px_patient_imd_objects: must be integer between 0 and 90", call. = FALSE)
  }
  
  # parameter test: max_age  
  if(!(max_age %in% seq(0,90))){
    stop("Invalid parameter (max_age) supplied to create_px_patient_imd_objects: must be integer between 0 and 90", call. = FALSE)
  }
  
  # Create objects ----------------------------------------------------------
  
  # create the dataset
  df <- get_prescription_patient_data(
    db_connection = db_connection,
    db_table_name = db_table_name,
    min_age = min_age,
    max_age = max_age,
    group_list = c("CALENDAR_YEAR", "IMD_QUINTILE")
  ) |> 
    dplyr::filter(!is.na(IMD_QUINTILE)) |> 
    dplyr::arrange(IMD_QUINTILE) |> 
    dplyr::rename(BASE_POPULATION := {{ patient_group }}) |> 
    dplyr::select(CALENDAR_YEAR, IMD_QUINTILE, BASE_POPULATION)
  
  # rename the field names in dataset
  df <- rename_df_fields(df)
  
  # return output
  return(list("support_data" = df))
  
}
