#' create_px_patient_age_objects_CY
#'
#' Create objects to summarise the NHS prescription age data
#' Output will include a column chart by custom age bands, supporting download data and data for supplementary datasets
#' 
#' Data will be based on patient counts using the get_prescription_patient_data function to extract data
#' The base dataset uses a CUSTOM_AGE_BAND column to assign people to relevant age bands linked to ages prescription charges apply
#' 
#' Parameters supplied to function will define which subset of patients to summarise
#'
#' @param db_connection active database connection
#' @param db_table_name database table containing application level data
#' @param patient_group PATIENT_COUNT or HRT_PATIENT_COUNT
#'
create_px_patient_age_objects_cy <- function(
    db_connection, 
    db_table_name,
    patient_group = 'PATIENT_COUNT'
){
  
  # Create objects ----------------------------------------------------------

  # create the dataset
  # request data for all ages (-1 to 150) as CUSTOM_AGE_BAND will apply 'Not Available' where required
  df <- get_prescription_patient_data(
    db_connection = db_connection, 
    db_table_name = db_table_name, 
    min_age = -1, 
    max_age = 150, 
    group_list = c("CALENDAR_YEAR", "CUSTOM_AGE_BAND")
  ) |> 
    dplyr::filter(CUSTOM_AGE_BAND != 'Not Available') |> 
    dplyr::arrange(CALENDAR_YEAR, CUSTOM_AGE_BAND) |> 
    dplyr::rename(BASE_POPULATION := {{ patient_group }}) |> 
    dplyr::select(CALENDAR_YEAR, CUSTOM_AGE_BAND, BASE_POPULATION)
  
  # rename the field names in dataset
  df <- rename_df_fields(df)
  
  # return output
  return(list("support_data" = df))
  
}
