#' create_px_patient_age_objects
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
create_px_patient_age_objects <- function(
    db_connection, 
    db_table_name, 
    patient_group = 'PATIENT_COUNT'
){
  
  # Test Parameters ---------------------------------------------------------
  
  # Test Parameter: patient_group
  # abort if invalid parameter
  if(DBI::dbExistsTable(conn = db_connection, name = DBI::Id(schema = toupper(db_connection@info$username), table = db_table_name)) == FALSE){
    stop("Invalid parameter (patient_group) supplied to create_px_patient_age_objects: Must be either PATIENT_COUNT or HRT_PATIENT_COUNT", call. = FALSE)
  }
  

  # Create objects ----------------------------------------------------------

  # create the dataset
  # request data for all ages (-1 to 150) as CUSTOM_AGE_BAND will apply 'N/A' where required
  df <- get_prescription_patient_data(
    db_connection = db_connection, 
    db_table_name = db_table_name, 
    min_age = -1, 
    max_age = 150, 
    group_list = c("CUSTOM_AGE_BAND")
  ) |> 
    dplyr::filter(CUSTOM_AGE_BAND != 'N/A') |> 
    dplyr::arrange(CUSTOM_AGE_BAND) |> 
    dplyr::rename(BASE_POPULATION := {{ patient_group }}) |> 
    dplyr::select(CUSTOM_AGE_BAND, BASE_POPULATION)
  
  # create the chart object
  obj_chart <- df |> 
    dplyr::mutate(BASE_POPULATION_SF = signif(BASE_POPULATION,3)) |> 
    nhsbsaVis::basic_chart_hc(
      x = CUSTOM_AGE_BAND,
      y = BASE_POPULATION_SF,
      type = "column",
      xLab = "Age Band",
      yLab = "Esimated patient count",
      seriesName = "Esimated patient count",
      title = "",
      dlOn = FALSE
    ) |> 
    highcharter::hc_tooltip(
      enabled = T,
      shared = T,
      sort = T
    ) |> 
    highcharter::hc_yAxis(labels = list(enabled = TRUE))
  
  # rename the field names in dataset
  df <- rename_df_fields(df)
  
  # return output
  return(list("chart" = obj_chart, "chart_data" = df))
  
}
