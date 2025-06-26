#' create_hes_age_objects_cy
#'
#' Create objects to summarise the HES data by age band
#' Output is data for supplementary datasets
#' TO DO: rewrite main create_hes_age_objects function to allow financial year or calendar year
#' 
#' Data will be based on issued certificates using the get_hes_issue_data function to extract data
#' The base dataset uses a CUSTOM_AGE_BAND column to assign people to relevant age bands
#' 
#' Parameters supplied to function will define the time period for which data will be produced
#'
#' @param db_connection active database connection
#' @param db_table_name database table containing application level data
#' @param service_area service area to collate data for, which must be one of: MAT, MED, PPC, TAX, LIS, HRTPPC
#' @param min_ym first month for full dataset (format YYYYMM)
#' @param max_ym last month for full dataset analysis (format YYYYMM)
#' @param subtype_split (TRUE/FALSE) Boolean parameter to define is certificate subtypes should be included
#'
create_hes_age_objects_cy <- function(db_connection, db_table_name, service_area, min_ym, max_ym, subtype_split = FALSE){
  
  # identify which fields to aggregate and sort by
  # will define is the certificate subtype and country fields are required
  if(subtype_split == TRUE){
    chart_fields <- c('SERVICE_AREA_NAME', 'CERTIFICATE_SUBTYPE', 'ISSUE_CY', 'CUSTOM_AGE_BAND')
    sort_fields_chart <- c('CERTIFICATE_SUBTYPE', 'CUSTOM_AGE_BAND')
    tab_align <- "llr"
    if(service_area %in% c("LIS","TAX")){
      supp_fields <- c('SERVICE_AREA_NAME','CERTIFICATE_SUBTYPE', 'COUNTRY', 'ISSUE_CY', 'CUSTOM_AGE_BAND')
    } else {
      supp_fields <- chart_fields # country not required
    }
  } else {
    chart_fields <- c('SERVICE_AREA_NAME', 'ISSUE_CY', 'CUSTOM_AGE_BAND') # subtype not required
    sort_fields_chart <- c('CUSTOM_AGE_BAND')
    tab_align <- "lr"
    if(service_area %in% c("LIS","TAX")){
      supp_fields <- c('SERVICE_AREA_NAME', 'COUNTRY', 'ISSUE_CY', 'CUSTOM_AGE_BAND')
    } else {
      supp_fields <- chart_fields # country not required
    }
  }
  
  # collect the issued certificate data
  df_issue <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, chart_fields)
  
  # create the support datasets (including historic CYs)
  # for "England only" services use a n/a placeholder for country
  if(service_area %in% c("MAT","MED","PPC","HRTPPC")){
    obj_suppData <- df_issue |> 
      dplyr::mutate(COUNTRY = 'n/a') |> 
      dplyr::relocate(COUNTRY, .after = ISSUE_CY)
  } else {
    # data needs to be collated from database again to include the country aggregation
    obj_suppData <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, supp_fields)
  }
  obj_suppData <- obj_suppData |>
    dplyr::arrange_at(supp_fields) |> 
    rename_df_fields()
  
  # return output
  return(list("support_data" = obj_suppData))
  
}
