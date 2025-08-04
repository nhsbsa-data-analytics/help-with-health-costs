#' create_hes_application_month_objects_cy
#'
#' Create objects to summarise the HES application data
#' Output is data for supplementary datasets
#' TO DO: rewrite main create_hes_application_month_objects function to allow financial year or calendar year
#' 
#' Data will be based on all applications to a service using the get_hes_application_data function to extract data
#' 
#' Parameters supplied to function will define the time period for which data will be produced
#'
#' @param db_connection active database connection
#' @param db_table_name database table containing application level data
#' @param service_area service area to collate data for, which must be one of: MAT, MED, PPC, TAX, HRTPPC
#' @param min_ym first month for analysis (format YYYYMM)
#' @param max_ym last month for analysis (format YYYYMM)
#' @param subtype_split (TRUE/FALSE) Boolean parameter to define is certificate subtypes should be included
#'
create_hes_application_month_objects_cy <- function(db_connection, db_table_name, service_area, min_ym, max_ym, subtype_split = FALSE){
  
  # if certificate sub-types are required slightly different objects will be required to allow for this
  if(subtype_split == TRUE){
    
    # for "England only" services use a n/a placeholder for country
    if(service_area %in% c("MAT","MED","PPC","HRTPPC")){
      obj_suppData <- get_hes_application_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'CERTIFICATE_SUBTYPE', 'APPLICATION_YM')) |> 
        dplyr::mutate(COUNTRY = 'n/a') |> 
        dplyr::relocate(COUNTRY, .after = APPLICATION_YM) |> 
        dplyr::arrange(CERTIFICATE_SUBTYPE, APPLICATION_YM) |> 
        dplyr::mutate(APPLICATION_YM = format(as.Date(paste0(APPLICATION_YM,'01'), '%Y%m%d'), '%b-%y')) |> 
        rename_df_fields()
      
    } else {
      obj_suppData <- get_hes_application_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'CERTIFICATE_SUBTYPE', 'COUNTRY', 'APPLICATION_YM')) |> 
        dplyr::arrange(CERTIFICATE_SUBTYPE, COUNTRY, APPLICATION_YM) |> 
        dplyr::mutate(APPLICATION_YM = format(as.Date(paste0(APPLICATION_YM,'01'), '%Y%m%d'), '%b-%y')) |> 
        rename_df_fields()
    }
    
  } else {
    
    # for "England only" services use a n/a placeholder for country
    if(service_area %in% c("MAT","MED","PPC","HRTPPC")){
      obj_suppData <- get_hes_application_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'APPLICATION_YM')) |> 
        dplyr::mutate(COUNTRY = 'n/a') |> 
        dplyr::relocate(COUNTRY, .after = APPLICATION_YM) |> 
        dplyr::arrange(APPLICATION_YM) |> 
        dplyr::mutate(APPLICATION_YM = format(as.Date(paste0(APPLICATION_YM,'01'), '%Y%m%d'), '%b-%y')) |> 
        rename_df_fields()
    } else {
      obj_suppData <- get_hes_application_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'APPLICATION_YM', 'COUNTRY')) |> 
        dplyr::arrange(COUNTRY, APPLICATION_YM) |> 
        dplyr::mutate(APPLICATION_YM = format(as.Date(paste0(APPLICATION_YM,'01'), '%Y%m%d'), '%b-%y')) |> 
        rename_df_fields()
    }
  }
  
  # return output
  return(list("support_data" = obj_suppData))
  
}
