#' create_hes_issued_month_objects_cy
#'
#' Create objects to summarise the HES issued outcomes data
#' Output is data for supplementary datasets
#' TO DO: rewrite main create_hes_issue_month_objects function to allow financial year or calendar year
#' 
#' Data will be based on outcomes issued to the customer, using the get_hes_issue_data function to extract data
#' 
#' Parameters supplied to function will define the time period for which data will be produced
#'
#' @param db_connection active database connection
#' @param db_table_name database table containing application level data
#' @param service_area service area to collate data for, which must be one of: MAT, MED, PPC, TAX, LIS, HRTPPC
#' @param min_ym first month for analysis (format YYYYMM)
#' @param max_ym last month for analysis (format YYYYMM)
#' @param subtype_split (TRUE/FALSE) Boolean parameter to define is certificate subtypes should be included
#'
create_hes_issued_month_objects_cy <- function(db_connection, db_table_name, service_area, min_ym, max_ym, subtype_split = FALSE){
  
  # if certificate sub-types are required slightly different objects will be required to allow for this
  if(subtype_split == TRUE){
    
    # create the data object for the visualisations
    obj_chart_data <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('CERTIFICATE_SUBTYPE', 'ISSUE_YM')) |> 
      dplyr::filter(CERTIFICATE_SUBTYPE != 'Not Available') |> 
      dplyr::arrange(CERTIFICATE_SUBTYPE, ISSUE_YM) |> 
      dplyr::mutate(ISSUE_YM = format(as.Date(paste0(ISSUE_YM,'01'), '%Y%m%d'), '%b-%y'))
    
    # for "England only" services use a n/a placeholder for country
    if(service_area %in% c("MAT","MED","PPC","HRTPPC")){
      obj_suppData <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'CERTIFICATE_SUBTYPE', 'ISSUE_YM')) |> 
        dplyr::mutate(COUNTRY = 'n/a') |> 
        dplyr::relocate(COUNTRY, .after = ISSUE_YM) |> 
        dplyr::arrange(CERTIFICATE_SUBTYPE, ISSUE_YM) |> 
        dplyr::mutate(ISSUE_YM = format(as.Date(paste0(ISSUE_YM,'01'), '%Y%m%d'), '%b-%y')) |> 
        rename_df_fields()
      
    } else {
      obj_suppData <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'CERTIFICATE_SUBTYPE', 'COUNTRY', 'ISSUE_YM')) |> 
        dplyr::arrange(CERTIFICATE_SUBTYPE, COUNTRY, ISSUE_YM) |> 
        dplyr::mutate(ISSUE_YM = format(as.Date(paste0(ISSUE_YM,'01'), '%Y%m%d'), '%b-%y')) |> 
        rename_df_fields()
    }
    
  } else {
    
    # for "England only" services use a n/a placeholder for country
    if(service_area %in% c("MAT","MED","PPC","HRTPPC")){
      obj_suppData <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'ISSUE_YM')) |> 
        dplyr::mutate(COUNTRY = 'n/a') |> 
        dplyr::relocate(COUNTRY, .after = ISSUE_YM) |> 
        dplyr::arrange(ISSUE_YM) |> 
        dplyr::mutate(ISSUE_YM = format(as.Date(paste0(ISSUE_YM,'01'), '%Y%m%d'), '%b-%y')) |> 
        rename_df_fields()
    } else {
      obj_suppData <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'COUNTRY', 'ISSUE_YM')) |> 
        dplyr::arrange(COUNTRY, ISSUE_YM) |> 
        dplyr::mutate(ISSUE_YM = format(as.Date(paste0(ISSUE_YM,'01'), '%Y%m%d'), '%b-%y')) |> 
        rename_df_fields()
    }
  }
  
  
  # apply custom formatting to LIS to change wording to outcome decisions
  if(service_area == 'LIS'){
    
    obj_suppData <- obj_suppData |> 
      dplyr::rename(`Number of outcome decisions` = `Number of certificates issued`)
  }
  
  # return output
  return(list("support_data" = obj_suppData))
  
}
