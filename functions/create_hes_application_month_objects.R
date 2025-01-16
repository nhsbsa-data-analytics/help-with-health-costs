#' create_hes_application_month_objects
#'
#' Create objects to summarise the HES application data
#' Output will include a line chart, supporting download data and data for supplementary datasets
#' 
#' Data will be based on all applications to a service using  the get_hes_application_data function to extract data
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
create_hes_application_month_objects <- function(db_connection, db_table_name, service_area, min_ym, max_ym, subtype_split = FALSE){
  
  # if certificate sub-types are required slightly different objects will be required to allow for this
  if(subtype_split == TRUE){
    
    # create the chart using a grouped chart object
    obj_chart <- get_hes_application_data(con, db_table_name, service_area, min_ym, max_ym, c('CERTIFICATE_SUBTYPE', 'APPLICATION_YM')) |> 
      dplyr::filter(CERTIFICATE_SUBTYPE != 'N/A') |> 
      dplyr::arrange(APPLICATION_YM, CERTIFICATE_SUBTYPE) |> 
      dplyr::mutate(APPLICATION_YM = paste0(substr(APPLICATION_YM,1,4),'-',substr(APPLICATION_YM,5,6))) |> 
      dplyr::mutate(APPLICATIONS_SF = signif(APPLICATIONS,3)) |> 
      nhsbsaVis::group_chart_hc(
        x = APPLICATION_YM,
        y = APPLICATIONS_SF,
        type = "line",
        group = "CERTIFICATE_SUBTYPE",
        xLab = "Month",
        yLab = "Number of applications received",
        title = "",
        dlOn = FALSE
      ) |> 
      highcharter::hc_tooltip(
        enabled = T,
        shared = T,
        sort = T
      )
    
    # create the support datasets
    obj_chData <- get_hes_application_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'APPLICATION_YM', 'CERTIFICATE_SUBTYPE')) |> 
      dplyr::arrange(APPLICATION_YM, CERTIFICATE_SUBTYPE) |> 
      dplyr::mutate(APPLICATION_YM = paste0(substr(APPLICATION_YM,1,4),'-',substr(APPLICATION_YM,5,6))) |> 
      rename_df_fields()
    
    # for "England only" services use a n/a placeholder for country
    if(service_area %in% c("MAT","MED","PPC","HRTPPC")){
      obj_suppData <- get_hes_application_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'APPLICATION_YM', 'CERTIFICATE_SUBTYPE')) |> 
        dplyr::mutate(COUNTRY = 'n/a') |> 
        dplyr::relocate(COUNTRY, .after = APPLICATION_YM) |> 
        dplyr::arrange(APPLICATION_YM, CERTIFICATE_SUBTYPE) |> 
        dplyr::mutate(APPLICATION_YM = paste0(substr(APPLICATION_YM,1,4),'-',substr(APPLICATION_YM,5,6))) |> 
        rename_df_fields()
      
    } else {
      obj_suppData <- get_hes_application_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'APPLICATION_YM', 'COUNTRY', 'CERTIFICATE_SUBTYPE')) |> 
        dplyr::arrange(COUNTRY, APPLICATION_YM, CERTIFICATE_SUBTYPE) |> 
        dplyr::mutate(APPLICATION_YM = paste0(substr(APPLICATION_YM,1,4),'-',substr(APPLICATION_YM,5,6))) |> 
        rename_df_fields()
    }
    
  } else {
    
    # create the chart using a basic single column chart
    obj_chart <- get_hes_application_data(con, db_table_name, service_area, min_ym, max_ym, c('APPLICATION_YM')) |>
      dplyr::arrange(APPLICATION_YM) |> 
      dplyr::mutate(APPLICATION_YM = paste0(substr(APPLICATION_YM,1,4),'-',substr(APPLICATION_YM,5,6))) |> 
      dplyr::mutate(APPLICATIONS_SF = signif(APPLICATIONS,3)) |> 
      nhsbsaVis::basic_chart_hc(
        x = APPLICATION_YM,
        y = APPLICATIONS_SF,
        type = "line",
        xLab = "Month",
        yLab = "Number of applications received",
        seriesName = "Applications received",
        title = "",
        dlOn = FALSE
      ) |> 
      highcharter::hc_tooltip(
        enabled = T,
        shared = T,
        sort = T
      )
    
    # create the support datasets
    obj_chData <- get_hes_application_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'APPLICATION_YM')) |> 
      dplyr::arrange(APPLICATION_YM) |> 
      dplyr::mutate(APPLICATION_YM = paste0(substr(APPLICATION_YM,1,4),'-',substr(APPLICATION_YM,5,6))) |> 
      rename_df_fields()
    
    # for "England only" services use a n/a placeholder for country
    if(service_area %in% c("MAT","MED","PPC","HRTPPC")){
      obj_suppData <- get_hes_application_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'APPLICATION_YM')) |> 
        dplyr::mutate(COUNTRY = 'n/a') |> 
        dplyr::relocate(COUNTRY, .after = APPLICATION_YM) |> 
        dplyr::arrange(APPLICATION_YM) |> 
        dplyr::mutate(APPLICATION_YM = paste0(substr(APPLICATION_YM,1,4),'-',substr(APPLICATION_YM,5,6))) |> 
        rename_df_fields()
    } else {
      obj_suppData <- get_hes_application_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'APPLICATION_YM', 'COUNTRY')) |> 
        dplyr::arrange(COUNTRY, APPLICATION_YM) |> 
        dplyr::mutate(APPLICATION_YM = paste0(substr(APPLICATION_YM,1,4),'-',substr(APPLICATION_YM,5,6))) |> 
        rename_df_fields()
    }
  }
  
  # format chart object if application volumes are in millions
  if(max(obj_chData$`Number of applications received` > 1000000)){
    obj_chart <- obj_chart |> 
      highcharter::hc_yAxis(labels = list(formatter = htmlwidgets::JS( 
        "function() {
        return (this.value/1000000)+'m'; /* all labels to absolute values */
    }")))
  }
  
  # return output
  return(list("chart" = obj_chart, "chart_data" = obj_chData, "support_data" = obj_suppData))
  
}
