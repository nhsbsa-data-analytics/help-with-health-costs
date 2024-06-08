#' create_hes_duration_objects
#'
#' Create objects to summarise the HES issued outcomes data split by certificate duration for a specific year
#' Output will include a column chart and table split by duration, supporting download data and data for supplementary datasets
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
create_hes_duration_objects <- function(db_connection, db_table_name, service_area, min_ym, max_ym, subtype_split = FALSE){
  
  # if certificate sub-types are required slightly different objects will be required to allow for this
  if(subtype_split == TRUE){
    
    # create the data object for the visualisations
    obj_chart_data <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('CERTIFICATE_SUBTYPE', 'CERTIFICATE_DURATION')) |> 
      dplyr::filter(CERTIFICATE_SUBTYPE != 'Not Available') |> 
      dplyr::filter(CERTIFICATE_DURATION != 'Not Available') |> 
      dplyr::arrange(CERTIFICATE_SUBTYPE, CERTIFICATE_DURATION)
    
    # create the chart using a grouped chart object
    obj_chart <- obj_chart_data |> 
      dplyr::mutate(ISSUED_CERTS = signif(ISSUED_CERTS,3)) |> 
      nhsbsaVis::group_chart_hc(
        x = CERTIFICATE_DURATION,
        y = ISSUED_CERTS,
        type = "column",
        group = "CERTIFICATE_SUBTYPE",
        xLab = "Certificate Duration",
        yLab = "Number of certificates issued",
        title = "",
        dlOn = FALSE
      ) |> 
      highcharter::hc_tooltip(
        enabled = T,
        shared = T,
        sort = T
      )
    
    # create the table object
    obj_table <- obj_chart_data |> 
      rename_df_fields() |> 
      knitr::kable(
        align = "llr",
        format.args = list(big.mark = ",")
      )
    
    # create the support datasets
    obj_chData <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'ISSUE_FY', 'CERTIFICATE_SUBTYPE', 'CERTIFICATE_DURATION')) |> 
      dplyr::arrange(CERTIFICATE_SUBTYPE, CERTIFICATE_DURATION) |> 
      rename_df_fields()
    
    # for "England only" services use a n/a placeholder for country
    if(service_area %in% c("MAT","MED","PPC","HRTPPC")){
      obj_suppData <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'ISSUE_FY', 'CERTIFICATE_SUBTYPE', 'CERTIFICATE_DURATION')) |> 
        dplyr::mutate(COUNTRY = 'n/a') |> 
        dplyr::relocate(COUNTRY, .after = ISSUE_FY) |> 
        dplyr::arrange(CERTIFICATE_SUBTYPE, CERTIFICATE_DURATION) |> 
        rename_df_fields()
      
    } else {
      obj_suppData <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'ISSUE_FY', 'COUNTRY', 'CERTIFICATE_SUBTYPE', 'CERTIFICATE_DURATION')) |> 
        dplyr::arrange(COUNTRY, CERTIFICATE_SUBTYPE, CERTIFICATE_DURATION) |> 
        rename_df_fields()
    }
    
  } else {
    
    # create the data object for the visualisations
    obj_chart_data <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('CERTIFICATE_DURATION')) |>
      dplyr::filter(CERTIFICATE_DURATION != 'Not Available') |> 
      dplyr::arrange(CERTIFICATE_DURATION)
    
    # create the chart using a basic single column chart
    obj_chart <- obj_chart_data |> 
      dplyr::mutate(ISSUED_CERTS = signif(ISSUED_CERTS,3)) |> 
      nhsbsaVis::basic_chart_hc(
        x = CERTIFICATE_DURATION,
        y = ISSUED_CERTS,
        type = "column",
        xLab = "Certificate duration",
        yLab = "Number of certificates issued",
        seriesName = "Certificates issued",
        title = "",
        dlOn = FALSE
      ) |> 
      highcharter::hc_tooltip(
        enabled = T,
        shared = T,
        sort = T
      )
    
    # create the table object
    obj_table <- obj_chart_data |> 
      rename_df_fields() |> 
      knitr::kable(
        align = "lr",
        format.args = list(big.mark = ",")
      )
    
    # create the support datasets
    obj_chData <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'ISSUE_FY', 'CERTIFICATE_DURATION')) |> 
      dplyr::arrange(ISSUE_FY) |> 
      rename_df_fields()
    
    # for "England only" services use a n/a placeholder for country
    if(service_area %in% c("MAT","MED","PPC","HRTPPC")){
      obj_suppData <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'ISSUE_FY', 'CERTIFICATE_DURATION')) |> 
        dplyr::mutate(COUNTRY = 'n/a') |> 
        dplyr::relocate(COUNTRY, .after = ISSUE_FY) |> 
        dplyr::arrange(ISSUE_FY) |> 
        rename_df_fields()
    } else {
      obj_suppData <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'ISSUE_FY', 'COUNTRY', 'CERTIFICATE_DURATION')) |> 
        dplyr::arrange(COUNTRY, CERTIFICATE_DURATION) |> 
        rename_df_fields()
    }
  }
  
  # format chart object if application volumes are in millions
  if(max(obj_chData$`Number of certificates issued` > 1000000)){
    obj_chart <- obj_chart |> 
      highcharter::hc_yAxis(labels = list(formatter = htmlwidgets::JS( 
        "function() {
        return (this.value/1000000)+'m'; /* all labels to absolute values */
    }")))
  }
  
  # add y-axis to chart
  obj_chart <- obj_chart |> 
    highcharter::hc_yAxis(labels = list(enabled = TRUE))
  
  
  
  # return output
  return(list("chart" = obj_chart, "table" = obj_table, "chart_data" = obj_chData, "support_data" = obj_suppData))
  
}
