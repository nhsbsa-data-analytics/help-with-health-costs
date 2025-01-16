#' create_hes_age_objects
#'
#' Create objects to summarise the HES data by age band
#' Output will include a column chart and table by custom age bands, supporting download data and data for supplementary datasets
#' 
#' Data will be based on issued certificates using the get_hes_issue_data function to extract data
#' The base dataset uses a CUSTOM_AGE_BAND column to assign people to relevant age bands
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
create_hes_age_objects <- function(db_connection, db_table_name, service_area, min_ym, max_ym, subtype_split = FALSE){
  
  # if certificate sub-types are required slightly different objects will be required to allow for this
  if(subtype_split == TRUE){
    
    # create the data object for the visualisations
    obj_chart_data <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('CERTIFICATE_SUBTYPE', 'CUSTOM_AGE_BAND')) |> 
      dplyr::filter(!CERTIFICATE_SUBTYPE %in% c('Not Available')) |> 
      dplyr::filter(CUSTOM_AGE_BAND != 'Not Available') |> 
      dplyr::arrange(CERTIFICATE_SUBTYPE, CUSTOM_AGE_BAND)
    
    # create the chart using a grouped chart object
    obj_chart <- obj_chart_data |> 
      dplyr::mutate(ISSUED_CERTS = signif(ISSUED_CERTS,3)) |> 
      nhsbsaVis::group_chart_hc(
        x = CUSTOM_AGE_BAND,
        y = ISSUED_CERTS,
        type = "column",
        group = "CERTIFICATE_SUBTYPE",
        xLab = "Age Band",
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
    obj_chData <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'ISSUE_FY', 'CERTIFICATE_SUBTYPE', 'CUSTOM_AGE_BAND')) |> 
      dplyr::arrange(CERTIFICATE_SUBTYPE, CUSTOM_AGE_BAND) |> 
      rename_df_fields()
    
    # for "England only" services use a n/a placeholder for country
    if(service_area %in% c("MAT","MED","PPC","HRTPPC")){
      obj_suppData <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'ISSUE_FY', 'CERTIFICATE_SUBTYPE', 'CUSTOM_AGE_BAND')) |> 
        dplyr::mutate(COUNTRY = 'n/a') |> 
        dplyr::relocate(COUNTRY, .after = ISSUE_FY) |> 
        dplyr::arrange(CERTIFICATE_SUBTYPE, CUSTOM_AGE_BAND) |> 
        rename_df_fields()
        
    } else {
      obj_suppData <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'ISSUE_FY', 'COUNTRY', 'CERTIFICATE_SUBTYPE', 'CUSTOM_AGE_BAND')) |> 
        dplyr::arrange(COUNTRY, CERTIFICATE_SUBTYPE, CUSTOM_AGE_BAND) |> 
        rename_df_fields()
    }
    
  } else {
    
    # create the data object for the visualisations
    obj_chart_data <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('CUSTOM_AGE_BAND')) |>
      dplyr::filter(CUSTOM_AGE_BAND != 'Not Available') |> 
      dplyr::arrange(CUSTOM_AGE_BAND)
    
    # create the chart using a basic single column chart
    obj_chart <- obj_chart_data |> 
      dplyr::mutate(ISSUED_CERTS = signif(ISSUED_CERTS,3)) |> 
      nhsbsaVis::basic_chart_hc(
        x = CUSTOM_AGE_BAND,
        y = ISSUED_CERTS,
        type = "column",
        xLab = "Age Band",
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
    obj_chData <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'ISSUE_FY', 'CUSTOM_AGE_BAND')) |> 
      dplyr::arrange(CUSTOM_AGE_BAND) |> 
      rename_df_fields()
    
    # for "England only" services use a n/a placeholder for country
    if(service_area %in% c("MAT","MED","PPC","HRTPPC")){
      obj_suppData <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'ISSUE_FY', 'CUSTOM_AGE_BAND')) |> 
        dplyr::mutate(COUNTRY = 'n/a') |> 
        dplyr::relocate(COUNTRY, .after = ISSUE_FY) |> 
        dplyr::arrange(CUSTOM_AGE_BAND) |> 
        rename_df_fields()
    } else {
      obj_suppData <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'ISSUE_FY', 'COUNTRY', 'CUSTOM_AGE_BAND')) |> 
        dplyr::arrange(COUNTRY, CUSTOM_AGE_BAND) |> 
        rename_df_fields()
    }
  }
  
  # add y-axis to chart
  obj_chart <- obj_chart |> 
    highcharter::hc_yAxis(labels = list(enabled = TRUE))
  
  # return output
  return(list("chart" = obj_chart, "table" = obj_table, "chart_data" = obj_chData, "support_data" = obj_suppData))
  
}
