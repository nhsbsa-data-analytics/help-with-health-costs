#' create_hes_issued_month_objects
#'
#' Create objects to summarise the HES issued outcomes data
#' Output will include a line chart, a table, supporting download data and data for supplementary datasets
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
create_hes_issued_month_objects <- function(db_connection, db_table_name, service_area, min_ym, max_ym, subtype_split = FALSE){
  
  # if certificate sub-types are required slightly different objects will be required to allow for this
  if(subtype_split == TRUE){
    
    # create the data object for the visualisations
    obj_chart_data <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('CERTIFICATE_SUBTYPE', 'ISSUE_YM')) |> 
      dplyr::filter(CERTIFICATE_SUBTYPE != 'Not Available') |> 
      dplyr::arrange(ISSUE_YM, CERTIFICATE_SUBTYPE) |> 
      dplyr::mutate(ISSUE_YM = format(as.Date(paste0(ISSUE_YM,'01'), '%Y%m%d'), '%b-%y'))
    
    # create the chart using a grouped chart object
    obj_chart <- obj_chart_data |> 
      dplyr::mutate(ISSUED_CERTS = signif(ISSUED_CERTS,3)) |> 
      nhsbsaVis::group_chart_hc(
        x = ISSUE_YM,
        y = ISSUED_CERTS,
        type = "line",
        group = "CERTIFICATE_SUBTYPE",
        xLab = "Month",
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
    obj_chData <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'ISSUE_YM', 'CERTIFICATE_SUBTYPE')) |> 
      dplyr::arrange(ISSUE_YM, CERTIFICATE_SUBTYPE) |> 
      dplyr::mutate(ISSUE_YM = format(as.Date(paste0(ISSUE_YM,'01'), '%Y%m%d'), '%b-%y')) |> 
      rename_df_fields()
    
    # for "England only" services use a n/a placeholder for country
    if(service_area %in% c("MAT","MED","PPC","HRTPPC")){
      obj_suppData <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'ISSUE_YM', 'CERTIFICATE_SUBTYPE')) |> 
        dplyr::mutate(COUNTRY = 'n/a') |> 
        dplyr::relocate(COUNTRY, .after = ISSUE_YM) |> 
        dplyr::arrange(ISSUE_YM, CERTIFICATE_SUBTYPE) |> 
        dplyr::mutate(ISSUE_YM = format(as.Date(paste0(ISSUE_YM,'01'), '%Y%m%d'), '%b-%y')) |> 
        rename_df_fields()
      
    } else {
      obj_suppData <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'ISSUE_YM', 'COUNTRY', 'CERTIFICATE_SUBTYPE')) |> 
        dplyr::arrange(COUNTRY, ISSUE_YM, CERTIFICATE_SUBTYPE) |> 
        dplyr::mutate(ISSUE_YM = format(as.Date(paste0(ISSUE_YM,'01'), '%Y%m%d'), '%b-%y')) |> 
        rename_df_fields()
    }
    
  } else {
    
    # create the data object for the visualisations
    obj_chart_data <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('ISSUE_YM')) |>
      dplyr::arrange(ISSUE_YM) |> 
      dplyr::mutate(ISSUE_YM = format(as.Date(paste0(ISSUE_YM,'01'), '%Y%m%d'), '%b-%y'))
    
    # create the chart using a basic single column chart
    obj_chart <- obj_chart_data |> 
      dplyr::mutate(ISSUED_CERTS = signif(ISSUED_CERTS,3)) |> 
      nhsbsaVis::basic_chart_hc(
        x = ISSUE_YM,
        y = ISSUED_CERTS,
        type = "line",
        xLab = "Month",
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
    obj_chData <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'ISSUE_YM')) |> 
      dplyr::arrange(ISSUE_YM) |> 
      dplyr::mutate(ISSUE_YM = format(as.Date(paste0(ISSUE_YM,'01'), '%Y%m%d'), '%b-%y')) |> 
      rename_df_fields()
    
    # for "England only" services use a n/a placeholder for country
    if(service_area %in% c("MAT","MED","PPC","HRTPPC")){
      obj_suppData <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'ISSUE_YM')) |> 
        dplyr::mutate(COUNTRY = 'n/a') |> 
        dplyr::relocate(COUNTRY, .after = ISSUE_YM) |> 
        dplyr::arrange(ISSUE_YM) |> 
        dplyr::mutate(ISSUE_YM = format(as.Date(paste0(ISSUE_YM,'01'), '%Y%m%d'), '%b-%y')) |> 
        rename_df_fields()
    } else {
      obj_suppData <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'ISSUE_YM', 'COUNTRY')) |> 
        dplyr::arrange(COUNTRY, ISSUE_YM) |> 
        dplyr::mutate(ISSUE_YM = format(as.Date(paste0(ISSUE_YM,'01'), '%Y%m%d'), '%b-%y')) |> 
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
  
  # apply custom formatting to LIS to change wording to outcome decisions
  if(service_area == 'LIS'){
    
    # update chart
    obj_chart <- obj_chart |> 
      highcharter::hc_yAxis(title = list(text = "Number of outcome decisions"))
    
    # update datasets
    obj_chData <- obj_chData |> 
      dplyr::rename(`Number of outcome decisions` = `Number of certificates issued`)
    
    obj_suppData <- obj_suppData |> 
      dplyr::rename(`Number of outcome decisions` = `Number of certificates issued`)
  }
  
  # return output
  return(list("chart" = obj_chart, "table" = obj_table, "chart_data" = obj_chData, "support_data" = obj_suppData))
  
}
