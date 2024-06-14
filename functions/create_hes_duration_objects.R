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
#' @param min_ym first month for full dataset (format YYYYMM)
#' @param max_ym last month for full dataset analysis (format YYYYMM)
#' @param focus_fy financial year for narrative analysis (format YYYY/YYYY)
#' @param subtype_split (TRUE/FALSE) Boolean parameter to define is certificate subtypes should be included
#'
create_hes_duration_objects <- function(db_connection, db_table_name, service_area, min_ym, max_ym, focus_fy, subtype_split = FALSE){
  
  # identify which fields to aggregate and sort by
  # will define is the certificate subtype and country fields are required
  if(subtype_split == TRUE){
    chart_fields <- c('SERVICE_AREA_NAME', 'ISSUE_FY', 'CERTIFICATE_SUBTYPE', 'CERTIFICATE_DURATION')
    sort_fields_chart <- c('CERTIFICATE_SUBTYPE', 'CERTIFICATE_DURATION')
    if(service_area %in% c("LIS","TAX")){
      supp_fields <- c('SERVICE_AREA_NAME', 'ISSUE_FY', 'COUNTRY','CERTIFICATE_SUBTYPE', 'CERTIFICATE_DURATION')
    } else {
      supp_fields <- chart_fields # country not required
    }
  } else {
    chart_fields <- c('SERVICE_AREA_NAME', 'ISSUE_FY', 'CERTIFICATE_DURATION') # subtype not required
    sort_fields_chart <- c('CERTIFICATE_DURATION')
    if(service_area %in% c("LIS","TAX")){
      supp_fields <- c('SERVICE_AREA_NAME', 'ISSUE_FY', 'COUNTRY', 'CERTIFICATE_DURATION')
    } else {
      supp_fields <- chart_fields # country not required
    }
  }
  
  # collect the issued certificate data
  df_issue <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, chart_fields)
  
  # create the data object for the visualisations
  obj_chart_data <- df_issue |> 
    dplyr::filter(ISSUE_FY == focus_fy) |> 
    dplyr::select(-SERVICE_AREA_NAME, -ISSUE_FY) |> 
    dplyr::filter(CERTIFICATE_DURATION != 'Not Available') |>
    dplyr::arrange_at(sort_fields_chart)
  
  if(subtype_split == TRUE){
    obj_chart_data <- obj_chart_data |> 
      dplyr::filter(CERTIFICATE_SUBTYPE != 'Not Available')
  }
  
  # if certificate sub-types are required different chart objects will be required to allow for grouping
  if(subtype_split == TRUE){
    
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
  } else {
    
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
  }
  
  # create the table object
  obj_table <- obj_chart_data |> 
    rename_df_fields() |> 
    knitr::kable(
      align = "llr",
      format.args = list(big.mark = ",")
    )
  
  # create the chart download datasets
  obj_chData <- df_issue |> 
    dplyr::filter(ISSUE_FY == focus_fy) |> 
    dplyr::arrange_at(sort_fields_chart) |> 
    rename_df_fields()
  
  # create the support datasets (including historic FYs)
  # for "England only" services use a n/a placeholder for country
  if(service_area %in% c("MAT","MED","PPC","HRTPPC")){
    obj_suppData <- df_issue |> 
      dplyr::mutate(COUNTRY = 'n/a') |> 
      dplyr::relocate(COUNTRY, .after = ISSUE_FY)
  } else {
    # data needs to be collated from database again to include the country aggregation
    obj_suppData <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, supp_fields)
  }
  obj_suppData <- obj_suppData |>
    dplyr::arrange_at(supp_fields) |> 
    # replace null durations with 'Not Available'
    dplyr::mutate(
      CERTIFICATE_DURATION = as.character(CERTIFICATE_DURATION),
      CERTIFICATE_DURATION = tidyr::replace_na(CERTIFICATE_DURATION, 'Not Available')
    ) |> 
    rename_df_fields()
   
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
