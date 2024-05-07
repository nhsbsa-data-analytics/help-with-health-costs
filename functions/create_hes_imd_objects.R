#' create_hes_imd_objects
#'
#' Create objects to summarise the HES data by IMD profile
#' Output will include a column chart by IMD quintile, supporting download data and data for supplementary datasets
#' 
#' Data will be based on issued certificates using the get_hes_issue_data function to extract data
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
create_hes_imd_objects <- function(db_connection, db_table_name, service_area, min_ym, max_ym, subtype_split = FALSE){
  
  # if certificate sub-types are required slightly different objects will be required to allow for this
  if(subtype_split == TRUE){
    
    # create the chart object
    obj_chart <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('CERTIFICATE_SUBTYPE', 'IMD_QUINTILE')) |>
      dplyr::filter(!CERTIFICATE_SUBTYPE %in% c('N/A','No certificate issued')) |> 
      dplyr::filter(!is.na(IMD_QUINTILE)) |> 
      dplyr::arrange(IMD_QUINTILE) |> 
      dplyr::mutate(ISSUED_CERTS_SF = signif(ISSUED_CERTS,3)) |> 
      nhsbsaVis::group_chart_hc(
        x = IMD_QUINTILE,
        y = ISSUED_CERTS_SF,
        type = "column",
        group = "CERTIFICATE_SUBTYPE",
        xLab = "IMD Quintile (1 = most deprived)",
        yLab = "Number of certificates issued",
        title = "",
        dlOn = FALSE
      ) |> 
      highcharter::hc_tooltip(
        enabled = T,
        shared = T,
        sort = T
      )
    
    # create the support datasets
    obj_chData <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'ISSUE_FY', 'CERTIFICATE_SUBTYPE', 'IMD_QUINTILE')) |>
      dplyr::arrange(ISSUE_FY, CERTIFICATE_SUBTYPE, IMD_QUINTILE) |> 
      rename_df_fields()
    
    # for "England only" services use a n/a placeholder for country
    if(service_area %in% c("MAT","MED","PPC","HRTPPC")){
      obj_suppData <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'ISSUE_FY', 'CERTIFICATE_SUBTYPE', 'IMD_QUINTILE')) |> 
        dplyr::mutate(COUNTRY = 'n/a') |> 
        dplyr::relocate(COUNTRY, .after = ISSUE_FY)
    } else {
      obj_suppData <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'ISSUE_FY', 'COUNTRY', 'CERTIFICATE_SUBTYPE', 'IMD_QUINTILE'))
    }
    
    obj_suppData <- obj_suppData |> 
      dplyr::arrange(ISSUE_FY, COUNTRY, CERTIFICATE_SUBTYPE, IMD_QUINTILE) |>
      rename_df_fields()
    
    
  } else {
    
    # create the chart object
    obj_chart <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('IMD_QUINTILE')) |>
      dplyr::arrange(IMD_QUINTILE) |> 
      dplyr::filter(!is.na(IMD_QUINTILE)) |> 
      dplyr::mutate(ISSUED_CERTS_SF = signif(ISSUED_CERTS,3)) |> 
      nhsbsaVis::basic_chart_hc(
        x = IMD_QUINTILE,
        y = ISSUED_CERTS_SF,
        type = "column",
        xLab = "IMD Quintile (1 = most deprived)",
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
    
    # create the support datasets
    obj_chData <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'ISSUE_FY', 'IMD_QUINTILE')) |>
      dplyr::arrange(ISSUE_FY, IMD_QUINTILE) |> 
      rename_df_fields()
    
    # for "England only" services use a n/a placeholder for country
    if(service_area %in% c("MAT","MED","PPC","HRTPPC")){
      obj_suppData <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'ISSUE_FY', 'IMD_QUINTILE')) |> 
        dplyr::mutate(COUNTRY = 'n/a') |> 
        dplyr::relocate(COUNTRY, .after = ISSUE_FY)
    } else {
      obj_suppData <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c('SERVICE_AREA_NAME', 'ISSUE_FY', 'COUNTRY', 'IMD_QUINTILE'))
    }
    
    obj_suppData <- obj_suppData |> 
      dplyr::arrange(ISSUE_FY, COUNTRY, IMD_QUINTILE) |>
      rename_df_fields()
  }
  
  # add y-axis to chart
  obj_chart <- obj_chart |> 
    highcharter::hc_yAxis(labels = list(enabled = TRUE))

  # return output
  return(list("chart" = obj_chart, "chart_data" = obj_chData, "support_data" = obj_suppData))
  
}
