#' create_matex_duration_objects
#'
#' Create objects to summarise the MATEX issued outcomes data split by certificate duration for a specific year
#' For MATEX the data is presented as a cumulative chart to emphasise the number of applicants receiving the certificate early in pregnancy
#' Output will include a cumulative line chart and table, supporting download data and data for supplementary datasets
#' 
#' Data will be based on certificates issued to the customer, using the get_matex_duration_data function to extract data
#' 
#' Parameters supplied to function will define the time period for which data will be produced
#'
#' @param db_connection active database connection
#' @param min_ym first month for analysis (format YYYYMM)
#' @param max_ym last month for analysis (format YYYYMM)
#'
create_matex_duration_objects <- function(db_connection, min_ym, max_ym){
  
  # create the data object for the visualisations
    obj_chart_data <- get_matex_duration_data(db_connection, 'HWHC_HES_FACT', min_ym, max_ym)
    
    # create the chart as a cumulutive line chart
    obj_chart <- obj_chart_data |> 
      nhsbsaVis::basic_chart_hc(
        x = CERTIFICATE_DURATION_MONTHS,
        y = PROP_CUM_SUM_ISSUED_CERTS,
        type = "line",
        xLab = "Duration of certificate (months)",
        yLab = "Proportion of certificates issued (%)",
        seriesName = "Proportion of certificates issued (%)",
        title = "",
        dlOn = FALSE
      ) |> 
      highcharter::hc_tooltip(
        enabled = T,
        shared = T,
        sort = T
      ) |> 
      highcharter::hc_yAxis(max = 100) |> 
      highcharter::hc_xAxis(plotLines = list(
        list(
          label = list(
            text = "Expected due date",
            verticalAlign = "bottom",
            textAlign = "left",
            rotation = 270,
            x = -5,
            y = -5
          ),
          color = "#000000",
          width = 2,
          value = 12
        )
      ),
      reversed = TRUE
      )
    
    # create the table object
    obj_table <- obj_chart_data |> 
      dplyr::filter(!is.na(CERTIFICATE_DURATION_MONTHS)) |> 
      dplyr::select(-SERVICE_AREA_NAME, -ISSUE_FY) |> 
      rename_df_fields() |> 
      knitr::kable(
        align = "rrrr",
        format.args = list(big.mark = ",")
      )
    
    # create the support datasets
    obj_chData <- obj_chart_data |> 
      dplyr::mutate(CERTIFICATE_DURATION_MONTHS = ifelse(is.na(CERTIFICATE_DURATION_MONTHS),"Not Available",CERTIFICATE_DURATION_MONTHS)) |> 
      rename_df_fields()
    
    # for "England only" services use a n/a placeholder for country
    obj_suppData <- obj_chart_data |> 
      dplyr::mutate(CERTIFICATE_DURATION_MONTHS = ifelse(is.na(CERTIFICATE_DURATION_MONTHS ),"Not Available",CERTIFICATE_DURATION_MONTHS)) |> 
      dplyr::mutate(COUNTRY = "N/A") |> 
      dplyr::relocate(COUNTRY, .after = ISSUE_FY) |> 
      rename_df_fields()
  
  # return output
  return(list("chart" = obj_chart, "table" = obj_table, "chart_data" = obj_chData, "support_data" = obj_suppData))
  
}
