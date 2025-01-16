#' create_hes_icb_objects
#'
#' Create objects to summarise the HES data by ICB geography
#' Output will include a map, column chart and table by ICB, supporting download data and data for supplementary datasets
#' As ICB areas can vary substantially in size, a population denominator will be used to standardise results for reporting
#' 
#' Data will be based on issued certificates using the get_hes_issue_data function to extract data
#' The base dataset uses the following columns to assign people to geographic areas: ICB, ICB_NAME
#' 
#' A baseline population figure will be used to standardise data and prevent area size impacting results.
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
#' @param population_db_table database table containing base population level data
#'
create_hes_icb_objects <- function(
    db_connection,
    db_table_name,
    service_area,
    min_ym,
    max_ym,
    focus_fy,
    subtype_split = FALSE,
    population_db_table
){
  

  # Parameter Tests: --------------------------------------------------------
  
  # Test Parameter: service_area
  # abort if invalid service_area has been supplied
  if(!toupper(service_area) %in% c('MAT', 'MED', 'PPC', 'TAX', 'LIS', 'HRTPPC')){
    stop("Invalid parameter (service_area) supplied to create_hes_icb_objects: Must be one of: MAT, MED, PPC, TAX, LIS, HRTPPC", call. = FALSE)
  } 

  # Test Parameter: min_ym
  # abort if not a valid YM
  if(is.na(as.Date(paste0(substr(min_ym,1,4),'-',substr(min_ym,5,6),'-01'), optional = TRUE) == TRUE)){
    stop("Invalid parameter (min_ym) supplied to create_hes_icb_objects: Must be valid year_month (YYYYMM)", call. = FALSE)
  }
  # abort if too early (pre April 2015) or later than current month
  if(min_ym < 201504 | min_ym > format(Sys.Date(),'%Y%m')){
    stop("Invalid parameter (min_ym) supplied to create_hes_icb_objects: Must be valid year_month (YYYYMM) between 201504 and current month", call. = FALSE)
  }
  
  # Test Parameter: max_ym
  # abort if not a valid YM
  if(is.na(as.Date(paste0(substr(max_ym,1,4),'-',substr(max_ym,5,6),'-01'), optional = TRUE) == TRUE)){
    stop("Invalid parameter (max_ym) supplied to create_hes_icb_objects: Must be valid year_month (YYYYMM)", call. = FALSE)
  }
  # abort if too early (pre April 2015) or later than current month
  if(max_ym < 201504 | max_ym > format(Sys.Date(),'%Y%m')){
    stop("Invalid parameter (max_ym) supplied to create_hes_icb_objects: Must be valid year_month (YYYYMM) between 201504 and current month", call. = FALSE)
  }
  

  # Calculate baseline population data --------------------------------------
  population_data <- dplyr::tbl(
    con, 
    from = dbplyr::in_schema(toupper(con@info$username), population_db_table)
  ) |> 
    # filter to service area and time periods
    dplyr::filter(SERVICE_AREA == toupper(service_area)) |> 
    dplyr::collect()
  
  
  # Create output objects ---------------------------------------------------

  # create a custom label for metric figure
  metric_text = paste0("Number of issued certificates per ", format(config$ons_pop_rate_denominator,big.mark=","), " population")
  
  # define data grouping based on service area
  if(service_area %in% c("LIS","TAX")){
    # for LIS and TAX other countries should be included in the breakdown
    aggCols = c("SERVICE_AREA_NAME", "ISSUE_FY","COUNTRY","ICB","ICB_CODE","ICB_NAME")
    pvtJoinCols = c("ISSUE_FY", "COUNTRY", "ICB")
  } else {
    # for other services there is no country split applicable
    aggCols = c("SERVICE_AREA_NAME", "ISSUE_FY","ICB","ICB_CODE","ICB_NAME")
    pvtJoinCols = c("ISSUE_FY", "ICB")
  }
  
  # create the map & chart data object
  # the map/chart will be limited to only certificates issued to the applicant (excluding other outcome responses for services like LIS)
  # create a single dataset based on issued certificates combined to population data
  # all figures can be aggregated to country and ICB
  df_issue <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, aggCols)
    
  df <- df_issue |>
    dplyr::left_join(
      y = population_data,
      by = c("ISSUE_FY" = "FINANCIAL_YEAR","ICB" = "GEO_CODE", "SERVICE_AREA_NAME" = "SERVICE_AREA_NAME")
    ) |> 
    dplyr::mutate(ISSUED_CERTS_PER_POP = ISSUED_CERTS / BASE_POPULATION * config$ons_pop_rate_denominator) |> 
    dplyr::mutate(
      ISSUED_CERTS_SF = signif(ISSUED_CERTS,3),
      BASE_POPULATION_SF = signif(BASE_POPULATION,3),
      ISSUED_CERTS_PER_POP_SF = round(ISSUED_CERTS_PER_POP,0)
    )
  
  # create chart object
  obj_chart <- df |>
    dplyr::filter(ISSUE_FY == focus_fy) |> 
    dplyr::filter(ICB != 'Not Available') |> 
    dplyr::arrange(desc(ISSUED_CERTS_PER_POP)) |> 
    nhsbsaVis::basic_chart_hc(
      x = ICB_NAME,
      y = ISSUED_CERTS_PER_POP,
      type = "column",
      xLab = "ICB",
      yLab = metric_text,
      title = "",
      dlOn = FALSE
    ) |>
    highcharter::hc_tooltip(
      enabled = T,
      shared = T,
      sort = T,
      pointFormat = paste0(
        metric_text,": {point.ISSUED_CERTS_PER_POP_SF:,.0f} <br>",
        "Certificates issued: {point.ISSUED_CERTS_SF:,.0f} <br>",
        "Base Population: {point.POP_TYPE}: {point.BASE_POPULATION_SF:,.0f}"
      )
    ) |> 
    highcharter::hc_xAxis(labels = list(enabled = FALSE)) |> 
    highcharter::hc_yAxis(labels = list(enabled = TRUE))

  # Map:
  obj_map <- basic_map_hc(
    geo_data = get_icb_map_boundaries(config$icb_classification),
    df = df |> dplyr::filter(ISSUE_FY == focus_fy),
    ons_code_field = "ICB",
    area_name_field = "ICB_NAME",
    value_field = "ISSUED_CERTS_PER_POP",
    metric_definition_string = metric_text,
    decimal_places = 0,
    value_prefix = "",
    order_of_magnitude = "",
    custom_tooltip = paste0(
      "<b>ICB:</b> {point.ICB_NAME}<br>",
      "<b>",metric_text,":</b> {point.ISSUED_CERTS_PER_POP_SF:,.0f}<br>",
      "<b>Certificates issued:</b> {point.ISSUED_CERTS_SF:,.0f}<br>",
      "<b>Base Population: {point.POP_TYPE}:</b> {point.BASE_POPULATION_SF:,.0f}"
    )
  )
  
  # build a column header for the population
  population_text <- paste0("Population:",unique(na.omit(df[df$ISSUE_FY == focus_fy,]$POP_TYPE)))
  
  # create the table object
  obj_table <- df |> 
    dplyr::filter(ISSUE_FY == focus_fy) |> 
    dplyr::filter(ICB != 'Not Available') |> 
    dplyr::select(ICB_NAME, ISSUED_CERTS_PER_POP_SF,ISSUED_CERTS,BASE_POPULATION) |> 
    dplyr::arrange(ICB_NAME) |> 
    # apply custom naming that could change based on parameters
    dplyr::rename(
      {{metric_text}} := ISSUED_CERTS_PER_POP_SF,
      {{population_text}} := BASE_POPULATION
    ) |> 
    rename_df_fields() |> 
    knitr::kable(
      align = "lrrr",
      format.args = list(big.mark = ",")
    )
  
  # create support data
  obj_suppData <- df
  
  if(subtype_split == TRUE){
    # for split certificate types additional columns should show the breakdown by certificate subtype
    obj_suppData <- df |>
      dplyr::left_join(
        y = get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c(aggCols, "CERTIFICATE_SUBTYPE")) |>
          dplyr::select(all_of(c(pvtJoinCols,"CERTIFICATE_SUBTYPE","ISSUED_CERTS"))) |> 
          dplyr::arrange(CERTIFICATE_SUBTYPE) |> 
          tidyr::pivot_wider(
            names_from = CERTIFICATE_SUBTYPE,
            names_prefix = "pvt_issue_",
            values_from = ISSUED_CERTS
          ),
        by = pvtJoinCols
      )
  }
  
  # remove unwanted fields
  obj_suppData <- obj_suppData |> 
    dplyr::arrange(ISSUE_FY, ICB_NAME) |> 
    dplyr::select(-ICB, -ISSUED_CERTS_SF, -BASE_POPULATION_SF, -ISSUED_CERTS_PER_POP, -SERVICE_AREA) |> 
    # apply custom naming that could change based on parameters
    dplyr::rename(
      "Base Population Classification" = POP_TYPE,
      "Base Population" = BASE_POPULATION,
      {{metric_text}} := ISSUED_CERTS_PER_POP_SF
    )
  
  # create subset for chart data
  obj_chData <- obj_suppData |> 
    dplyr::filter(ISSUE_FY == focus_fy) |> 
    rename_df_fields()
  
  # rename fields
  obj_suppData <- obj_suppData |> 
    rename_df_fields()
    
  # return output
  return(list("chart" = obj_chart, "map" = obj_map, "table" = obj_table, "chart_data" = obj_chData, "support_data" = obj_suppData))

}