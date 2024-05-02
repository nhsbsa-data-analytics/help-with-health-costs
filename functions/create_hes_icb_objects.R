#' create_hes_icb_objects
#'
#' Create objects to summarise the HES data by ICB geography
#' Output will include a column chart by custom age bands, supporting download data and data for supplementary datasets
#' As ICB areas can vary substantially in size, a population denominator will be used to standardise results for reporting
#' 
#' Data will be based on issued certificates using the get_hes_issue_data function to extract data
#' The base dataset uses the following columns to assign people to geographic areas: ICB, ICB_NAME
#' 
#' The icb_population_data function will be used to extract population estimates published by ONS
#' Population figures will be based on appropriate age ranges depending on the service area
#' 
#' Parameters supplied to function will define the time period for which data will be produced
#'
#' @param db_connection active database connection
#' @param db_table_name database table containing application level data
#' @param service_area service area to collate data for, which must be one of: MAT, MED, PPC, TAX, LIS
#' @param min_ym first month for analysis (format YYYYMM)
#' @param max_ym last month for analysis (format YYYYMM)
#' @param subtype_split (TRUE/FALSE) Boolean parameter to define is certificate subtypes should be included
#' @param population_year population year based on mid-year estimate (2021, 2022)
#' @param population_geo geography level to aggregate population data by (SICBL, ICB, NHSREG)
#' @param population_min_age minimum age to consider in population aggregation (0 to 90)
#' @param population_max_age maximum age to consider in population aggregation (0 to 90)
#' @param population_gender character to identify gender to include in population reporting (M = Male, F = Female, T = Total)
#'
create_hes_icb_objects <- function(
    db_connection,
    db_table_name,
    service_area,
    min_ym, max_ym,
    subtype_split = FALSE,
    population_year,
    population_geo,
    population_min_age,
    population_max_age,
    population_gender
){
  
  # create a custom label for the population based on supplied parameters
  population_text = paste0(
    "ONS population estimate (",
    population_year,
    switch(
      population_gender,
      "M" = " male",
      "F" = " female",
      "T" = ""
    ),
    " population aged ",
    population_min_age,
    ifelse(population_max_age==90,"+", paste0(" to ", population_max_age)),
    ")"
  )
  
  # create a custom label for metric figure
  metric_text = paste0("Number of issued certificates per ",config$ons_pop_rate_denominator," population")
  
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
  df <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, aggCols, TRUE) |> 
    dplyr::left_join(
      y = icb_population_data(
        year = population_year, 
        geo = population_geo, 
        min_age = population_min_age, 
        max_age = population_max_age,
        gender = population_gender
      ),
      by = "ICB"
    ) |> 
    dplyr::mutate(ISSUED_CERTS_PER_POP = ISSUED_CERTS / BASE_POPULATION * config$ons_pop_rate_denominator) |> 
    dplyr::mutate(
      ISSUED_CERTS_SF = signif(ISSUED_CERTS,3),
      BASE_POPULATION_SF = signif(BASE_POPULATION,3),
      ISSUED_CERTS_PER_POP_SF = round(ISSUED_CERTS_PER_POP,0)
    )
  
  # create chart object
  obj_chart <- df |>
    dplyr::filter(ICB != 'N/A') |> 
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
        metric_text,": {point.ISSUED_CERTS_PER_POP_SF} <br>",
        "Certificates issued: {point.ISSUED_CERTS_SF} <br>",
        population_text,": {point.BASE_POPULATION_SF}"
      )
    ) |> 
    highcharter::hc_xAxis(labels = list(enabled = FALSE)) |> 
    highcharter::hc_yAxis(labels = list(enabled = TRUE))

  # Map:
  obj_map <- basic_map_hc(
    geo_data = get_icb_map_boundaries(config$icb_classification),
    df = df,
    ons_code_field = "ICB",
    area_name_field = "ICB_NAME",
    value_field = "ISSUED_CERTS_PER_POP",
    metric_definition_string = metric_text,
    decimal_places = 0,
    value_prefix = "",
    order_of_magnitude = "",
    custom_tooltip = paste0(
      "<b>ICB:</b> {point.ICB_NAME}<br>",
      "<b>",metric_text,":</b> {point.ISSUED_CERTS_PER_POP_SF}<br>",
      "<b>Certificates issued:</b> {point.ISSUED_CERTS_SF}<br>",
      "<b>",population_text,":</b> {point.BASE_POPULATION_SF}"
    )
  )
  
  # create support data
  obj_chData <- df
  
  # remove wanted fields (ONS coding and rounded number fields)
  if(subtype_split == TRUE){
    # for split certificate types additional columns should show the breakdown by certificate subtype
    obj_chData <- df |>
      dplyr::left_join(
        y = get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, c(aggCols, "CERTIFICATE_SUBTYPE"), FALSE) |>
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
   
  # remove unwanted fields and rename fields
  obj_chData <- obj_chData |> 
    dplyr::arrange(ICB) |> 
    dplyr::select(-ICB, -ISSUED_CERTS_SF, -BASE_POPULATION_SF, -ISSUED_CERTS_PER_POP) |> 
    # apply custom naming that could change based on parameters
    dplyr::rename(
      {{population_text}} := BASE_POPULATION,
      {{metric_text}} := ISSUED_CERTS_PER_POP_SF
    ) |> 
    rename_df_fields()
    
  # return output
  return(list("chart" = obj_chart, "map" = obj_map, "chart_data" = obj_chData, "support_data" = obj_chData))

}