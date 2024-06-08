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
#' The baseline population to use will be determined by the base_population_source parameter.
#' If "PX" is chosen, the db_px_patient_table and px_population_type parameters must be supplied to define which table and 
#' field to take population data from. This table will already be aggregated to relevant CUSTOM_AGE
#' If "ONS" is chose the ons_population_... parameters must be populated to be passed to the icb_population_data function. This will 
#' be used to extract population estimates published by ONS.
#' 
#' Parameters supplied to function will define the time period for which data will be produced
#'
#' @param db_connection active database connection
#' @param db_table_name database table containing application level data
#' @param service_area service area to collate data for, which must be one of: MAT, MED, PPC, TAX, LIS, HRTPPC
#' @param min_ym first month for analysis (format YYYYMM)
#' @param max_ym last month for analysis (format YYYYMM)
#' @param subtype_split (TRUE/FALSE) Boolean parameter to define is certificate subtypes should be included
#' @param base_population_source (PX / ONS) definition of source for baseline population figures
#' @param population_min_age minimum age to consider in population aggregation (0 to 90)
#' @param population_max_age maximum age to consider in population aggregation (0 to 90)
#' @param db_px_patient_table database table containing patient count data with data already aggregation to relevant groupings
#' @param px_population_type (PATIENT_COUNT / HRT_PATIENT_COUNT) 
#' @param ons_population_year population year based on mid-year estimate (2021, 2022)
#' @param ons_population_gender character to identify gender to include in population reporting (M = Male, F = Female, T = Total)
#'
create_hes_icb_objects <- function(
    db_connection,
    db_table_name,
    service_area,
    min_ym, 
    max_ym,
    subtype_split = FALSE,
    base_population_source,
    population_min_age,
    population_max_age,
    db_px_patient_table = NULL,
    px_population_type = NULL,
    ons_population_year = NULL,
    ons_population_gender = NULL
){
  

  # Parameter Tests: --------------------------------------------------------
  
  # Test Parameter: db_table
  # abort if supplied table does not exist
  if(DBI::dbExistsTable(conn = db_connection, name = DBI::Id(schema = toupper(db_connection@info$username), table = db_table_name)) == FALSE){
    stop(paste0("Invalid parameter (db_table_name) supplied to create_hes_icb_objects: ", db_table_name, " does not exist!"), call. = FALSE)
  }
  
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
  
  # Test Parameter: base_population_source
  # abort if invalid base_population_source has been supplied
  if(!toupper(base_population_source) %in% c('PX', 'ONS')){
    stop("Invalid parameter (base_population_source) supplied to create_hes_icb_objects: Must be one of: PX, ONS", call. = FALSE)
  } 
  # if PX is chosen check a valid table has been supplied
  if(toupper(base_population_source) == 'PX'){
    if(DBI::dbExistsTable(conn = db_connection, name = DBI::Id(schema = toupper(db_connection@info$username), table = db_table_name)) == FALSE){
      stop(paste0("Invalid parameter (db_px_patient_table) supplied to create_hes_icb_objects: ", db_px_patient_table, " does not exist!"), call. = FALSE)
    }
  }
  # if ONS is chosen check the additional parameters have been supplied
  if(toupper(base_population_source) == 'ONS'){
    if(is.null(ons_population_year)){stop(paste0("Parameter missing (create_hes_icb_objects): ons_population_year required for ONS baseline population"), call. = FALSE)}
    if(is.null(ons_population_gender)){stop(paste0("Parameter missing (create_hes_icb_objects): ons_population_gender required for ONS baseline population"), call. = FALSE)}
  }
  

  # Calculate baseline population data --------------------------------------
  if(base_population_source == 'PX'){
    
    # create the population data object
    population_data <- get_prescription_patient_data(
      db_connection = db_connection,
      db_table_name = db_px_patient_table,
      min_age = population_min_age,
      max_age = population_max_age,
      group_list = c("ICB")
    ) |> 
      dplyr::filter(ICB != 'Not Available') |> 
      dplyr::rename(BASE_POPULATION := {{ px_population_type }}) |> 
      dplyr::select(ICB, BASE_POPULATION)
    
    # create the population definition text object to be used as custom label
    population_text = paste0(
      "Population estimate (population aged ",
      population_min_age,
      ifelse(population_max_age==90,"+", paste0(" to ", population_max_age)),
      " with NHS prescribing",
      switch(
        px_population_type,
        "PATIENT_COUNT" = "",
        "HRT_PATIENT_COUNT" = " of HRT PPC qualifying medication"
      ),
      ")"
    )
  }
  
  if(base_population_source == 'ONS'){
    
    # create the population data object
    population_data <- icb_population_data(
      year = ons_population_year,
      min_age = population_min_age, 
      max_age = population_max_age,
      gender = ons_population_gender
    )
    
    # create the population definition text object to be used as custom label
    population_text = paste0(
      "ONS population estimate (",
      ons_population_year,
      switch(
        ons_population_gender,
        "M" = " male",
        "F" = " female",
        "T" = ""
      ),
      " population aged ",
      population_min_age,
      ifelse(population_max_age==90,"+", paste0(" to ", population_max_age)),
      ")"
    )
  }

  
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
  df <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, aggCols) |> 
    dplyr::left_join(
      y = population_data,
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
        population_text,": {point.BASE_POPULATION_SF:,.0f}"
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
      "<b>",metric_text,":</b> {point.ISSUED_CERTS_PER_POP_SF:,.0f}<br>",
      "<b>Certificates issued:</b> {point.ISSUED_CERTS_SF:,.0f}<br>",
      "<b>",population_text,":</b> {point.BASE_POPULATION_SF:,.0f}"
    )
  )
  
  # create the table object
  obj_table <- df |> 
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
  obj_chData <- df
  
  # remove wanted fields (ONS coding and rounded number fields)
  if(subtype_split == TRUE){
    # for split certificate types additional columns should show the breakdown by certificate subtype
    obj_chData <- df |>
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
  return(list("chart" = obj_chart, "map" = obj_map, "table" = obj_table, "chart_data" = obj_chData, "support_data" = obj_chData))

}