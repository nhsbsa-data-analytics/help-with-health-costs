#' create_hes_icb_objects_cy
#'
#' Create objects to summarise the HES data by ICB geography
#' Output is data for supplementary datasets
#' TO DO: rewrite main create_hes_icb_objects function to allow financial year or calendar year
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
#' @param subtype_split (TRUE/FALSE) Boolean parameter to define is certificate subtypes should be included
#' @param population_db_table database table containing base population level data
#'
create_hes_icb_objects_cy <- function(
    db_connection,
    db_table_name,
    service_area,
    min_ym,
    max_ym,
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
    aggCols = c("SERVICE_AREA_NAME", "ISSUE_CY","COUNTRY","ICB","ICB_CODE","ICB_NAME")
    pvtJoinCols = c("ISSUE_CY", "COUNTRY", "ICB")
  } else {
    # for other services there is no country split applicable
    aggCols = c("SERVICE_AREA_NAME", "ISSUE_CY","ICB","ICB_CODE","ICB_NAME")
    pvtJoinCols = c("ISSUE_CY", "ICB")
  }
  
  # extract underlying data and mutate to get number per population denominator
  # will be limited to only certificates issued to the applicant (excluding other outcome responses for services like LIS)
  # create a single dataset based on issued certificates combined to population data
  # all figures can be aggregated to country and ICB
  df_issue <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, aggCols)
  
  df <- df_issue |>
    dplyr::left_join(
      y = population_data,
      by = c("ISSUE_CY" = "CALENDAR_YEAR","ICB" = "GEO_CODE", "SERVICE_AREA_NAME" = "SERVICE_AREA_NAME")
    ) |> 
    dplyr::mutate(ISSUED_CERTS_PER_POP = ISSUED_CERTS / BASE_POPULATION * config$ons_pop_rate_denominator) |> 
    dplyr::mutate(
      ISSUED_CERTS_SF = signif(ISSUED_CERTS,3),
      BASE_POPULATION_SF = signif(BASE_POPULATION,3),
      ISSUED_CERTS_PER_POP_SF = round(ISSUED_CERTS_PER_POP,0)
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
    dplyr::arrange(ISSUE_CY, ICB_NAME) |> 
    dplyr::select(-ICB, -ISSUED_CERTS_SF, -BASE_POPULATION_SF, -ISSUED_CERTS_PER_POP, -SERVICE_AREA) |> 
    # apply custom naming that could change based on parameters
    dplyr::rename(
      "Base Population Classification" = POP_TYPE,
      "Base Population" = BASE_POPULATION,
      {{metric_text}} := ISSUED_CERTS_PER_POP_SF
    )
  
  # rename fields
  obj_suppData <- obj_suppData |> 
    rename_df_fields()
  
  # return output
  return(list("support_data" = obj_suppData))
  
}