#' create_hes_duration_objects_cy
#'
#' Create objects to summarise the HES issued outcomes data split by certificate duration for a specific year
#' Output is data for supplementary datasets
#' TO DO: rewrite main create_hes_duration_objects function to allow financial year or calendar year
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
#' @param subtype_split (TRUE/FALSE) Boolean parameter to define is certificate subtypes should be included
#'
create_hes_duration_objects_cy <- function(db_connection, db_table_name, service_area, min_ym, max_ym, subtype_split = FALSE){
  
  # identify which fields to aggregate and sort by
  # will define is the certificate subtype and country fields are required
  if(subtype_split == TRUE){
    chart_fields <- c('SERVICE_AREA_NAME', 'CERTIFICATE_SUBTYPE', 'ISSUE_CY', 'CERTIFICATE_DURATION')
    sort_fields_chart <- c('CERTIFICATE_SUBTYPE', 'CERTIFICATE_DURATION')
    if(service_area %in% c("LIS","TAX")){
      supp_fields <- c('SERVICE_AREA_NAME', 'CERTIFICATE_SUBTYPE', 'COUNTRY', 'ISSUE_CY', 'CERTIFICATE_DURATION')
    } else {
      supp_fields <- chart_fields # country not required
    }
  } else {
    chart_fields <- c('SERVICE_AREA_NAME', 'ISSUE_CY', 'CERTIFICATE_DURATION') # subtype not required
    sort_fields_chart <- c('CERTIFICATE_DURATION')
    if(service_area %in% c("LIS","TAX")){
      supp_fields <- c('SERVICE_AREA_NAME', 'COUNTRY', 'ISSUE_CY', 'CERTIFICATE_DURATION')
    } else {
      supp_fields <- chart_fields # country not required
    }
  }
  
  # collect the issued certificate data
  df_issue <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, chart_fields)
  
  # reclassify MATEX duration
  if(service_area == 'MAT'){
    df_issue <- df_issue |> 
      mutate(CERTIFICATE_DURATION = dplyr::case_when(
        CERTIFICATE_DURATION < 12 ~ '11 months or less',
        CERTIFICATE_DURATION >= 20 ~ '20 to 22 months',
        CERTIFICATE_DURATION >= 18 ~ '18 to 19 months',
        CERTIFICATE_DURATION >= 16 ~ '16 to 17 months',
        CERTIFICATE_DURATION >= 14 ~ '14 to 15 months',
        CERTIFICATE_DURATION >= 12 ~ '12 to 13 months',
        TRUE ~ 'Not Available'
      )
      ) |> 
      dplyr::group_by(across(all_of(chart_fields))) |> 
      dplyr::summarise(ISSUED_CERTS = sum(ISSUED_CERTS), .groups = "keep") |> 
      dplyr::ungroup()
  }
  
  # create the support datasets (including historic CYs)
  # for "England only" services use a n/a placeholder for country
  if(service_area %in% c("MAT","MED","PPC","HRTPPC")){
    obj_suppData <- df_issue |> 
      dplyr::mutate(COUNTRY = 'n/a') |> 
      dplyr::relocate(COUNTRY, .after = ISSUE_CY)
  } else {
    # data needs to be collated from database again to include the country aggregation
    obj_suppData <- get_hes_issue_data(con, db_table_name, service_area, min_ym, max_ym, supp_fields)
    
    # reclassify MATEX duration
    if(service_area == 'MAT'){
      obj_suppData <- obj_suppData |> 
        mutate(CERTIFICATE_DURATION = dplyr::case_when(
          CERTIFICATE_DURATION < 12 ~ '11 months or less',
          CERTIFICATE_DURATION >= 20 ~ '20 to 22 months',
          CERTIFICATE_DURATION >= 18 ~ '18 to 19 months',
          CERTIFICATE_DURATION >= 16 ~ '16 to 17 months',
          CERTIFICATE_DURATION >= 14 ~ '14 to 15 months',
          CERTIFICATE_DURATION >= 12 ~ '12 to 13 months',
          TRUE ~ 'Not Available'
        )
        ) |> 
        dplyr::group_by(across(all_of(chart_fields))) |> 
        dplyr::summarise(ISSUED_CERTS = sum(ISSUED_CERTS), .groups = "keep") |> 
        dplyr::ungroup()
    }
    
  }
  obj_suppData <- obj_suppData |>
    dplyr::arrange_at(supp_fields) |> 
    # replace null durations with 'Not Available'
    dplyr::mutate(
      CERTIFICATE_DURATION = as.character(CERTIFICATE_DURATION),
      CERTIFICATE_DURATION = tidyr::replace_na(CERTIFICATE_DURATION, 'Not Available')
    ) |> 
    rename_df_fields()
  
  # return output
  return(list("support_data" = obj_suppData))
  
}
