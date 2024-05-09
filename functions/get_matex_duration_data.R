#' get_matex_duration_data
#'
#' collect issued certificate data from the HES dataset looking at MATEX only
#' split certificates by months between due date and date of certificate issue
#' additional parameters will identify the time period for analysis
#'
#' @param db_connection active database connection
#' @param db_table_name database table containing application level data
#' @param min_ym first month for analysis (format YYYYMM)
#' @param max_ym last month for analysis (format YYYYMM)
#'
get_matex_duration_data <- function(db_connection, db_table_name, min_ym, max_ym) {
  
  # Test Parameters ---------------------------------------------------------
  
  # Test Parameter: db_table
  # abort if supplied table does not exist
  if(DBI::dbExistsTable(conn = db_connection, name = DBI::Id(schema = toupper(db_connection@info$username), table = db_table_name)) == FALSE){
    stop(paste0("Invalid parameter (db_table_name) supplied to get_hes_issue_data: ", db_table_name, " does not exist!"), call. = FALSE)
  }
  
  # Test Parameter: min_ym
  # abort if not a valid YM
  if(is.na(as.Date(paste0(substr(min_ym,1,4),'-',substr(min_ym,5,6),'-01'), optional = TRUE) == TRUE)){
    stop("Invalid parameter (min_ym) supplied to get_hes_issue_data Must be valid year_month (YYYYMM)", call. = FALSE)
  }
  # abort if too early (pre April 2015) or later than current month
  if(min_ym < 201504 | min_ym > format(Sys.Date(),'%Y%m')){
    stop("Invalid parameter (min_ym) supplied to get_hes_issue_data Must be valid year_month (YYYYMM) between 201504 and current month", call. = FALSE)
  }
  
  # Test Parameter: max_ym
  # abort if not a valid YM
  if(is.na(as.Date(paste0(substr(max_ym,1,4),'-',substr(max_ym,5,6),'-01'), optional = TRUE) == TRUE)){
    stop("Invalid parameter (max_ym) supplied to get_hes_issue_data Must be valid year_month (YYYYMM)", call. = FALSE)
  }
  # abort if too early (pre April 2015) or later than current month
  if(max_ym < 201504 | max_ym > format(Sys.Date(),'%Y%m')){
    stop("Invalid parameter (max_ym) supplied to get_hes_issue_data Must be valid year_month (YYYYMM) between 201504 and current month", call. = FALSE)
  }
  
  
  # Extract Data ------------------------------------------------------------
  
  # create connection to database table
  df_issue <- dplyr::tbl(
    con, 
    from = dbplyr::in_schema(toupper(con@info$username), db_table_name)
  ) |> 
    # filter to service area and time periods
    dplyr::filter(
      CERTIFICATE_TYPE == 'MAT',
      ISSUE_YM >= min_ym,
      ISSUE_YM <= max_ym,
      CERTIFICATE_ISSUED_FLAG == 1 
    ) |>
    # apply formatting to reclassify anything beyond -9 months and +12 months
    dplyr::mutate(MONTHS_BETWEEN_DUE_DATE_AND_ISSUE = dplyr::case_when(
      MONTHS_BETWEEN_DUE_DATE_AND_ISSUE < -9 ~ NA,
      MONTHS_BETWEEN_DUE_DATE_AND_ISSUE > 12 ~ NA,
      TRUE ~ MONTHS_BETWEEN_DUE_DATE_AND_ISSUE
    )) |> 
    # summarise, splitting by month/fy and country of applicant
    dplyr::group_by(SERVICE_AREA_NAME, ISSUE_FY, MONTHS_BETWEEN_DUE_DATE_AND_ISSUE) |> 
    dplyr::summarise(ISSUED_CERTS = n(), .groups = "keep") |> 
    dplyr::collect()
  
  # format to calculate rolling sums
  df_issue <- df_issue |> 
    dplyr::arrange(MONTHS_BETWEEN_DUE_DATE_AND_ISSUE) |> 
    dplyr::ungroup() |> 
    dplyr::mutate(
      CUM_SUM_ISSUED_CERTS = cumsum(ISSUED_CERTS),
      PROP_CUM_SUM_ISSUED_CERTS = round(CUM_SUM_ISSUED_CERTS / sum(ISSUED_CERTS) * 100, 1)
    )
  
  # return output
  return(df_issue)
  
}
