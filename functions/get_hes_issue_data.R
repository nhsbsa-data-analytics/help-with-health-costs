#' get_hes_issue_data
#'
#' collect issued certificate data from the HES datasets
#' This dataset covers multiple services so service area should be specified by a parameter
#' additional parameters will identify the time period for analysis and which fields to group results by
#'
#' @param db_connection active database connection
#' @param db_table_name database table containing application level data
#' @param service_area service area to collate data for, which must be one of: MAT, MED, PPC, TAX, LIS, HRTPPC
#' @param min_ym first month for analysis (format YYYYMM)
#' @param max_ym last month for analysis (format YYYYMM)
#' @param group_list list of fields to group results by
#'
get_hes_issue_data <- function(db_connection, db_table_name, service_area, min_ym, max_ym, group_list) {
  
  # Test Parameters ---------------------------------------------------------
  
  # Test Parameter: db_table
  # abort if supplied table does not exist
  if(DBI::dbExistsTable(conn = db_connection, name = DBI::Id(schema = toupper(db_connection@info$username), table = db_table_name)) == FALSE){
    stop(paste0("Invalid parameter (db_table_name) supplied to get_hes_issue_data: ", db_table_name, " does not exist!"), call. = FALSE)
  }
  
  # Test Parameter: service_area
  # abort if invalid service_area has been supplied
  if(!toupper(service_area) %in% c('MAT', 'MED', 'PPC', 'TAX', 'LIS','HRTPPC')){
    stop("Invalid parameter (service_area) supplied to get_hes_issue_data. Must be one of: MAT, MED, PPC, TAX, LIS, HRTPPC", call. = FALSE)
  } 
  
  # Test Parameter: min_ym
  # abort if not a valid YM
  if(is.na(as.Date(paste0(substr(min_ym,1,4),'-',substr(min_ym,5,6),'-01'), optional = TRUE) == TRUE)){
    stop("Invalid parameter (min_ym) supplied to get_hes_issue_data. Must be valid year_month (YYYYMM)", call. = FALSE)
  }
  # abort if too early (pre April 2015) or later than current month
  if(min_ym < 201504 | min_ym > format(Sys.Date(),'%Y%m')){
    stop("Invalid parameter (min_ym) supplied to get_hes_issue_data. Must be valid year_month (YYYYMM) between 201504 and current month", call. = FALSE)
  }
  
  # Test Parameter: max_ym
  # abort if not a valid YM
  if(is.na(as.Date(paste0(substr(max_ym,1,4),'-',substr(max_ym,5,6),'-01'), optional = TRUE) == TRUE)){
    stop("Invalid parameter (max_ym) supplied to get_hes_issue_data. Must be valid year_month (YYYYMM)", call. = FALSE)
  }
  # abort if too early (pre April 2015) or later than current month
  if(max_ym < 201504 | max_ym > format(Sys.Date(),'%Y%m')){
    stop("Invalid parameter (max_ym) supplied to get_hes_issue_data. Must be valid year_month (YYYYMM) between 201504 and current month", call. = FALSE)
  }
  
  
  # Extract Data ------------------------------------------------------------
  
  # create connection to database table
  df_issue <- dplyr::tbl(
    con, 
    from = dbplyr::in_schema(toupper(con@info$username), db_table_name)
  ) |> 
    # filter to service area and time periods
    dplyr::filter(
      SERVICE_AREA == toupper(service_area),
      ISSUE_YM >= min_ym,
      ISSUE_YM <= max_ym,
      CERTIFICATE_ISSUED_FLAG == 1
    ) |> 
    # summarise, splitting by supplied field list
    dplyr::group_by(across(all_of(group_list))) |> 
    dplyr::summarise(ISSUED_CERTS = n(), .groups = "keep") |> 
    dplyr::ungroup() |> 
    dplyr::collect()
  
  # return output
  return(df_issue)
  
}
