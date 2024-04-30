#' get_ons_live_birth_data
#'
#' Create a dataset based on live birth data published by ONS
#' Data will show the number of live births split by ageband of mother
#' The published data includes data for multiple years
#' Data have been collected and used to populate a table in database
#' 
#' Parameters supplied to function will define the time period for which data will be produced
#'
#' @param db_connection active database connection
#' @param db_table_name database table containing application level data
#' @param geography geography classification (only COUNTRY currently available)
#' @param year live birth year based on calendar year figures (2013 to 2022)
#'
get_ons_live_birth_data <- function(db_connection, db_table_name, geography = 'COUNTRY', year = 2021){
  
  # Parameter tests ---------------------------------------------------------
  
  # parameter test: geography  
  if(!(geography %in% c("COUNTRY"))){
    stop("Invalid parameter (geography) supplied to ons_live_birth_data: must be one of COUNTRY", call. = FALSE)
  }
  
  # parameter test: year  
  if(!(year >= 2013 & year <= 2022)){
    stop("Invalid parameter (year) supplied to ons_live_birth_data: must be year between 2013 and 2022", call. = FALSE)
  }
  
  # Data Collection ---------------------------------------------------------
  
  # create connection to database table
  df_births <- dplyr::tbl(
    con, 
    from = dbplyr::in_schema(toupper(con@info$username), db_table_name)
  ) |> 
    # filter to time period and geography classification
    dplyr::filter(
      GEOGRAPHY_TYPE == geography,
      YEAR == year
    ) |> 
    dplyr::collect()
  
  # return output
  return(df_births)
    
}