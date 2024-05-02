#' get_ons_live_birth_imd_data
#'
#' Create a dataset based on live birth data published by ONS
#' Data will show the number of live births split by IMD
#' The published data includes data for multiple years
#' Data have been collected and used to populate a table in database
#' 
#' Parameters supplied to function will define the time period for which data will be produced
#'
#' @param db_connection active database connection
#' @param db_table_name database table containing application level data
#' @param aggregation_level (IMD_DECILE or IMD_QUINTILE) aggregation level to summarise data
#' @param year live birth year based on calendar year figures (2013 to 2021)
#'
get_ons_live_birth_imd_data <- function(db_connection, db_table_name, aggregation_level = 'IMD_DECILE', year = 2021){
  
  # Parameter tests ---------------------------------------------------------
  
  # parameter test: geography  
  if(!(aggregation_level %in% c("IMD_DECILE","IMD_QUINTILE"))){
    stop("Invalid parameter (aggregation_level) supplied to get_ons_live_birth_imd_data: must be one of IMD_DECILE / IMD_QUINTILE", call. = FALSE)
  }
  
  # parameter test: year  
  if(!(year >= 2013 & year <= 2021)){
    stop("Invalid parameter (year) supplied to get_ons_live_birth_imd_data: must be year between 2013 and 2021", call. = FALSE)
  }
  
  # Data Collection ---------------------------------------------------------
  
  # create connection to database table
  df_births <- dplyr::tbl(
    con, 
    from = dbplyr::in_schema(toupper(con@info$username), db_table_name)
  ) |> 
    # filter to time period
    dplyr::filter(
      YEAR == year
    ) |>
    # aggregate based on provided column
    dplyr::group_by(across(all_of(aggregation_level))) |> 
    dplyr::summarise(LIVE_BIRTHS = sum(LIVE_BIRTHS)) |> 
    dplyr::collect()
  
  # return output
  return(df_births)
  
}
