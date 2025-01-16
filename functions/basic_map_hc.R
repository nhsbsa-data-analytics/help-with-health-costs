#' @title
#' Highcharts bar plot for values split by IMD decile
#'
#' @name basic_map_hc
#'
#' @description
#' Visualisation creation function: Create highchart map object showing single series data by area.
#'
#' The map will be a highchart object for a single series, with the parameters supplied defining
#' basic formatting to be applied to the map object.
#'
#' The map boundary data will be supplied as a parameter along with a data-frame containing data
#' split by the same area classifications.
#'
#' It is important to ensure that both the map geo data file and the data utilise the same codings
#' to ensure the map will render the correct data. Typically this will be codes assigned by ONS.
#'
#' Some chart elements (e.g. title, caption) have been excluded from the function although these
#' can be added to the highcharts object returned from this function.
#'
#'
#' @param geo_data geosjon data object which includes coordinate data for mapping
#' @param df A data frame containing data to plot
#' @param ons_code_field String defining name of column within the data that holds the area code (ONS coding)
#' @param area_name_field String defining name of column within the data that holds the area name to include in tooltip
#' @param value_field String defining name of column within the data that holds the values to be plotted
#' @param metric_definition_string String for the metric being reported (will be used as y-axis title and in tooltip)
#' @param decimal_places  No. of decimal places to format values to
#' \itemize{
#'  \item Default = 0
#'  \item Only values between 0 and 3 are valid
#'  \item Decimal values beyond 3 points are not easy to digest and alternative reporting should be considered
#' }
#' @param value_prefix Any value prefix that should be displayed
#' \itemize{
#'  \item Default = ""
#'  \item Prefix will be included in the axis title and prior to data values (e.g. £1)
#' }
#' @param order_of_magnitude Character to identify if figures should be adjusted to specific abbreviation
#' \itemize{
#'  \item Default = ""
#'  \item k:- values will be divided by 1,000 and figures will include "k" suffix
#'  \item m:- values will be divided by 1,000,000 and figures will include "m" suffix
#'  \item bn:- values will be divided by 1,000,000,000 and figures will include "bn" suffix
#'  \item tn:- values will be divided by 1,000,000,000,000 and figures will include "tn" suffix
#' }
#' @param custom_tooltip (Optional) String containing custom tooltip string
#' \itemize{
#'  \item If omitted the default tooltip will include the {area_name_field} and {value_field}
#'  \item HTML tags can be included in the tooltip string, e.g. &lt;b>&lt;/b>, &lt;br>
#'  \item Fields in the dataset should be referenced as \{point.field_name\}, the field_name will be case sensitive to match the dataset
#'  \item Example tooltip: "&lt;b>Area:&lt;/b> \{point.AREA_FIELD\}&lt;br>&lt;b>Metric:&lt;/b>\{point.METRIC_FIELD\}"
#' }
#'
#' @return Highchart object for map object
#'
#' @export
basic_map_hc <- function(
    geo_data,
    df,
    ons_code_field,
    area_name_field,
    value_field,
    metric_definition_string,
    decimal_places = 0,
    value_prefix = "",
    order_of_magnitude = "",
    custom_tooltip = ""
) {
  
  # Input review --------------------------------------------------------------------------------
  
  # review input parameters, providing error/warning messages as required and aborting call
  
  # area_code_col
  if(!ons_code_field %in% colnames(df)){
    stop(paste0("Invalid parameter value (ons_code_field) : Field name (",ons_code_field,") does not exist in supplied data"), call. = FALSE)
  }
  
  # area_name_field
  if(!area_name_field %in% colnames(df)){
    stop(paste0("Invalid parameter value (area_name_field) : Field name (",area_name_field,") does not exist in supplied data"), call. = FALSE)
  }
  
  # value_field
  if(!value_field %in% colnames(df)){
    stop(paste0("Invalid parameter value (value_field) : Field name (",value_field,") does not exist in supplied data"), call. = FALSE)
  }
  
  # decimal_places
  if(!decimal_places  %in% c(0,1,2,3)){
    stop("Invalid parameter value (decimal_places ) : Must be one of 0/1/2/3", call. = FALSE)
  }
  
  # order_of_magnitude
  if(!order_of_magnitude %in% c("","k","m","bn","tn")){
    stop("Invalid parameter value (order_of_magnitude) : Must be blank or one of k/m/bn/tn", call. = FALSE)
  }
  
  
  # Data object ---------------------------------------------------------------------------------
  
  # define the data object based on parameters
  # as strings are being passed to define the key fields create these as additional columns
  # this will allow the other columns in the data to be retained for things like tooltips
  df <- df |>
    # easiest method is to rename the existing columns
    dplyr::rename(ONS_CODE := {{ ons_code_field }},
                  AREA := {{ area_name_field }},
                  VAL_COL := {{ value_field }}
    ) |>
    # then create duplicates with the original names
    dplyr::mutate({{ ons_code_field }} := ONS_CODE,
                  {{ area_name_field }} := AREA,
                  {{ value_field }} := VAL_COL
    )
  
  # adjust values based on supplied order of magnitude
  if(order_of_magnitude != ""){
    df <- df |>
      dplyr::mutate(VAL_COL = VAL_COL / switch(order_of_magnitude,
                                               "k" = 1000,
                                               "m" = 1000000,
                                               "bn" = 1000000000,
                                               "tn" = 1000000000000)
      )
  }
  
  # Tooltip creation ----------------------------------------------------------------------------
  
  if(custom_tooltip == ""){
    # create the tooltip object
    tooltip_text <- glue::glue(
      "<b>Area:</b>{{point.AREA}} <br>\\
      <b>{metric_definition_string}:</b>{value_prefix}{{point.value:,.{decimal_places}f}}{order_of_magnitude}"
    )
  } else {
    # use the custom tooltip
    tooltip_text <- custom_tooltip
  }
  
  
  # Create map object ---------------------------------------------------------------------------
  
  map <- highcharter::highchart() |>
    highcharter::hc_add_series_map(
      map = geo_data,
      df = df,
      joinBy = "ONS_CODE",
      value = "VAL_COL",
      # tooltip = list(text = tooltip_text),
      tooltip = list(
        headerFormat = "",
        pointFormat = tooltip_text
      ),
      animation = FALSE,
      borderWidth = 1,
      borderColor = '#000000'
    ) |>
    highcharter::hc_credits(enabled = TRUE) |>
    highcharter::hc_legend(
      enabled = TRUE,
      align = "left",
      verticalAlign = "bottom",
      title = list(text = metric_definition_string)
    ) |>
    highcharter::hc_colorAxis(
      min = min(df$VAL_COL),
      max = max(df$VAL_COL)
    ) |>
    nhsbsaR::theme_nhsbsa_highchart()
  
  # return the map object
  return(map)
  
}