#' get_icb_map_boundaries
#'
#' Extract map boundary data for ICB classifications from ONS arcgis
#' @param icb_year year to define which ICB classification to use
#'
get_icb_map_boundaries <- function(icb_year){

# Parameter tests ---------------------------------------------------------
  
  # parameter test: icb_year  
  if(!(icb_year %in% c(2023))){
    stop("Invalid parameter (icb_year) supplied to get_icb_map_boundaries: must be year between 2023 and 2023", call. = FALSE)
  }
  
  # define paramaters based on supplied year for ICB classifications
  if (icb_year == 2023){
    map_url = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Integrated_Care_Boards_April_2023_EN_BSC/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
    icb_field = "icb23cd"
  }
  
# Data Collection ---------------------------------------------------------
  
  # Import map boundary data for ICB from ONS arcgis service
  icb_map_boundaries <- sf::read_sf(map_url) |>
    janitor::clean_names() |>
    dplyr::rename(ONS_CODE = icb_field) |> 
    # limit to the relevant fields and limit to England, converting area codes to generic ONS_CODE
    dplyr::filter(grepl("^E", ONS_CODE)) |>
    dplyr::select(ONS_CODE, ONS_GEOMETRY = geometry) |>
    sf::st_transform(crs = 27700) |>
    geojsonsf::sf_geojson() |>
    jsonlite::fromJSON(simplifyVector = FALSE)
 
    return(icb_map_boundaries)   
}