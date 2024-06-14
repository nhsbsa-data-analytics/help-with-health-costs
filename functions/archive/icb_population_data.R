#' icb_population_data
#'
#' Create a dataset based on population data published by ONS
#' Data will show the population, including the male/female splits
#' 
#' Parameters supplied to function will define the time period for which data will be produced,
#' the geography level and the age range
#'
#' @param year population year based on mid-year estimate (2021, 2022)
#' @param min_age minimum age to consider in aggregation (0 to 90)
#' @param max_age maximum age to consider in aggregation (0 to 90)
#' @param gender character to identify gender to include in reporting (M = Male, F = Female, T = Total)
#'
icb_population_data <- function(
    year, 
    min_age, 
    max_age,
    gender
){
  
# Parameter tests ---------------------------------------------------------

  # parameter test: year  
  if(!(year %in% c(2021, 2022))){
    stop("Invalid parameter (year) supplied to icb_population_data: must be year between 2021 and 2022", call. = FALSE)
  }
  
  # parameter test: min_age  
  if(!(min_age %in% seq(0,90))){
    stop("Invalid parameter (min_age) supplied to icb_population_data: must be integer between 0 and 90", call. = FALSE)
  }
  
  # parameter test: max_age  
  if(!(max_age %in% seq(0,90))){
    stop("Invalid parameter (max_age) supplied to icb_population_data: must be integer between 0 and 90", call. = FALSE)
  }
  
  # parameter test: gender  
  if(!(gender %in% c("M", "F", "T"))){
    stop("Invalid parameter (gender) supplied to icb_population_data: must be one of M, F or T (M=male/F=female/T=total)", call. = FALSE)
  }
  


# Data Collection ---------------------------------------------------------

  # define dataset based on the parameters supplied
  # different years may use different datasets
  
  if(year %in% c(2021,2022)){
    
    # url for identified dataset published by ONS with SICBL and ICB population by age and gender
    url = "https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/clinicalcommissioninggroupmidyearpopulationestimates/mid2021andmid2022/sapehealthgeogstablefinal.xlsx"
    
    # define dataset range depending on year
    if(year == 2021){
      sheet = "Mid-2021 ICB 2023"
      range = "A4:GG110"
    } else if (year == 2022){
      sheet = "Mid-2022 ICB 2023"
      range = "A4:GG110"
    } 
    
    # download the data file from the URL
    temp1 <- tempfile()
    icb_pop_url <-
      utils::download.file(
        url,
        temp1,
        mode = "wb"
      )
    
    # read xlsx population file which contains data by SICBL and age/gender
    pop_data <- readxl::read_xlsx(
      temp1,
      sheet = sheet,
      range = range,
      col_names = TRUE
    )
    
    # pivot to convert the columns for age/gender into rows
    pop_data <- pop_data |> 
      tidyr::pivot_longer(
        cols = `Total`:`M90`,
        names_to = "Category",
        values_to = "Population"
      )
    
    # reformat the category to split out age and gender
    pop_data <- pop_data |> 
      dplyr::filter(Category != "Total") |> 
      dplyr::mutate(
        Gender = substring(Category, 1, 1),
        Age = as.numeric(substring(Category, 2))
      ) |> 
      dplyr::select(-Category)
    
    # rename the geography columns
    pop_data <- pop_data |> 
      dplyr::rename(
        SICBL = `SICBL 2023 Code`,
        ICB = `ICB 2023 Code`,
        NHSREG = `NHSER 2023 Code`
      )
  }
  

# Data Aggregation --------------------------------------------------------

  # filter data by supplied age range
  pop_data <- pop_data |> 
    dplyr::filter(Age >= min_age & Age <= max_age) |> 
    dplyr::mutate(PopulationAgeRange = paste0(min_age, "-", max_age))
  
  # pivot to have columns for each gender (plus total)
  pop_data <- pop_data |> 
    tidyr::pivot_wider(
      names_from = Gender,
      values_from = Population
    ) |> 
    dplyr::mutate(T = M + F)
  
  # apply grouping based on chosen geography
  pop_data <- pop_data |>
    dplyr::group_by(ICB, PopulationAgeRange) |> 
    dplyr::summarise(
      M = sum(M),
      F = sum(F),
      T = sum(T),
      .groups = "keep"
    ) |> 
    dplyr::ungroup()
  
  # return only the chosen gender results
  pop_data <- pop_data |>
    dplyr::select(
      ICB,
      BASE_POPULATION := {{ gender }}
    )
  
  # return the formatted dataset
  return(pop_data)
}