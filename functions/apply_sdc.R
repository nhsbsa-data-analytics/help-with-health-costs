#function to apply statistical disclosure control (SDC)
#applies to total identified patients, total items, total NIC columns 
#user can set level to apply masking to, and set masking value
#defaults to a level of fewer than 5 identified patients and masks with NA 
#example: apply_sdc()

apply_sdc <- function(data,
                          suppress_column,
                          level = 5,
                          rounding = FALSE,
                          round_val = 5,
                          mask = "",
                          exclude_columns = NULL) {
  
  rnd <- round_val
  
  if (is.character(mask)) {
    type <- function(x) as.character(x)
  } else {
    type <- function(x) x
  }
  
  # Get numeric columns that are not in exclude_columns
  numeric_exclude <- setdiff(names(data), exclude_columns)
  numeric_exclude <- intersect(numeric_exclude, names(data)[sapply(data, is.numeric)])
  
  # Apply SDC only to numeric columns not in exclude_columns
  data |> dplyr::mutate(dplyr::across(
    .cols = all_of(numeric_exclude),
    .fns = ~ dplyr::case_when(
      .data[[suppress_column]] >= level & rounding == TRUE ~ as.numeric(type(rnd * round(.x / rnd))),
      .data[[suppress_column]] < level & .data[[suppress_column]] > 0 & rounding == TRUE ~ as.numeric(mask),
      .data[[suppress_column]] < level & .data[[suppress_column]] > 0 & rounding == FALSE ~ as.numeric(mask),
      TRUE ~ as.numeric(type(.x))
    ),
    .names = "{.col}"
  ))
}