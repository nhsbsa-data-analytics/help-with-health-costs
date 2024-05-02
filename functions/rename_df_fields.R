#' rename_df_fields function
#'
#' apply column renaming to a dataframe to support readability
#'
#' @param df dataframe object to apply field name changes to
#'
#' @return updated dataframe object
#' @export
rename_df_fields <- function(df) {
  
  # define the lookup for the renaming
  rename_lookup <- c(
    `HwHC Service` = "SERVICE_AREA_NAME",
    `Certificate Type` = "CERTIFICATE_SUBTYPE",
    `Financial Year` = "APPLICATION_FY",
    `Financial Year` = "ISSUE_FY",
    `Financial Year` = "FINANCIAL_YEAR",
    `Country` = "COUNTRY",
    `Age Band` = "CUSTOM_AGE_BAND",
    `Age Group` = "AGE_GROUP",
    `IMD Quintile` = "IMD_QUINTILE",
    `ICB Code` = "ICB_CODE",
    `ICB Name` = "ICB_NAME",
    `Number of certificates issued` = "ISSUED_CERTS",
    `Number of applications received` = "APPLICATIONS",
    `Number of active certificates` = "ACTIVE_CERTS",
    `Number of HC2 certificates issued` = "pvt_issue_HC2",
    `Number of HC3 certificates issued` = "pvt_issue_HC3",
    `Number of outcomes with no certificate issued` = "pvt_issue_No certificate issued"
  )
  
  # apply renaming if columns exist
  df <- df |> 
    dplyr::rename(any_of(rename_lookup))
  
  
  # return sql code
  return(df)
}