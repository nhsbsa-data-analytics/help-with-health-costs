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
    `Calendar Year` = "APPLICATION_CY",
    `Month` = "APPLICATION_YM",
    `Financial Year` = "ISSUE_FY",
    `Calendar Year` = "ISSUE_CY",
    `Month` = "ISSUE_YM",
    `Financial Year` = "FINANCIAL_YEAR",
    `Calendar Year` = "CALENDAR_YEAR",
    `Country` = "COUNTRY",
    `Certificate Duration` = "CERTIFICATE_DURATION",
    `Certificate Duration (months)` = "CERTIFICATE_DURATION_MONTHS",
    `Age Band` = "CUSTOM_AGE_BAND",
    `Age Group` = "AGE_GROUP",
    `IMD Quintile` = "IMD_QUINTILE",
    `ICB Code` = "ICB_CODE",
    `ICB Name` = "ICB_NAME",
    `Number of certificates issued` = "ISSUED_CERTS",
    `Number of certificates issued` = "ISSUED_CERTS_SF",
    `Number of applications received` = "APPLICATIONS",
    `Number of active certificates` = "ACTIVE_CERTS",
    `Number of HC2 certificates issued` = "pvt_issue_HC2",
    `Number of HC3 certificates issued` = "pvt_issue_HC3",
    `Number of HC3 certificates issued` = "ISSUED_HC3",
    `Number of HC3 certificates including HBD11 letter` = "ISSUED_HBD11",
    `Proportion of HC3 certificates including HBD11 letter (%)` = "PROP_HC3_HBD11",
    `Number of 12-month certificates issued` = "pvt_issue_12-month",
    `Number of 3-month certificates issued` = "pvt_issue_3-month",
    `Number of outcomes with no certificate issued` = "pvt_issue_No certificate issued",
    `Number of months between due date and certificate issue date` = "MONTHS_BETWEEN_DUE_DATE_AND_ISSUE",
    `Number of certificates issued (cumulative)` = "CUM_SUM_ISSUED_CERTS",
    `Proportion of certificates issued (cumulative %)` = "PROP_CUM_SUM_ISSUED_CERTS",
    `Number of issued certificates post-dated to start the following month` = 'POST_DATE_CERTS',
    `Issued certificates post-dated to start the following month (%)` = 'PROP_CERTS_POSTDATE'
  )
  
  # apply renaming if columns exist
  df <- df |> 
    dplyr::rename(any_of(rename_lookup))
  
  
  # return sql code
  return(df)
}