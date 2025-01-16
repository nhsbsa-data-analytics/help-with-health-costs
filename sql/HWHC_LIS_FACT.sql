/*
NHSBSA Official Statistics: Health Exemption Services
NHS Low Income Scheme data collection
Version 1.0


AMENDMENTS:
	2024-03-25  : Steven Buckley    : Initial script created
    2024-03-28  : Steven Buckley    : Amended code to handle outcomes other than a HC2/HC3 being issued
    2024-04-09  : Steven Buckley    : Added custom fields for CERTIFICATE_DURATION, AGE_BAND and IMD_QUINTILE
    2024-06-04  : Steven Buckley    : Switched source for postcode reference
                                      Changed N/A to Not Available
    2024-06-06  : Steven Buckley    : Switched to BUSINESS_DT_ID as the date of application

DESCRIPTION:
    Identify a basic dataset holding key information related to NHS Low Income Scheme (LIS) applications and certificates.

    The NHSBSA Data Warehouse holds multiple records per application/certificate.
        The latest record will be identified by DW_ACTIVE_IND = 1
        To identify static data as of a point in time this can be done using a combination of DW_DATE_CREATED and DW_DATE_UPDATED
            Needs refinement as in rare occasions a certificate may multiple records for a single date

    To identify where applications have been completed and a response supplied we can use:
        LATEST_APPLICATION_STATUS_CODE in ('PRINT', 'REPRI')

    During assessment if the applicant would be required to contribute more than the typical maximum cost of treatment they will not get a HC3,
    instead they will get a letter stating that they are over the threshold for support (NB: they can request a HC3 issued with these limits but 
    this is rare). These are captured as a LETTER_TYPE_CODE of 'HBD11' and are treated as a HC3.


DEPENDENCIES:
	AML.CRS_APPLICATION_FACT    :   "Fact" table containing records during certificate lifecycle
                                    Each application/certificate could have multiple records
                                    Includes key dates and certificate outcome/status
                                    
    DIM.AGE                     :   Reference table providing age band classification lookups
    
    OST.ONS_NSPL_MAY_24_11CEN   :   Reference table for National Statistics Postcode Lookup (NSPL)
                                    Contains mapping data from postcode to key geographics and deprivation profile data
                                    Based on NSPL for May 2024
    
*/

------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------SCRIPT START----------------------------------------------------------------------------------------------------------------------

create table HWHC_LIS_FACT_TEST compress for query high as
with 
base as
(
select      standard_hash(caf.CASE_REF, 'SHA256')                                       as ID,
            caf.CASE_REF,
            --update CERTIFICATE_TYPE_CODE label for cases where no certificate was issued
            case
                when    caf.CERTIFICATE_TYPE_CODE = '-1'
                then    'No certificate issued'
                else    caf.CERTIFICATE_TYPE_CODE
            end                                                                         as CERTIFICATE_TYPE,
            caf.LETTER_TYPE_CODE,
            caf.HC2_FLAG,
            caf.HC3_FLAG,
            --flag where a HC2 or HC3 certificate was issued
            --this will include HC3 certificates above the support threshold)
            case
                when    caf.LATEST_APPLICATION_STATUS_CODE in ('PRINT', 'REPRI')
                    and caf.CERTIFICATE_TYPE_CODE in ('HC2','HC3')
                    and caf.CERTIFICATE_ISSUE_DT_ID != 19000101
                then    1
                else    0
            end                                                                         as CERTIFICATE_ISSUED_FLAG,
            --flag where the application has been resolved and response issued to customer
            case
                when    caf.LATEST_APPLICATION_STATUS_CODE in ('PRINT', 'REPRI')
                then    1
                else    0
            end                                                                         as APPLICATION_COMPLETE_FLAG,
        --flag HC3 certificates with no px/dental support due to exceeding contribution limits
            case
                when    caf.CERTIFICATE_TYPE_CODE = 'HC3'
                    and caf.DENTAL_CONTRIBUTION_AMT >= 384
                then    1
                else    0
            end                                                                         as ABOVE_HC3_SUPPORT_THRESHOLD_FLAG,
            --applicant/holder information
            caf.APPLICANT_AGE                                                           as CERTIFICATE_HOLDER_AGE,
            age.BAND_5YEARS,
            age.BAND_10YEARS,
            caf.CLIENT_GROUP_ID,
            ccgd.CLIENT_GROUP_DESC,
            pcd.LSOA11                                                                  as LSOA,
            pcd.ICB,
            pcd.ICB23CDH                                                                as ICB_CODE,
            pcd.ICB23NM                                                                 as ICB_NAME, 
            pcd.IMD_DECILE,
            case
                when pcd.CTRY = 'E92000001' then 'England'
                when pcd.CTRY is null       then 'Unknown'
                                            else 'Other'
            end                                                                         as COUNTRY,            
            --key dates: application
            BUSINESS_DT_ID,
            to_date(caf.BUSINESS_DT_ID, 'YYYYMMDD')                                     as APPLICATION_DATE,
            substr(caf.BUSINESS_DT_ID,1,6)                                              as APPLICATION_YM,
            --key dates: issue
            to_date(caf.CERTIFICATE_ISSUE_DT_ID, 'YYYYMMDD')                            as ISSUE_DATE,
            substr(caf.CERTIFICATE_ISSUE_DT_ID,1,6)                                     as ISSUE_YM,
            --key dates: active
            to_date(caf.VALID_FROM_DT_ID, 'YYYYMMDD')                                   as CERTIFICATE_START_DATE,
            substr(caf.VALID_FROM_DT_ID, 1,6)                                           as CERTIFICATE_START_YM,
            to_date(caf.VALID_TO_DT_ID, 'YYYYMMDD')                                     as CERTIFICATE_EXPIRY_DATE,
            substr(caf.VALID_TO_DT_ID, 1,6)                                             as CERTIFICATE_EXPIRY_YM
from        AML.CRS_APPLICATION_FACT    caf
inner join  DIM.CRS_CLIENT_GROUP_DIM    ccgd    on  caf.CLIENT_GROUP_ID =   ccgd.CLIENT_GROUP_ID
left join   OST.ONS_NSPL_MAY_24_11CEN   pcd     on  regexp_replace(upper(caf.POSTCODE),'[^A-Z0-9]','') = regexp_replace(upper(pcd.PCD),'[^A-Z0-9]','')
left join   DIM.AGE_DIM                 age     on  caf.APPLICANT_AGE   =   age.AGE
where       1=1
    --limit to records as of a set date supplied at runtime
    and     caf.DW_DATE_CREATED <= to_date(&&p_extract_date,'YYYYMMDD')
    and     nvl(caf.DW_DATE_UPDATED,to_date(99991231,'YYYYMMDD')) > to_date(&&p_extract_date,'YYYYMMDD')
    --exclude the UKVI Asylum Seekers that should not be included in reporting
    and     caf.CLIENT_GROUP_ID != 7
    --limit to HC1 applications excluding HC5 (refund only) applications
    and     caf.APPLICATION_TYPE_ID in (1,3,4)
)

select      ID,
            'LIS'                                                                               as SERVICE_AREA,
            'NHS Low Income Scheme'                                                             as SERVICE_AREA_NAME,
            CASE_REF,
            --Recode CERTIFICATE_TYPE to only show HC2/HC3 if a H2/HC3 was issued
            CERTIFICATE_TYPE,
            case
                when CERTIFICATE_ISSUED_FLAG = 1
                then CERTIFICATE_TYPE
                else 'No certificate issued'
            end                                                                                 as CERTIFICATE_SUBTYPE,
            LETTER_TYPE_CODE,
            HC2_FLAG,
            HC3_FLAG,
            CERTIFICATE_ISSUED_FLAG,
            case
                when CERTIFICATE_ISSUED_FLAG = 1
                then round(months_between(CERTIFICATE_EXPIRY_DATE, CERTIFICATE_START_DATE),0)
                else null
            end                                                                                 as CERTIFICATE_DURATION_MONTHS,
            case
                when CERTIFICATE_ISSUED_FLAG != 1   then 'Not Available'
                when round(months_between(CERTIFICATE_EXPIRY_DATE, CERTIFICATE_START_DATE),0) < 6   then '0 to 5 months'
                when round(months_between(CERTIFICATE_EXPIRY_DATE, CERTIFICATE_START_DATE),0) = 6   then '06 months'
                when round(months_between(CERTIFICATE_EXPIRY_DATE, CERTIFICATE_START_DATE),0) < 12  then '07 to 11 months'
                when round(months_between(CERTIFICATE_EXPIRY_DATE, CERTIFICATE_START_DATE),0) = 12  then '12 months'
                when round(months_between(CERTIFICATE_EXPIRY_DATE, CERTIFICATE_START_DATE),0) > 12  then '13 months +'
                else 'Not Available'
            end                                                                                 as CERTIFICATE_DURATION,
            APPLICATION_COMPLETE_FLAG,
            ABOVE_HC3_SUPPORT_THRESHOLD_FLAG,
            CERTIFICATE_HOLDER_AGE,
            BAND_5YEARS,
            BAND_10YEARS,
            case
                when CERTIFICATE_HOLDER_AGE < 15    then 'Not Available'
                when CERTIFICATE_HOLDER_AGE > 99    then 'Not Available'
                when CERTIFICATE_HOLDER_AGE >= 65   then '65+'
                                                    else BAND_5YEARS
            end                                                                                 as CUSTOM_AGE_BAND,
            CLIENT_GROUP_ID,
            CLIENT_GROUP_DESC,
            LSOA,
            --remove ONS code for non-England areas
            case when substr(ICB,1,1) = 'E' then ICB else 'Not Available' end                   as ICB,
            nvl(ICB_CODE,'Not Available')                                                       as ICB_CODE,
            nvl(ICB_NAME,'Not Available')                                                       as ICB_NAME,
            IMD_DECILE,
            case
                when IMD_DECILE in (1,2)    then 1
                when IMD_DECILE in (3,4)    then 2
                when IMD_DECILE in (5,6)    then 3
                when IMD_DECILE in (7,8)    then 4
                when IMD_DECILE in (9,10)   then 5
                                            else null
            end                                                                                 as IMD_QUINTILE,
            COUNTRY,
            --key dates: application
            APPLICATION_DATE,
            APPLICATION_YM,
            extract(YEAR from add_months(APPLICATION_DATE, -3))||'/'||
                extract(YEAR from add_months(APPLICATION_DATE, 9))                              as APPLICATION_FY,
            --key dates: issue
            ISSUE_DATE,
            ISSUE_YM,
            extract(YEAR from add_months(ISSUE_DATE, -3))||'/'||    
                extract(YEAR from add_months(ISSUE_DATE, 9))                                    as ISSUE_FY,
            --key dates: active
            --where a certificate was not issued make sure dates are null as no certificate to be active
            case
                when CERTIFICATE_ISSUED_FLAG = 1    then CERTIFICATE_START_DATE
                                                    else NULL
            end                                                                                 as CERTIFICATE_START_DATE,
            case
                when CERTIFICATE_ISSUED_FLAG = 1    then CERTIFICATE_START_YM
                                                    else NULL
            end                                                                                 as CERTIFICATE_START_YM,
            case
                when CERTIFICATE_ISSUED_FLAG = 1    then CERTIFICATE_EXPIRY_DATE
                                                    else NULL
            end                                                                                 as CERTIFICATE_EXPIRY_DATE,
            case
                when CERTIFICATE_ISSUED_FLAG = 1    then CERTIFICATE_EXPIRY_YM
                                                    else NULL
            end                                                                                 as CERTIFICATE_EXPIRY_YM
from        base
;
---------------------SCRIPT END-----------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------