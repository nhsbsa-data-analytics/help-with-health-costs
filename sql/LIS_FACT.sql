/*
NHSBSA Official Statistics: Health Exemption Services
NHS Low Income Scheme data collection
Version 1.0


AMENDMENTS:
	2024-03-25  : Steven Buckley    : Initial script created


DESCRIPTION:
    Identify a basic dataset holding key information related to NHS Low Income Scheme (LIS) applications and certificates.
    
    The NHSBSA Data Warehouse holds multiple records per application/certificate.
        The latest record will be identified by DW_ACTIVE_IND = 1
        To identify static data as of a point in time this can be done using a combination of DW_DATE_CREATED and DW_DATE_UPDATED
            Needs refinement as in rare occasions a certificate may multiple records for a single date
    

DEPENDENCIES:
	AML.CRS_APPLICATION_FACT    :   "Fact" table containing records during certificate lifecycle
                                    Each application/certificate could have multiple records
                                    Includes key dates and certificate outcome/status
                                    
    GRALI.ONS_NSPL_MAY_23       :   Reference table for National Statistics Postcode Lookup (NSPL)
                                    Contains mapping data from postcode to key geographics and deprivation profile data
                                    Based on NSPL for May 2023
*/
------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------SCRIPT START----------------------------------------------------------------------------------------------------------------------


drop table LIS_FACT purge;
create table LIS_FACT compress for query high as
select      caf.CASE_REF,            
            --update CERTIFICATE_TYPE_CODE label for cases where no certificate was issued
            case
                when    caf.CERTIFICATE_TYPE_CODE = '-1'
                then    'NO_CERT_ISSUED'
                else    caf.CERTIFICATE_TYPE_CODE
            end                                                                                         as CERTIFICATE_TYPE,
            caf.LETTER_TYPE_CODE,
            caf.HC2_FLAG,
            caf.HC3_FLAG,
            --flag where a HC2 or HC3 certificate was issued
            --this will include HC3 certificates above the support threshold)
            case
                when    caf.LATEST_APPLICATION_STATUS_CODE in ('PRINT', 'REPRI')
                    and caf.CERTIFICATE_TYPE_CODE in ('HC2','HC3')
                then    1
                else    0
            end                                                                                         as CERTIFICATE_ISSUED_FLAG,
            --flag where the application has been resolved and response issued to customer
            case
                when    caf.LATEST_APPLICATION_STATUS_CODE in ('PRINT', 'REPRI')
                then    1
                else    0
            end                                                                                         as APPLICATION_COMPLETE_FLAG,
        --flag HC3 certificates with no px/dental support due to exceeding contribution limits
            case
                when    caf.CERTIFICATE_TYPE_CODE = 'HC3'
                    and caf.DENTAL_CONTRIBUTION_AMT >= 384
                then    1
                else    0
            end                                                                                         as ABOVE_HC3_SUPPORT_THRESHOLD_FLAG,
            --applicant/holder information
            caf.APPLICANT_AGE                                                                           as CERTIFICATE_HOLDER_AGE,
            caf.CLIENT_GROUP_ID,
            ccgd.CLIENT_GROUP_DESC,
            caf.POSTCODE,
            pcd.LSOA11                                                                                  as LSOA,
            pcd.ICB,
            pcd.ICB23CDH,
            pcd.ICB23NM, 
            pcd.IMD_DECILE,
            case
                when pcd.CTRY = 'E92000001' then 'England'
                when pcd.CTRY is null       then 'Unknown'
                                            else 'Other'
            end                                                                                         as COUNTRY,            
            --key dates: application
            to_date(caf.APPLICATION_START_DT_ID, 'YYYYMMDD')                                            as APPLICATION_DATE,
            substr(caf.APPLICATION_START_DT_ID,1,6)                                                     as APPLICATION_YM,
            extract(YEAR from add_months(to_date(caf.APPLICATION_START_DT_ID,'YYYYMMDD'), -3))||'/'||
                extract(YEAR from add_months(to_date(caf.APPLICATION_START_DT_ID,'YYYYMMDD'), 9))       as APPLICATION_FY,
            --key dates: issue
            to_date(caf.CERTIFICATE_ISSUE_DT_ID, 'YYYYMMDD')                                            as ISSUE_DATE,
            substr(caf.CERTIFICATE_ISSUE_DT_ID,1,6)                                                     as ISSUE_YM,
            extract(YEAR from add_months(to_date(caf.CERTIFICATE_ISSUE_DT_ID,'YYYYMMDD'), -3))||'/'||    
                extract(YEAR from add_months(to_date(caf.CERTIFICATE_ISSUE_DT_ID,'YYYYMMDD'), 9))       as ISSUE_FY,
            --key dates: active
            to_date(caf.VALID_FROM_DT_ID, 'YYYYMMDD')                                                   as CERTIFICATE_START_DATE,
            to_date(caf.VALID_TO_DT_ID, 'YYYYMMDD')                                                     as CERTIFICATE_EXPIRY_DATE            
from        AML.CRS_APPLICATION_FACT    caf
inner join  DIM.CRS_CLIENT_GROUP_DIM    ccgd    on  caf.CLIENT_GROUP_ID =   ccgd.CLIENT_GROUP_ID
left join   GRALI.ONS_NSPL_MAY_23       pcd     on  regexp_replace(upper(caf.POSTCODE),'[^A-Z0-9]','') = regexp_replace(upper(pcd.PCD),'[^A-Z0-9]','')
where       1=1
    --limit to single record per certificate based on a specific date
    and     caf.DW_ACTIVE_IND = 1
    --exclude the UKVI Asylum Seekers that should not be included in reporting
    and     caf.CLIENT_GROUP_ID != 7
    --limit to HC1 applications excluding HC5 (refund only) applications
    and     caf.APPLICATION_TYPE_ID in (1,3,4)
;
---------------------SCRIPT END-----------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------