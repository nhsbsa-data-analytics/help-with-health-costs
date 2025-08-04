/*
NHSBSA Official Statistics: Health Exemption Services
HRT-PPC data collection
Version 1.0


AMENDMENTS:
	2024-03-25  : Steven Buckley    : Initial script created
    2024-05-03  : Steven Buckley    : Tweaked script to fit report scripts
                                        Added some additional fields and links to age bands
    2024-06-04  : Steven Buckley    : Switched source for postcode reference
                                        Changed N/A to Not Available
    2024-06-06  : Steven Buckley    : Adjust code to flag issued certificates
                                        Warehouse change removed ISSUED status to replace with ACTIVE/EXPIRED
                                        Switched to exclude PENDING and FAILED to limit impact of future changes
    2025-04-11  : Grace Libby       : Changed NSPL version used to Aug 24 (2011 census LSOAs)
    2025-06-06  : Grace Libby       : Added APPLICATION_CY and ISSUE_CY calendar year columns


DESCRIPTION:
    Identify a basic dataset holding key information related to HRT PPC applications and certificates.
    
    The NHSBSA Data Warehouse currently (as of March 2024) retains records for test records created during automated service testing.
    These records will need to be excluded from the dataset.
        These test records can be identified based on the captured name/address information.
        These records are expected to be automatically excluded from the data following future service development.   
    
    The NHSBSA Data Warehouse currently (as of March 2024) retains only a single record per application/certificate.
        The AML.HRT_APPLICATION_PROCESS_FACT is expected to hold multiple records
            The latest record will be identified by DW_ACTIVE_IND = 1
            To identify static data as of a point in time this can be done using a combination of DW_DATE_CREATED and DW_DATE_UPDATED        
    

DEPENDENCIES:
	DIM.HRT_CERTIFICATE_DIM             :   "Dimension" table containing certificate holder information
                                            Includes applicant information that is required to identify test cases to exclude
    
    AML.HRT_APPLICATION_PROCESS_FACT    :   "Fact" table containing records during certificate lifecycle
                                            Each application/certificate could have multiple records
                                            Includes key dates and certificate outcome/status

    OST.ONS_NSPL_AUG_24_11CEN           :   Reference table for National Statistics Postcode Lookup (NSPL)
                                            Contains mapping data from postcode to key geographics and deprivation profile data
                                            Based on NSPL for August 2024
*/

------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------SCRIPT START----------------------------------------------------------------------------------------------------------------------

create table HWHC_HRTPPC_FACT compress for query high as
select      hapf.CERTIFICATE_ID                                                                             as CERTIFICATE_NUMBER,
            'HRTPPC'                                                                                        as CERTIFICATE_TYPE,
            'Not Available'                                                                                 as CERTIFICATE_SUBTYPE,
            'HRTPPC'                                                                                        as SERVICE_AREA,
            'NHS Hormone Replacement Therapy Prescription Prepayment Certificate (HRT PPC)'                 as SERVICE_AREA_NAME,
            case when hapf.CERTIFICATE_STATUS_ID not in ('PENDING','FAILED') then 1 else 0 end              as CERTIFICATE_ISSUED_FLAG,
            hapf.CERTIFICATE_COUNT,
            hapf.CERTIFICATE_STATUS_ID                                                                      as CERTIFICATE_STATUS,
            case when hapf.CERTIFICATE_STATUS_ID = 'CANCELLED' then 1 else 0 end                            as CERTIFICATE_CANCELLED_FLAG,
            --applicant/holder information
            hapf.AGE_ON_APPLICATION                                                                         as CERTIFICATE_HOLDER_AGE,
            age.BAND_5YEARS,
            age.BAND_10YEARS,
            --calculate custom age band
            case
                -- for MATEX, MEDEX and PPC exclude any ages outside of expected range 15-59 (likely errors)
                --for MATEX anything above 45 group as 45+
                when hapf.AGE_ON_APPLICATION <= 15  then 'Not Available'
                when hapf.AGE_ON_APPLICATION >= 60  then 'Not Available'
                                                    else age.BAND_5YEARS
            end                                                                                             as CUSTOM_AGE_BAND,
            hapf.POSTCODE,
            pcd.LSOA11                                                                                      as LSOA,
            --remove ONS code for non-England areas
            case when substr(pcd.ICB,1,1) = 'E' then ICB else 'Not Available' end                           as ICB,
            nvl(pcd.ICB23CDH,'Not Available')                                                               as ICB_CODE,
            nvl(pcd.ICB23NM,'Not Available')                                                                as ICB_NAME,
            pcd.IMD_DECILE,
            case
                when pcd.IMD_DECILE in (1,2)    then 1
                when pcd.IMD_DECILE in (3,4)    then 2
                when pcd.IMD_DECILE in (5,6)    then 3
                when pcd.IMD_DECILE in (7,8)    then 4
                when pcd.IMD_DECILE in (9,10)   then 5
                                            else null
            end                                                                                             as IMD_QUINTILE,
            case
                when pcd.CTRY = 'E92000001' then 'England'
                when pcd.CTRY is null       then 'Unknown'
                                            else 'Other'
            end                                                                                             as COUNTRY,
            --key dates: application
            to_date(hapf.APPLICATION_DATE_SID, 'YYYYMMDD')                                                  as APPLICATION_DATE,
            substr(hapf.APPLICATION_DATE_SID,1,6)                                                           as APPLICATION_YM,
            extract(YEAR from add_months(to_date(hapf.APPLICATION_DATE_SID,'YYYYMMDD'), -3))||'/'||
                extract(YEAR from add_months(to_date(hapf.APPLICATION_DATE_SID,'YYYYMMDD'), 9))             as APPLICATION_FY,
            extract(YEAR from (to_date(hapf.APPLICATION_DATE_SID,'YYYYMMDD')))                              as APPLICATION_CY,
            --key dates: issue
            to_date(hapf.CERTIFICATE_ACTIVE_DATE_SID, 'YYYYMMDD')                                           as ISSUE_DATE,
            substr(hapf.CERTIFICATE_ACTIVE_DATE_SID,1,6)                                                    as ISSUE_YM,
            extract(YEAR from add_months(to_date(hapf.CERTIFICATE_ACTIVE_DATE_SID,'YYYYMMDD'), -3))||'/'||    
                extract(YEAR from add_months(to_date(hapf.CERTIFICATE_ACTIVE_DATE_SID,'YYYYMMDD'), 9))      as ISSUE_FY,
            extract(YEAR from (to_date(hapf.CERTIFICATE_ACTIVE_DATE_SID,'YYYYMMDD')))                       as ISSUE_CY,
            --key dates: active
            to_date(hapf.CERTIFICATE_START_DATE_SID, 'YYYYMMDD')                                            as CERTIFICATE_START_DATE,
            to_date(hapf.CERTIFICATE_END_DATE_SID, 'YYYYMMDD')                                              as CERTIFICATE_EXPIRY_DATE,
            --flag if certificates are post-dated to start the following month
            case 
                when substr(hapf.CERTIFICATE_START_DATE_SID,1,6) > substr(hapf.CERTIFICATE_ACTIVE_DATE_SID,1,6) 
                then 1 
                else 0 
            end                                                                                             as FLAG_START_FOLLOWING_MONTH
from        AML.HRT_APPLICATION_PROCESS_FACT    hapf
inner join  DIM.HRT_CERTIFICATE_DIM             hcd     on  hapf.CERTIFICATE_ID                                 =   hcd.CERTIFICATE_ID
left join   OST.ONS_NSPL_AUG_24_11CEN           pcd     on  regexp_replace(upper(hapf.POSTCODE),'[^A-Z0-9]','') =   regexp_replace(upper(pcd.PCD),'[^A-Z0-9]','')
left join   DIM.AGE_DIM                         age     on  hapf.AGE_ON_APPLICATION                             =   age.AGE
where       1=1
    --limit to records as of a set date supplied at runtime
    and     trunc(hapf.DW_DATE_CREATED) <= to_date(&&p_extract_date,'YYYYMMDD')
    and     nvl(hapf.DW_DATE_UPDATED,to_date(99991231,'YYYYMMDD')) > to_date(&&p_extract_date,'YYYYMMDD')
    --exclude test cases
    and     hapf.TEST_FLAG = 0
    and     not upper(hcd.ADDRESS_LINE1) like '%NHS B S A%'
    and     not (   (   upper(hcd.FIRST_NAME) like '%TEST%'
                    or  upper(hcd.LAST_NAME) in ('TEST', 'TESTINGTON', 'TESTING', 'HRT')
                    )
                or  (   upper(hcd.FIRST_NAME)   = 'UNKNOWN'
                    and upper(hcd.LAST_NAME)    = 'UNKNOWN'
                    )
                ) 
;
---------------------SCRIPT END-----------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------