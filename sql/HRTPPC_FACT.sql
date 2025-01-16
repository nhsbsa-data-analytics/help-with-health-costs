/*
NHSBSA Official Statistics: Health Exemption Services
HRT-PPC data collection
Version 1.0


AMENDMENTS:
	2024-03-25  : Steven Buckley    : Initial script created


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

    GRALI.ONS_NSPL_MAY_23               :   Reference table for National Statistics Postcode Lookup (NSPL)
                                            Contains mapping data from postcode to key geographics and deprivation profile data
                                            Based on NSPL for May 2023
*/

------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------SCRIPT START----------------------------------------------------------------------------------------------------------------------

drop table HRTPPC_FACT purge;
create table HRTPPC_FACT compress for query high as
select      hapf.CERTIFICATE_ID                                                                             as CERTIFICATE_NUMBER,
            hapf.CERTIFICATE_COUNT                                                                          as CERTIFICATE_ISSUED_FLAG,
            hapf.CERTIFICATE_STATUS_ID,
            --applicant/holder information
            hapf.AGE_ON_APPLICATION                                                                         as CERTIFICATE_HOLDER_AGE,
            hapf.POSTCODE,
            pcd.LSOA11                                                                                      as LSOA,
            pcd.ICB,
            pcd.ICB23CDH,
            pcd.ICB23NM, 
            pcd.IMD_DECILE,
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
            --key dates: issue
            to_date(hapf.CERTIFICATE_ACTIVE_DATE_SID, 'YYYYMMDD')                                           as ISSUE_DATE,
            substr(hapf.CERTIFICATE_ACTIVE_DATE_SID,1,6)                                                    as ISSUE_YM,
            extract(YEAR from add_months(to_date(hapf.CERTIFICATE_ACTIVE_DATE_SID,'YYYYMMDD'), -3))||'/'||    
                extract(YEAR from add_months(to_date(hapf.CERTIFICATE_ACTIVE_DATE_SID,'YYYYMMDD'), 9))      as ISSUE_FY,
            --key dates: active
            to_date(hapf.CERTIFICATE_START_DATE_SID, 'YYYYMMDD')                                            as CERTIFICATE_START_DATE,
            to_date(hapf.CERTIFICATE_END_DATE_SID, 'YYYYMMDD')                                              as CERTIFICATE_EXPIRY_DATE
from        AML.HRT_APPLICATION_PROCESS_FACT    hapf
inner join  DIM.HRT_CERTIFICATE_DIM             hcd     on  hapf.CERTIFICATE_ID  =   hcd.CERTIFICATE_ID
left join   GRALI.ONS_NSPL_MAY_23               pcd     on  regexp_replace(upper(hapf.POSTCODE),'[^A-Z0-9]','') = regexp_replace(upper(pcd.PCD),'[^A-Z0-9]','')
where       1=1
    --limit to single record per certificate
    and     hapf.DW_ACTIVE_IND = 1
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