/*
NHSBSA Official Statistics: Health Exemption Services
HES data collection (PPC, MATEX, MEDEX, TAX CREDIT)
Version 1.0


AMENDMENTS:
	2024-03-25  : Steven Buckley    : Initial script created


DESCRIPTION:
    Identify a basic dataset holding key information related to HES applications and certificates.
        The HES database includes:
            Prescription Prepayment Certificates (PPC)
            maternity exemption certificates (MATEX) 
            medical exemption certificates (MEDEX)
            Tax Credit Exemption Certificate  
    
    The NHSBSA Data Warehouse holds multiple records per application/certificate.
        The latest record will be identified by DW_ACTIVE_IND = 1
        To identify static data as of a point in time this can be done using a combination of DW_DATE_CREATED and DW_DATE_UPDATED
            Needs refinement as in rare occasions a certificate may multiple records for a single date
    

DEPENDENCIES:
	DIM.HES_CERTIFICATE_DIM             :   "Dimension" table containing certificate holder information
                                            Includes the type and duration of the certificate
                                            Includes applicant information including age
    
    AML.HES_APPLICATION_PROCESS_FACT    :   "Fact" table containing records during certificate lifecycle
                                            Each application/certificate could have multiple records
                                            Includes key dates and certificate outcome/status
                                            
    DIM.HES_CERTIFICATE_STATUS_DIM      :   Reference table containing lookup for status code to description

    GRALI.ONS_NSPL_MAY_23               :   Reference table for National Statistics Postcode Lookup (NSPL)
                                            Contains mapping data from postcode to key geographics and deprivation profile data
                                            Based on NSPL for May 2023
*/

------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------SCRIPT START----------------------------------------------------------------------------------------------------------------------


drop table HES_FACT purge;
create table HES_FACT compress for query high as
select      hcd.CERTIFICATE_NUMBER,
            hcd.CERTIFICATE_TYPE,
            hcd.CERTIFICATE_DURATION,
            hapf.CERTIFICATE_ISSUED_FLAG,
            hapf.CERTIFICATE_STATUS,
            hcsd.CERTIFICATE_STATUS_DESC,
            case when hapf.CERTIFICATE_STATUS in (97,99) then 1 else 0 end                                  as CERTIFICATE_CANCELLED_FLAG,
            --applicant/holder information
            hcd.CERTIFICATE_HOLDER_AGE,
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
            to_date(hapf.APPLICATION_START_DATE_WID, 'YYYYMMDD')                                            as APPLICATION_DATE,
            substr(hapf.APPLICATION_START_DATE_WID,1,6)                                                     as APPLICATION_YM,
            extract(YEAR from add_months(to_date(hapf.APPLICATION_START_DATE_WID,'YYYYMMDD'), -3))||'/'||
                extract(YEAR from add_months(to_date(hapf.APPLICATION_START_DATE_WID,'YYYYMMDD'), 9))       as APPLICATION_FY,
            --key dates: issue
            to_date(hapf.CERTIFICATE_ISSUED_DATE_WID  default null on conversion error, 'YYYYMMDD')         as ISSUE_DATE,
            substr(hapf.CERTIFICATE_ISSUED_DATE_WID,1,6)                                                    as ISSUE_YM,
            extract(YEAR from add_months(to_date(hapf.CERTIFICATE_ISSUED_DATE_WID default null on conversion error,'YYYYMMDD'), -3))||'/'||    
                extract(YEAR from add_months(to_date(hapf.CERTIFICATE_ISSUED_DATE_WID default null on conversion error,'YYYYMMDD'), 9))      as ISSUE_FY,
            --key dates: active
            to_date(hapf.CERTIFICATE_START_DATE_WID default null on conversion error, 'YYYYMMDD')                                            as CERTIFICATE_START_DATE,
            to_date(hapf.CERTIFICATE_EXPIRY_DATE_WID default null on conversion error, 'YYYYMMDD')                                           as CERTIFICATE_EXPIRY_DATE
from        DIM.HES_CERTIFICATE_DIM             hcd
inner join  AML.HES_APPLICATION_PROCESS_FACT    hapf    on  hcd.CERTIFICATE_NUMBER  =   hapf.CERTIFICATE_NUMBER
inner join  DIM.HES_CERTIFICATE_STATUS_DIM      hcsd    on  hapf.CERTIFICATE_STATUS =   hcsd.CERTIFICATE_STATUS
left join   GRALI.ONS_NSPL_MAY_23               pcd     on  regexp_replace(upper(hapf.POSTCODE),'[^A-Z0-9]','') = regexp_replace(upper(pcd.PCD),'[^A-Z0-9]','')
where       1=1
    --limit to single record per certificate based on a specific date
    and     hapf.DW_ACTIVE_IND = 1
    
;
---------------------SCRIPT END-----------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------