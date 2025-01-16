/*
NHSBSA Official Statistics: Health Exemption Services
HES data collection (PPC, MATEX, MEDEX, TAX CREDIT)
Version 1.0


AMENDMENTS:
	2024-03-25  : Steven Buckley    : Initial script created
    2024-04-22  : Steven Buckley    : Revised script to fit in pipeline approach
    2024-06-04  : Steven Buckley    : Switched source for postcode reference
                                      Changed N/A to Not Available


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

    OST.ONS_NSPL_MAY_24_11CEN           :   Reference table for National Statistics Postcode Lookup (NSPL)
                                            Contains mapping data from postcode to key geographics and deprivation profile data
                                            Based on NSPL for May 2024
*/

------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------SCRIPT START----------------------------------------------------------------------------------------------------------------------

create table HWHC_HES_FACT compress for query high as
with 
base as
(
select      standard_hash(hcd.CERTIFICATE_NUMBER, 'SHA256')                                                                                     as ID,
            hcd.CERTIFICATE_TYPE                                                                                                                as SERVICE_AREA,
            hcd.CERTIFICATE_TYPE,
            case
                when hcd.CERTIFICATE_TYPE = 'MAT' then 'Maternity exemption certificate'
                when hcd.CERTIFICATE_TYPE = 'MED' then 'Medical exemption certificate'
                when hcd.CERTIFICATE_TYPE = 'PPC' then 'NHS Prescription Prepayment Certificate'
                when hcd.CERTIFICATE_TYPE = 'TAX' then 'NHS tax credit exemption certificate'
            end                                                                                                                                 as SERVICE_AREA_NAME,
            case
                when hcd.CERTIFICATE_TYPE != 'PPC'  then 'Not Available'
                when hcd.CERTIFICATE_DURATION = 3   then '3-month'
                when hcd.CERTIFICATE_DURATION = 12  then '12-month'
                                                    else 'Not Available'
            end                                                                                                                                 as CERTIFICATE_SUBTYPE,
            hcd.CERTIFICATE_DURATION,
            hapf.CERTIFICATE_ISSUED_FLAG,
            hapf.CERTIFICATE_STATUS,
            hcsd.CERTIFICATE_STATUS_DESC,
            case when hapf.CERTIFICATE_STATUS in (97,99) then 1 else 0 end                                                                      as CERTIFICATE_CANCELLED_FLAG,
            --applicant/holder information
            hcd.CERTIFICATE_HOLDER_AGE,
            age.BAND_5YEARS,
            age.BAND_10YEARS,
            --calculate custom age band
            case
                -- for MATEX, MEDEX and PPC exclude any ages outside of expected range 15-59 (likely errors)
                --for MATEX anything above 45 group as 45+
                when hcd.CERTIFICATE_TYPE is null                                                           then 'Not Available'
                when hcd.CERTIFICATE_HOLDER_AGE is null                                                     then 'Not Available'
                when hcd.CERTIFICATE_TYPE in ('MAT','MED','PPC','TAX') and hcd.CERTIFICATE_HOLDER_AGE <= 14 then 'Not Available'
                when hcd.CERTIFICATE_TYPE in ('MAT','MED','PPC') and hcd.CERTIFICATE_HOLDER_AGE >= 60       then 'Not Available'
                when hcd.CERTIFICATE_TYPE in ('TAX') and hcd.CERTIFICATE_HOLDER_AGE > 90                    then 'Not Available'
                when hcd.CERTIFICATE_TYPE = 'MAT' and hcd.CERTIFICATE_HOLDER_AGE >= 45                      then '45+'
                when hcd.CERTIFICATE_TYPE = 'TAX' and hcd.CERTIFICATE_HOLDER_AGE >= 65                      then '65+'
                                                                                                            else age.BAND_5YEARS
            end                                                                                                                                 as CUSTOM_AGE_BAND,
            pcd.LSOA11                                                                                                                          as LSOA,
            --remove ONS code for non-England areas
            case when substr(pcd.ICB,1,1) = 'E' then ICB else 'Not Available' end                                                               as ICB,
            nvl(pcd.ICB23CDH,'Not Available')                                                                                                   as ICB_CODE,
            nvl(pcd.ICB23NM,'Not Available')                                                                                                    as ICB_NAME, 
            pcd.IMD_DECILE,
            case
                when pcd.IMD_DECILE in (1,2)    then 1
                when pcd.IMD_DECILE in (3,4)    then 2
                when pcd.IMD_DECILE in (5,6)    then 3
                when pcd.IMD_DECILE in (7,8)    then 4
                when pcd.IMD_DECILE in (9,10)   then 5
                                                else null
            end                                                                                                                                 as IMD_QUINTILE,
            case
                when pcd.CTRY = 'E92000001' then 'England'
                when pcd.CTRY is null       then 'Unknown'
                                            else 'Other'
            end                                                                                                                                 as COUNTRY,
            --key dates: application
            to_date(hapf.APPLICATION_START_DATE_WID, 'YYYYMMDD')                                                                                as APPLICATION_DATE,
            substr(hapf.APPLICATION_START_DATE_WID,1,6)                                                                                         as APPLICATION_YM,
            extract(YEAR from add_months(to_date(hapf.APPLICATION_START_DATE_WID,'YYYYMMDD'), -3))||'/'||
                extract(YEAR from add_months(to_date(hapf.APPLICATION_START_DATE_WID,'YYYYMMDD'), 9))                                           as APPLICATION_FY,
            --key dates: issue
            to_date(hapf.CERTIFICATE_ISSUED_DATE_WID  default null on conversion error, 'YYYYMMDD')                                             as ISSUE_DATE,
            substr(hapf.CERTIFICATE_ISSUED_DATE_WID,1,6)                                                                                        as ISSUE_YM,
            extract(YEAR from add_months(to_date(hapf.CERTIFICATE_ISSUED_DATE_WID default null on conversion error,'YYYYMMDD'), -3))||'/'||    
                extract(YEAR from add_months(to_date(hapf.CERTIFICATE_ISSUED_DATE_WID default null on conversion error,'YYYYMMDD'), 9))         as ISSUE_FY,
            --key dates: active
            to_date(hapf.CERTIFICATE_START_DATE_WID default null on conversion error, 'YYYYMMDD')                                               as CERTIFICATE_START_DATE,
            substr(hapf.CERTIFICATE_START_DATE_WID, 1,6)                                                                                        as CERTIFICATE_START_YM,
            to_date(hapf.CERTIFICATE_EXPIRY_DATE_WID default null on conversion error, 'YYYYMMDD')                                              as CERTIFICATE_EXPIRY_DATE,
            substr(hapf.CERTIFICATE_EXPIRY_DATE_WID, 1,6)                                                                                       as CERTIFICATE_EXPIRY_YM,
            hcd.BABY_DUE_DATE
from        DIM.HES_CERTIFICATE_DIM             hcd
inner join  AML.HES_APPLICATION_PROCESS_FACT    hapf    on  hcd.CERTIFICATE_NUMBER                              =   hapf.CERTIFICATE_NUMBER
inner join  DIM.HES_CERTIFICATE_STATUS_DIM      hcsd    on  hapf.CERTIFICATE_STATUS                             =   hcsd.CERTIFICATE_STATUS
left join   OST.ONS_NSPL_MAY_24_11CEN           pcd     on  regexp_replace(upper(hapf.POSTCODE),'[^A-Z0-9]','') = regexp_replace(upper(pcd.PCD),'[^A-Z0-9]','')
left join   DIM.AGE_DIM                         age     on  hcd.CERTIFICATE_HOLDER_AGE                          =   age.AGE
where       1=1
    --limit to records as of a set date supplied at runtime
    and     hapf.DW_DATE_CREATED <= to_date(&&p_extract_date,'YYYYMMDD')
    and     nvl(hapf.DW_DATE_UPDATED,to_date(99991231,'YYYYMMDD')) > to_date(&&p_extract_date,'YYYYMMDD')
)

select      ID,
            SERVICE_AREA,
            CERTIFICATE_TYPE,
            SERVICE_AREA_NAME,
            CERTIFICATE_SUBTYPE,
            CERTIFICATE_ISSUED_FLAG,
            CERTIFICATE_STATUS,
            CERTIFICATE_STATUS_DESC,
            CERTIFICATE_CANCELLED_FLAG,
            CERTIFICATE_HOLDER_AGE,
            BAND_5YEARS,
            BAND_10YEARS,
            CUSTOM_AGE_BAND,
            LSOA,
            ICB,
            ICB_CODE,
            ICB_NAME,
            IMD_DECILE,
            IMD_QUINTILE,
            COUNTRY,
            APPLICATION_DATE,
            APPLICATION_YM,
            APPLICATION_FY,
            ISSUE_DATE,
            ISSUE_YM,
            ISSUE_FY,
            CERTIFICATE_START_DATE,
            CERTIFICATE_START_YM,
            CERTIFICATE_EXPIRY_DATE,
            CERTIFICATE_EXPIRY_YM,
            --certificate duration (only capture where a certificate has been issued)
            --for MEDEX and PPC these can default based on set lengths
            --for MATEX, recode any where the dates would be less than 0 or greater than 22 as outside of feasible range and probably data error
            case
                when    ISSUE_YM = 190001                                                               then null
                when    CERTIFICATE_TYPE = 'MED'                                                        then 60
                when    CERTIFICATE_TYPE = 'PPC' 
                    and CERTIFICATE_DURATION in (3,12)                                                  then CERTIFICATE_DURATION
                when    CERTIFICATE_TYPE = 'PPC' 
                    and CERTIFICATE_DURATION not in (3,12)                                              then null
                when    CERTIFICATE_TYPE = 'MAT' 
                    and round(months_between(CERTIFICATE_EXPIRY_DATE, CERTIFICATE_START_DATE),0) < 0    then null
                when    CERTIFICATE_TYPE = 'MAT' 
                    and round(months_between(CERTIFICATE_EXPIRY_DATE, CERTIFICATE_START_DATE),0) > 22   then null
                else    round(months_between(CERTIFICATE_EXPIRY_DATE, CERTIFICATE_START_DATE),0)
            end                                                                                                                                 as CERTIFICATE_DURATION_MONTHS,
            --months between issue and due date
            --to replace with duration
            case
                when CERTIFICATE_TYPE != 'MAT'  then null
                when ISSUE_YM = 190001          then null
                else round(months_between(ISSUE_DATE, BABY_DUE_DATE),0)
            end                                                                                                                                 as MONTHS_BETWEEN_DUE_DATE_AND_ISSUE
from        base
;

---------------------SCRIPT END-----------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------