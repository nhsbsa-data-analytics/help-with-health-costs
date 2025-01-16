/*
NHSBSA Official Statistics: Health Exemption Services
NHS Prescription data collection : Base Population
Version 1.0


AMENDMENTS:
	2024-05-03  : Steven Buckley    : Initial script created
    2024-06-04  : Steven Buckley    : Switched source for postcode reference
                                        Changed N/A to Not Available
    

DESCRIPTION:
    Identify a patient summary dataset to get estimated patient counts split by financial year:
        AGE
            use the PDS_DOB field to calculate an age as of date supplied as parameter
            use the latest prescription from the time period
        ICB
            use the NSPL to map ICB to the patients LSOA as captured from EPS data
            use the latest prescription from the time period
    
    Count the overall number of patient and also the number of patients receiving prescription items for HRT qualifying medication
        Use drugs ever classified as HRT qualifying during the period
        
EXECUTION NOTES:
    Performance if trying to run this script for multiple years is very poor.
    Script may need to be executed for a single financial year at a time

DEPENDENCIES:
	AML.PX_FORM_ITEM_ELEM_COMB_FACT :   "Fact" table containing records during certificate lifecycle
                                        Each application/certificate could have multiple records
                                        Includes key dates and certificate outcome/status
                                    
    DIM.CDR_EP_DRUG_BNF_DIM         :   "Dimension" table containing drug classification information
    
    OST.ONS_NSPL_MAY_24_11CEN       :   Reference table for National Statistics Postcode Lookup (NSPL)
                                        Contains mapping data from postcode to key geographics and deprivation profile data
                                        Based on NSPL for May 2024
    
*/
------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------SCRIPT START----------------------------------------------------------------------------------------------------------------------

create table HWHC_PX_PAT_FY_ICB as

with
-----SECTION START: LSOA CLASSIFICATION---------------------------------------------------------------------------------------------------------------
--using the NSPL identify a single IMD_DECILE and ICB per LSOA11 code
lsoa_classification_icb as
(
select  /*+ materialize */
        distinct
            LSOA11  as LSOA_CODE,
            ICB
from        OST.ONS_NSPL_MAY_24_11CEN
where       1=1
    and     LSOA11 like 'E%'
)
--select * from lsoa_classification_icb;
-----SECTION END: LSOA CLASSIFICATION-----------------------------------------------------------------------------------------------------------------

,

-----SECTION START: DRUG CLASSIFICATION---------------------------------------------------------------------------------------------------------------
--using the drug dimension identify if a drug was classified as eligible for the HRT PPC
--combine time periods to show if the drug was ever covered by the HRT PPC during the chosen time period
cdrd as
(
select  /*+ materialize */
            RECORD_ID,
            max(case when HRT_FLAG = 'Y' then 1 else 0 end) as HRT_FLAG
from        DIM.CDR_EP_DRUG_BNF_DIM
where       1=1
    and     YEAR_MONTH between &&p_min_ym and &&p_max_ym
group by    RECORD_ID
)
--select * from cdrd;
-----SECTION END: DRUG CLASSIFICATION-----------------------------------------------------------------------------------------------------------------

,

-----SECTION START: LATEST LSOA CLASSIFICATION--------------------------------------------------------------------------------------------------------
--for each identified PATIENT_ID, identify the latest used PATIENT_LSOA_CODE during the supplied time period
--limit to only records with a PATIENT_LSOA_CODE identified
patient_lsoa as
(
select  /*+ materialize */
            pl.FINANCIAL_YEAR,
            pl.PATIENT_ID,
            pl.PATIENT_LSOA_CODE,
            lc.ICB
from        (
            select      ymd.FINANCIAL_YEAR,
                        fact.PATIENT_ID,
                        fact.PATIENT_LSOA_CODE,
                        rank() over (
                                    partition by    ymd.FINANCIAL_YEAR, 
                                                    fact.PATIENT_ID 
                                    order by        fact.YEAR_MONTH desc, 
                                                    fact.PRESCRIBED_DATE desc, 
                                                    fact.PATIENT_LSOA_CODE
                                    )                                               as RNK
            from        AML.PX_FORM_ITEM_ELEM_COMB_FACT     fact
            inner join  DIM.YEAR_MONTH_DIM                  ymd on  fact.YEAR_MONTH = ymd.YEAR_MONTH
            where       1=1
                and     fact.YEAR_MONTH between &&p_min_ym and &&p_max_ym
                and     fact.PATIENT_IDENTIFIED = 'Y'
                and     fact.NHS_PAID_FLAG = 'Y'
                and     nvl(fact.CONSULT_ONLY_IND,'N') != 'Y'
                and     fact.DISPENSER_COUNTRY_OU = 1
                and     fact.PATIENT_LSOA_CODE is not null
            )                       pl
left join   lsoa_classification_icb lc  on  pl.PATIENT_LSOA_CODE = lc.LSOA_CODE
where       1=1
    and     RNK = 1
group by    pl.FINANCIAL_YEAR,
            pl.PATIENT_ID,
            pl.PATIENT_LSOA_CODE,
            lc.ICB
)
--select * from patient_lsoa;
-----SECTION END: LATEST LSOA CLASSIFICATION----------------------------------------------------------------------------------------------------------

,

-----SECTION START: FINANCIAL_YEAR AGE DATE-----------------------------------------------------------------------------------------------------------
--Identify the date to calculate age at for each financial year
--This will be the 30th September during the financial year
fy_age_date as
(
select  /*+ materialize */
            FINANCIAL_YEAR,
            min(CALENDAR_YEAR)||'0930' as CALC_AGE_DATE
from        DIM.YEAR_MONTH_DIM
where       1=1
    and     YEAR_MONTH between &&p_min_ym and &&p_max_ym
group by    FINANCIAL_YEAR
)
--select * from fy_age_date;
-----SECTION END: FINANCIAL_YEAR AGE DATE-------------------------------------------------------------------------------------------------------------

,

-----SECTION START: LATEST AGE CLASSIFICATION---------------------------------------------------------------------------------------------------------
--for each identified PATIENT_ID, identify the latest used CALC_AGE during the supplied time period
--limit to only records with a CALC_AGE identified
patient_age as
(
select  /*+ materialize */
            px_age.FINANCIAL_YEAR,
            px_age.PATIENT_ID,
            px_age.CALC_AGE
from        (
            select      ymd.FINANCIAL_YEAR,
                        fact.PATIENT_ID,
                        trunc((fad.CALC_AGE_DATE - to_number(to_char(fact.PDS_DOB,'YYYYMMDD')))/10000)   as CALC_AGE,
                        rank() over (
                                    partition by    ymd.FINANCIAL_YEAR, 
                                                    fact.PATIENT_ID 
                                    order by        fact.YEAR_MONTH desc, 
                                                    fact.PRESCRIBED_DATE desc, 
                                                    trunc((fad.CALC_AGE_DATE - to_number(to_char(fact.pds_dob,'YYYYMMDD')))/10000) desc
                                    )                                                                                                       as RNK
            from        AML.PX_FORM_ITEM_ELEM_COMB_FACT     fact
            inner join  DIM.YEAR_MONTH_DIM                  ymd on  fact.YEAR_MONTH     =   ymd.YEAR_MONTH
            inner join  fy_age_date                         fad on  ymd.FINANCIAL_YEAR  =   fad.FINANCIAL_YEAR
            where       1=1
                and     fact.YEAR_MONTH between &&p_min_ym and &&p_max_ym
                and     fact.PATIENT_IDENTIFIED = 'Y'
                and     fact.NHS_PAID_FLAG = 'Y'
                and     nvl(fact.CONSULT_ONLY_IND,'N') != 'Y'
                and     fact.DISPENSER_COUNTRY_OU = 1
                and     fact.PDS_DOB is not null
            )           px_age
where       1=1
    and     px_age.RNK = 1
group by    px_age.FINANCIAL_YEAR,
            px_age.PATIENT_ID,
            px_age.CALC_AGE
)
--select * from patient_age;
-----SECTION END: LATEST AGE CLASSIFICATION-----------------------------------------------------------------------------------------------------------

,

-----SECTION START: PATIENT SUMMARY-------------------------------------------------------------------------------------------------------------------
--summarise data for each individual PATIENT_ID
--each patient will only appear as a single record in the output
pat_summary as
(
select  /*+ materialize */
            ymd.FINANCIAL_YEAR,
            case when fact.PATIENT_IDENTIFIED = 'Y' then 1 else 0 end   as PATIENT_IDENTIFIED_FLAG,
            fact.PATIENT_ID,
            nvl(pa.CALC_AGE,-1)                                         as AGE,
            nvl(pl.ICB,'Not Available')                                 as ICB,
            max(cdrd.HRT_FLAG)                                          as HRT_FLAG,
            sum(fact.ITEM_COUNT)                                        as ITEMS,
            sum(fact.ITEM_COUNT * cdrd.HRT_FLAG)                        as HRT_ITEMS
from        AML.PX_FORM_ITEM_ELEM_COMB_FACT     fact
inner join  DIM.YEAR_MONTH_DIM                  ymd     on  fact.YEAR_MONTH                 =   ymd.YEAR_MONTH
inner join                                      cdrd    on  fact.CALC_PREC_DRUG_RECORD_ID   =   cdrd.RECORD_ID
left join   patient_lsoa                        pl      on  ymd.FINANCIAL_YEAR              =   pl.FINANCIAL_YEAR
                                                        and fact.PATIENT_ID                 =   pl.PATIENT_ID
left join   patient_age                         pa      on  ymd.FINANCIAL_YEAR              =   pa.FINANCIAL_YEAR
                                                        and fact.PATIENT_ID                 =   pa.PATIENT_ID
where       1=1
    and     fact.YEAR_MONTH between &&p_min_ym and &&p_max_ym
    and     fact.NHS_PAID_FLAG = 'Y'
    and     nvl(fact.CONSULT_ONLY_IND,'N') != 'Y'
    and     fact.DISPENSER_COUNTRY_OU = 1
group by    ymd.FINANCIAL_YEAR,
            fact.PATIENT_IDENTIFIED,
            fact.PATIENT_ID,
            nvl(pa.CALC_AGE,-1),
            nvl(pl.ICB,'Not Available')            
)
--select * from pat_summary;
-----SECTION END: PATIENT SUMMARY---------------------------------------------------------------------------------------------------------------------

-----OUTPUT-------------------------------------------------------------------------------------------------------------------------------------------
--aggregate patient counts by the different combinations of age and location field
select      FINANCIAL_YEAR,
            ICB,
            sum(PATIENT_IDENTIFIED_FLAG)                as PATIENT_COUNT,
            sum(PATIENT_IDENTIFIED_FLAG * HRT_FLAG)     as HRT_PATIENT_COUNT,
            sum(case
                    when AGE between 16 and 59
                    then PATIENT_IDENTIFIED_FLAG
                    else 0
                end)                                    as PATIENT_COUNT_16_59,
            sum(case
                    when AGE between 16 and 59
                    then PATIENT_IDENTIFIED_FLAG * HRT_FLAG
                    else 0
                end)                                    as HRT_PATIENT_COUNT_16_59,
            sum(ITEMS)                                  as TOTAL_ITEMS,
            sum(PATIENT_IDENTIFIED_FLAG * ITEMS)        as PATIENT_ITEMS,
            sum(PATIENT_IDENTIFIED_FLAG * HRT_ITEMS)    as PATIENT_HRT_ITEMS,
            sum(case
                    when AGE between 16 and 59
                    then PATIENT_IDENTIFIED_FLAG * ITEMS
                    else 0
                end)                                    as PATIENT_ITEMS_16_59,
            sum(case
                    when AGE between 16 and 59
                    then PATIENT_IDENTIFIED_FLAG * HRT_ITEMS
                    else 0
                end)                                    as HRT_PATIENT_ITEMS_16_59
from        pat_summary
group by    FINANCIAL_YEAR,
            ICB
;
---------------------SCRIPT END-----------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------