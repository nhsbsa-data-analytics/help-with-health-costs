/*
NHSBSA Official Statistics: Health Exemption Services
NHS Prescription data collection
Version 1.0


AMENDMENTS:
	2024-05-03  : Steven Buckley    : Initial script created
    2024-06-04  : Steven Buckley    : Switched source for postcode and IMD reference
                                        Changed N/A to Not Available
    

DESCRIPTION:
    Identify a patient summary dataset to get estimated patient counts split by:
        AGE
            use the PDS_DOB field to calculate an age as of date supplied as parameter
            use the latest prescription from the time period
        ICB & IMD
            use the NSPL to map ICB and IMD to the patients LSOA as captured from EPS data
            use the latest prescription from the time period
    
    Count the overall number of patient and also the number of patients receiving prescription items for HRT qualifying medication
        Use drugs ever classified as HRT qualifying during the period

DEPENDENCIES:
	AML.PX_FORM_ITEM_ELEM_COMB_FACT :   "Fact" table containing records during certificate lifecycle
                                        Each application/certificate could have multiple records
                                        Includes key dates and certificate outcome/status
                                    
    DIM.CDR_EP_DRUG_BNF_DIM         :   "Dimension" table containing drug classification information
    
    DIM.AGE                         :   Reference table providing age band classification lookups
    
    OST.ONS_NSPL_MAY_24_11CEN       :   Reference table for National Statistics Postcode Lookup (NSPL)
                                        Contains mapping data from postcode to key geographics and deprivation profile data
                                        Based on NSPL for May 2024
    
    OST.IMD_2019                    :   Reference table for Indices of Multiple Deprivation
                                        Contains mapping data from LSOA to IMD_DECILE
    
*/
------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------SCRIPT START----------------------------------------------------------------------------------------------------------------------

create table HWHC_PX_PAT_FACT as

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

,

lsoa_classification_imd as
(
select  /*+ materialize */
            LSOA11              as LSOA_CODE,
            IMD_DECILE,
            ceil(IMD_DECILE/2)  as IMD_QUINTILE
from        OST.IMD_2019
)
--select * from lsoa_classification_imd;
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
select      /*+ materialize */
            pl.PATIENT_ID,
            pl.PATIENT_LSOA_CODE,
            icb.ICB,
            imd.IMD_DECILE,
            imd.IMD_QUINTILE
from        (
            select      PATIENT_ID,
                        PATIENT_LSOA_CODE,
                        rank() over (partition by PATIENT_ID order by YEAR_MONTH desc, PRESCRIBED_DATE desc, PATIENT_LSOA_CODE) as RNK
            from        AML.PX_FORM_ITEM_ELEM_COMB_FACT     fact
            where       1=1
                and     fact.YEAR_MONTH between &&p_min_ym and &&p_max_ym
                and     fact.PATIENT_IDENTIFIED = 'Y'
                and     fact.NHS_PAID_FLAG = 'Y'
                and     nvl(fact.CONSULT_ONLY_IND,'N') != 'Y'
                and     fact.DISPENSER_COUNTRY_OU = 1
                and     fact.PATIENT_LSOA_CODE is not null
            )                       pl
left join   lsoa_classification_icb icb  on  pl.PATIENT_LSOA_CODE = icb.LSOA_CODE
left join   lsoa_classification_imd imd  on  pl.PATIENT_LSOA_CODE = imd.LSOA_CODE
where       1=1
    and     RNK = 1
group by    pl.PATIENT_ID,
            pl.PATIENT_LSOA_CODE,
            icb.ICB,
            imd.IMD_DECILE,
            imd.IMD_QUINTILE
)
--select * from patient_lsoa;
-----SECTION END: LATEST LSOA CLASSIFICATION----------------------------------------------------------------------------------------------------------

,

-----SECTION START: LATEST AGE CLASSIFICATION---------------------------------------------------------------------------------------------------------
--for each identified PATIENT_ID, identify the latest used CALC_AGE during the supplied time period
--limit to only records with a CALC_AGE identified
patient_age as
(
select      /*+ materialize */
            px_age.PATIENT_ID,
            px_age.CALC_AGE,
            age_lkp.BAND_5YEARS,
            age_lkp.BAND_10YEARS,
            case 
                when px_age.CALC_AGE <16 then 'Not Available'
                when px_age.CALC_AGE >59 then 'Not Available'
                else age_lkp.BAND_5YEARS
            end as CUSTOM_AGE_BAND
from        (
            select      PATIENT_ID,
                        trunc((&&p_age_date - to_number(to_char(PDS_DOB,'YYYYMMDD')))/10000)   as CALC_AGE,
                        rank() over (partition by PATIENT_ID order by YEAR_MONTH desc, PRESCRIBED_DATE desc, trunc((&&p_age_date - to_number(to_char(pds_dob,'YYYYMMDD')))/10000) desc) as RNK
            from        AML.PX_FORM_ITEM_ELEM_COMB_FACT     fact
            where       1=1
                and     fact.YEAR_MONTH between &&p_min_ym and &&p_max_ym
                and     fact.PATIENT_IDENTIFIED = 'Y'
                and     fact.NHS_PAID_FLAG = 'Y'
                and     nvl(fact.CONSULT_ONLY_IND,'N') != 'Y'
                and     fact.DISPENSER_COUNTRY_OU = 1
                and     fact.PDS_DOB is not null
            )           px_age
left join   DIM.AGE_DIM age_lkp on  px_age.CALC_AGE = age_lkp.AGE
where       1=1
    and     px_age.RNK = 1
group by    px_age.PATIENT_ID,
            px_age.CALC_AGE,
            age_lkp.BAND_5YEARS,
            age_lkp.BAND_10YEARS,
            case 
                when px_age.CALC_AGE <16 then 'Not Available'
                when px_age.CALC_AGE >59 then 'Not Available'
                else age_lkp.BAND_5YEARS
            end
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
            case when fact.PATIENT_IDENTIFIED = 'Y' then 1 else 0 end   as PATIENT_IDENTIFIED_FLAG,
            fact.PATIENT_ID,
            nvl(pa.CALC_AGE,-1)                                         as AGE,
            nvl(pa.BAND_5YEARS,'Not Available')                         as BAND_5YEARS,
            nvl(pa.BAND_10YEARS,'Not Available')                        as BAND_10YEARS,
            nvl(pa.CUSTOM_AGE_BAND,'Not Available')                     as CUSTOM_AGE_BAND,
            nvl(pl.ICB,'Not Available')                                 as ICB,
            pl.IMD_DECILE,
            pl.IMD_QUINTILE,
            max(cdrd.HRT_FLAG)                                          as HRT_FLAG,
            sum(fact.ITEM_COUNT)                                        as ITEMS,
            sum(fact.ITEM_COUNT * cdrd.HRT_FLAG)                        as HRT_ITEMS
from        AML.PX_FORM_ITEM_ELEM_COMB_FACT     fact
inner join                                      cdrd    on  fact.CALC_PREC_DRUG_RECORD_ID   =   cdrd.RECORD_ID
left join   patient_lsoa                        pl      on  fact.PATIENT_ID                 =   pl.PATIENT_ID
left join   patient_age                         pa      on  fact.PATIENT_ID                 =   pa.PATIENT_ID
where       1=1
    and     fact.YEAR_MONTH between &&p_min_ym and &&p_max_ym
    and     fact.NHS_PAID_FLAG = 'Y'
    and     nvl(fact.CONSULT_ONLY_IND,'N') != 'Y'
    and     fact.DISPENSER_COUNTRY_OU = 1
group by    fact.PATIENT_IDENTIFIED,
            fact.PATIENT_ID,
            nvl(pa.CALC_AGE,-1),
            nvl(pa.BAND_5YEARS,'Not Available'),
            nvl(pa.BAND_10YEARS,'Not Available'),
            nvl(pa.CUSTOM_AGE_BAND,'Not Available'),
            nvl(pl.ICB,'Not Available'),
            pl.IMD_DECILE,
            pl.IMD_QUINTILE
)
--select * from pat_summary;
-----SECTION END: PATIENT SUMMARY---------------------------------------------------------------------------------------------------------------------

-----OUTPUT-------------------------------------------------------------------------------------------------------------------------------------------
--aggregate patient counts by the different combinations of age and location field
select      AGE,
            BAND_5YEARS,
            BAND_10YEARS,
            CUSTOM_AGE_BAND,
            ICB,
            IMD_DECILE,
            IMD_QUINTILE,
            sum(PATIENT_IDENTIFIED_FLAG)                as PATIENT_COUNT,
            sum(PATIENT_IDENTIFIED_FLAG * HRT_FLAG)     as HRT_PATIENT_COUNT,
            sum(ITEMS)                                  as TOTAL_ITEMS,
            sum(PATIENT_IDENTIFIED_FLAG * ITEMS)        as PATIENT_ITEMS,
            sum(PATIENT_IDENTIFIED_FLAG * HRT_ITEMS)    as PATIENT_HRT_ITEMS
from        pat_summary
group by    AGE,
            BAND_5YEARS,
            BAND_10YEARS,
            CUSTOM_AGE_BAND,
            ICB,
            IMD_DECILE,
            IMD_QUINTILE
;
---------------------SCRIPT END-----------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------