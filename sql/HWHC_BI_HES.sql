/*
NHSBSA Official Statistics: Health Exemption Services
PowerBI dataset creation: HES (MATEX, MEDEX, PPC and Tax Credits)
Version 1.0


AMENDMENTS:
	2024-05-15  : Steven Buckley    : Initial script created
    2024-06-03  : Steven Buckley    : Adjusted script to remove country aggregation for services without distinct country splits

DESCRIPTION:
    Identify a base dataset for PowerBI based on NHS Low Income Scheme data.
    
    Aggregating data for HES areas (MATEX, MEDEX, PPC, Tax Credits) to only the fields required for PowerBI.
    
    The different geography levels (Overall, Country and ICB) will be captured as seperate records so no aggregation is required in PowerBI.
        Country is only applicable for Tax Credits as other services are England only by design
    
    For each month the number of applications and issued certificates will be calcualted (based on different attribute groups)

DEPENDENCIES:
	HWHC_HES_FACT    :   Prebuilt dataset to application/certificate level for all cases from the PPC database system    
*/
------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------SCRIPT START----------------------------------------------------------------------------------------------------------------------

create table HWHC_BI_HES compress for query high as
with 

-----SECTION START: DATA: IDENTIFY APPLICATION DATA---------------------------------------------------------------------------------------------------
--include all applicaitons, aggregated by the application YM and FY
--for applications no certificate type is possible unless the SERVICE_AREA is PPC
--duration is only relevant for PPC, the subtype can be used
--for MAT, MED and PPC country should be assumed as ENGLAND as service not provided elsewhere
--not hardcoding this would get some with OTHER and UNKNOWN
application_data as
(
select  /*+ materialize */
            APPLICATION_FY                                              as FY,
            APPLICATION_YM                                              as YM,
            substr(APPLICATION_YM,1,4)||'-'||substr(APPLICATION_YM,5,2) as YEAR_MONTH,
            SERVICE_AREA_NAME,
            CERTIFICATE_SUBTYPE,
            CERTIFICATE_SUBTYPE                                         as CERTIFICATE_DURATION,
            case 
                when SERVICE_AREA = 'TAX'
                then COUNTRY
                else 'England'
            end                                                         as COUNTRY,
            ICB_NAME,
            ICB,
            nvl(to_char(IMD_QUINTILE),'Not Available')                  as IMD_QUINTILE,
            CUSTOM_AGE_BAND,
            sum(1)                                                      as APPLICATION_COUNT,
            0                                                           as ISSUED_COUNT
from        HWHC_HES_FACT
where       1=1
    and     APPLICATION_YM >= &&p_min_ym
    and     APPLICATION_YM <= &&p_max_ym
    and     SERVICE_AREA != 'TAX'
group by    APPLICATION_FY,
            APPLICATION_YM,
            SERVICE_AREA_NAME,
            CERTIFICATE_SUBTYPE,
            case 
                when SERVICE_AREA = 'TAX'
                then COUNTRY
                else 'England'
            end,
            ICB_NAME,
            ICB,
            IMD_QUINTILE,
            CUSTOM_AGE_BAND
)
--select * from application_data;
-----SECTION END: DATA: IDENTIFY APPLICATION DATA-----------------------------------------------------------------------------------------------------

,

-----SECTION START: DATA: IDENTIFY OUTCOMES DATA------------------------------------------------------------------------------------------------------
--include all cases where a certificate has been issued, aggregated by the issue YM and FY
--certificate type and duration are only relevant for PPC, use CERTIFICATE_SUBTYPE for both
outcome_data as
(
select  /*+ materialize */
            ISSUE_FY                                        as FY,
            ISSUE_YM                                        as YM,
            substr(ISSUE_YM,1,4)||'-'||substr(ISSUE_YM,5,2) as YEAR_MONTH,
            SERVICE_AREA_NAME,
            CERTIFICATE_SUBTYPE,
            case
                when SERVICE_AREA != 'MAT'              
                    then CERTIFICATE_SUBTYPE
                when CERTIFICATE_DURATION_MONTHS < 12   
                    then '11 months or less'
                when CERTIFICATE_DURATION_MONTHS >= 20
                    then '20 to 22 months'
                when CERTIFICATE_DURATION_MONTHS >= 18
                    then '18 to 19 months'
                when CERTIFICATE_DURATION_MONTHS >= 16
                    then '16 to 17 months'
                when CERTIFICATE_DURATION_MONTHS >= 14
                    then '14 to 15 months'
                when CERTIFICATE_DURATION_MONTHS >= 12
                    then '12 to 13 months'
                else 'Not Available'
            end                                             as CERTIFICATE_DURATION,
            case 
                when SERVICE_AREA = 'TAX'
                then COUNTRY
                else 'England'
            end                                             as COUNTRY,
            ICB_NAME,
            ICB,
            nvl(to_char(IMD_QUINTILE),'Not Available')      as IMD_QUINTILE,
            CUSTOM_AGE_BAND,
            0                                               as APPLICATION_COUNT,
            sum(1)                                          as ISSUED_COUNT
from        HWHC_HES_FACT
where       1=1
    and     ISSUE_YM >= &&p_min_ym
    and     ISSUE_YM <= &&p_max_ym
    and     CERTIFICATE_ISSUED_FLAG = 1
group by    ISSUE_FY,
            ISSUE_YM,
            SERVICE_AREA_NAME,
            CERTIFICATE_SUBTYPE,
            case
                when SERVICE_AREA != 'MAT'              
                    then CERTIFICATE_SUBTYPE
                when CERTIFICATE_DURATION_MONTHS < 12   
                    then '11 months or less'
                when CERTIFICATE_DURATION_MONTHS >= 20
                    then '20 to 22 months'
                when CERTIFICATE_DURATION_MONTHS >= 18
                    then '18 to 19 months'
                when CERTIFICATE_DURATION_MONTHS >= 16
                    then '16 to 17 months'
                when CERTIFICATE_DURATION_MONTHS >= 14
                    then '14 to 15 months'
                when CERTIFICATE_DURATION_MONTHS >= 12
                    then '12 to 13 months'
                else 'Not Available'
            end,
            case 
                when SERVICE_AREA = 'TAX'
                then COUNTRY
                else 'England'
            end,
            ICB_NAME,
            ICB,
            IMD_QUINTILE,
            CUSTOM_AGE_BAND
)
--select * from outcome_data;
-----SECTION END: DATA: IDENTIFY OUTCOMES DATA--------------------------------------------------------------------------------------------------------

,

-----SECTION START: DATA: COMBINE DATA----------------------------------------------------------------------------------------------------------------
--combine the application and issued datasets
base_data as
(
select * from application_data
union all
select * from outcome_data
)
--select * from base_data;
-----SECTION END: DATA: COMBINE DATA------------------------------------------------------------------------------------------------------------------

,

-----SECTION START: OUTPUT DATA: Overall Activity-----------------------------------------------------------------------------------------------------
--summarise all activity with no geographic breakdown
output_overall_data as
(
select      'OVERALL: All activity'     as GEO_CLASSIFICATION,
            'Not Available'             as GEO_CODE,
            FY,
            YM,
            YEAR_MONTH,
            SERVICE_AREA_NAME,
            CERTIFICATE_SUBTYPE,
            CERTIFICATE_DURATION,
            IMD_QUINTILE,
            CUSTOM_AGE_BAND,
            sum(APPLICATION_COUNT)      as APPLICATION_COUNT,
            sum(ISSUED_COUNT)           as ISSUED_COUNT
from        base_data
where       1=1
group by    FY,
            YM,
            YEAR_MONTH,
            SERVICE_AREA_NAME,
            CERTIFICATE_SUBTYPE,
            CERTIFICATE_DURATION,
            IMD_QUINTILE,
            CUSTOM_AGE_BAND
)
--select * from output_overall_data;
-----SECTION END: OUTPUT DATA: Overall Activity-------------------------------------------------------------------------------------------------------

,

-----SECTION START: OUTPUT DATA: Country Breakdown----------------------------------------------------------------------------------------------------
--summarise activity with geographic breakdown by country classification
--only applicable for TAX as other services are typically only England based
output_country_data as
(
select      'COUNTRY: '||COUNTRY        as GEO_CLASSIFICATION,
            'Not Available'             as GEO_CODE,
            FY,
            YM,
            YEAR_MONTH,
            SERVICE_AREA_NAME,
            CERTIFICATE_SUBTYPE,
            CERTIFICATE_DURATION,
            IMD_QUINTILE,
            CUSTOM_AGE_BAND,
            sum(APPLICATION_COUNT)      as APPLICATION_COUNT,
            sum(ISSUED_COUNT)           as ISSUED_COUNT
from        base_data
where       1=1
    and     SERVICE_AREA_NAME = 'NHS tax credit exemption certificate'
group by    'COUNTRY: '||COUNTRY,
            FY,
            YM,
            YEAR_MONTH,
            SERVICE_AREA_NAME,
            CERTIFICATE_SUBTYPE,
            CERTIFICATE_DURATION,
            IMD_QUINTILE,
            CUSTOM_AGE_BAND
)
--select * from output_country_data;
-----SECTION END: OUTPUT DATA: Country Breakdown------------------------------------------------------------------------------------------------------

,

-----SECTION START: OUTPUT DATA: ICB Breakdown--------------------------------------------------------------------------------------------------------
--summarise activity with geographic breakdown by ICB classification
output_icb_data as
(
select      'ICB: '||ICB_NAME           as GEO_CLASSIFICATION,
            ICB                         as GEO_CODE,
            FY,
            YM,
            YEAR_MONTH,
            SERVICE_AREA_NAME,
            CERTIFICATE_SUBTYPE,
            CERTIFICATE_DURATION,
            IMD_QUINTILE,
            CUSTOM_AGE_BAND,
            sum(APPLICATION_COUNT)      as APPLICATION_COUNT,
            sum(ISSUED_COUNT)           as ISSUED_COUNT
from        base_data
where       1=1
group by    'ICB: '||ICB_NAME,
            ICB,
            FY,
            YM,
            YEAR_MONTH,
            SERVICE_AREA_NAME,
            CERTIFICATE_SUBTYPE,
            CERTIFICATE_DURATION,
            IMD_QUINTILE,
            CUSTOM_AGE_BAND
)
--select * from output_icb_data;
-----SECTION END: OUTPUT DATA: ICB Breakdown----------------------------------------------------------------------------------------------------------

-----OUTPUT-------------------------------------------------------------------------------------------------------------------------------------------
            select * from output_overall_data
union all   select * from output_country_data
union all   select * from output_icb_data
;

---------------------SCRIPT END-----------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------