/*
NHSBSA Official Statistics: Health Exemption Services
PowerBI dataset creation: HRT PPC
Version 1.0


AMENDMENTS:
	2024-05-15  : Steven Buckley    : Initial script created

DESCRIPTION:
    Identify a base dataset for PowerBI based on NHS Low Income Scheme data.
    
    Aggregating data for HRT PPC to only the fields required for PowerBI.
    
    The different geography levels (Overall, Country and ICB will be captured as seperate records so no aggregation is required in PowerBI.
    
    For each month the number of applications and issued certificates will be calcualted (based on different attribute groups)

DEPENDENCIES:
	HRTPPC_FACT    :   Prebuilt dataset to application/certificate level for all HRT PPC cases 
*/
------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------SCRIPT START----------------------------------------------------------------------------------------------------------------------

create table HWHC_BI_HRTPPC compress for query high as
with 

-----SECTION START: DATA: IDENTIFY APPLICATION DATA---------------------------------------------------------------------------------------------------
--include all applications, aggregated by the application YM and FY
--duration is not relevant for HRT PPC, the subtype can be used
--for HRTPPC country should be assumed as ENGLAND as service not provided elsewhere
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
            'England'                                                   as COUNTRY,
            ICB_NAME,
            ICB,
            nvl(to_char(IMD_QUINTILE),'N/A')                            as IMD_QUINTILE,
            CUSTOM_AGE_BAND,
            sum(1)                                                      as APPLICATION_COUNT,
            0                                                           as ISSUED_COUNT
from        HRTPPC_FACT
where       1=1
    and     APPLICATION_YM >= &&p_min_ym
    and     APPLICATION_YM <= &&p_max_ym
group by    APPLICATION_FY,
            APPLICATION_YM,
            SERVICE_AREA_NAME,
            CERTIFICATE_SUBTYPE,
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
            CERTIFICATE_SUBTYPE                             as CERTIFICATE_DURATION,
            'England'                                       as COUNTRY,
            ICB_NAME,
            ICB,
            nvl(to_char(IMD_QUINTILE),'N/A')                as IMD_QUINTILE,
            CUSTOM_AGE_BAND,
            0                                               as APPLICATION_COUNT,
            sum(1)                                          as ISSUED_COUNT
from        HES_FACT
where       1=1
    and     ISSUE_YM >= &&p_min_ym
    and     ISSUE_YM <= &&p_max_ym
    and     CERTIFICATE_ISSUED_FLAG = 1
group by    ISSUE_FY,
            ISSUE_YM,
            SERVICE_AREA_NAME,
            CERTIFICATE_SUBTYPE,
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
            'N/A'                       as GEO_CODE,
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
output_country_data as
(
select      'COUNTRY: '||COUNTRY        as GEO_CLASSIFICATION,
            'N/A'                       as GEO_CODE,
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