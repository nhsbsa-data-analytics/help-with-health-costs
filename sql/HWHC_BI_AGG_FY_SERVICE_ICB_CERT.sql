/*
NHSBSA Official Statistics: Health Exemption Services
PowerBI dataset creation: Aggregated figures for ICB reporting
Version 1.0

AMENDMENTS:
	2024-05-23  : Steven Buckley    : Initial script created

DESCRIPTION:
    Aggregate the dataset to support requirements for ICB reporting
    
    Data should be aggregated by:
        GEO_CLASSIFICATION  :   geography classification 
        GEO_CODE            :   geography classification code
        FY                  :   financial year
        SERVICE_AREA_NAME   :   service area
        CERTIFICATE_SUBTYPE :   certificate subtype (Include an overall aggregation, split by type and specific combinations
    
    Figures should be reported, with disclosure controls applied, for:
        APPLICATION_COUNT   :   number of applications received
        ISSUED_COUNT        :   number of issued certificates

    Limited to only ICB data

DEPENDENCIES:
	HWHC_BI_OUTPUT  :   Summarised dataset for HWHC
*/
------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------SCRIPT START----------------------------------------------------------------------------------------------------------------------

create table HWHC_BI_AGG_FY_SERVICE_ICB_CERT compress for query high as 
with
certificate_split as
--split by certificate subtype
(
select      FY, 
            SERVICE_AREA_NAME,
            GEO_CLASSIFICATION,
            GEO_CODE,
            CERTIFICATE_SUBTYPE,
            sum(APPLICATION_COUNT)  as APPLICATION_COUNT,
            sum(ISSUED_COUNT)       as ISSUED_COUNT
from        HWHC_BI_OUTPUT
where       1=1
    and     GEO_CLASSIFICATION like 'ICB:%'
group by    FY, 
            SERVICE_AREA_NAME,
            GEO_CLASSIFICATION,
            GEO_CODE,
            CERTIFICATE_SUBTYPE
)
,
all_certificates as
--aggregate totals for services with certificate breakdowns
(
select      FY, 
            SERVICE_AREA_NAME,
            GEO_CLASSIFICATION,
            GEO_CODE,
            'All'                   as CERTIFICATE_SUBTYPE,
            sum(APPLICATION_COUNT)  as APPLICATION_COUNT,
            sum(ISSUED_COUNT)       as ISSUED_COUNT
from        HWHC_BI_OUTPUT
where       1=1
    and     GEO_CLASSIFICATION like 'ICB:%'
    and     SERVICE_AREA_NAME in ('NHS Low Income Scheme', 'NHS Prescription Prepayment Certificate')
group by    FY, 
            SERVICE_AREA_NAME,
            GEO_CLASSIFICATION,
            GEO_CODE
)
,
certificate_combinations as
--include totals for custom combinations of certificate subtype
(
select      FY, 
            SERVICE_AREA_NAME,
            GEO_CLASSIFICATION,
            GEO_CODE,
            case 
                when    SERVICE_AREA_NAME = 'NHS Low Income Scheme'
                    and CERTIFICATE_SUBTYPE in ('HC2','HC3')
                then    'HC2 + HC3'
                else    'N/A'
            end                     as CERTIFICATE_SUBTYPE,
            sum(APPLICATION_COUNT)  as APPLICATION_COUNT,
            sum(ISSUED_COUNT)       as ISSUED_COUNT
from        HWHC_BI_OUTPUT
where       1=1
    and     GEO_CLASSIFICATION like 'ICB:%'
    and     (   SERVICE_AREA_NAME = 'NHS Low Income Scheme'
            and CERTIFICATE_SUBTYPE in ('HC2','HC3')
            )
group by    FY, 
            SERVICE_AREA_NAME,
            GEO_CLASSIFICATION,
            GEO_CODE,
            case 
                when    SERVICE_AREA_NAME = 'NHS Low Income Scheme'
                    and CERTIFICATE_SUBTYPE in ('HC2','HC3')
                then    'HC2 + HC3'
                else    'N/A'
            end
)
--apply suppression rules;
select      base.FY, 
            base.SERVICE_AREA_NAME,
            base.GEO_CLASSIFICATION,
            base.GEO_CODE,
            base.CERTIFICATE_SUBTYPE,
            case
                when base.APPLICATION_COUNT between 1 and 4 
                then 5
                else round(base.APPLICATION_COUNT / 5) * 5
            end                                                         as APPLICATION_COUNT,
            case
                when base.ISSUED_COUNT between 1 and 4 
                then 5
                else round(base.ISSUED_COUNT / 5) * 5
            end                                                         as ISSUED_COUNT,
            pop.POP_TYPE,
            case
                when pop.BASE_POPULATION between 1 and 4 
                then 5
                else round(pop.BASE_POPULATION / 5) * 5
            end                                                         as BASE_POPULATION,
            round(base.APPLICATION_COUNT / pop.BASE_POPULATION * 10000) as APPLICATIONS_PER_POPULATION,
            round(base.ISSUED_COUNT / pop.BASE_POPULATION * 10000)      as ISSUED_PER_POPULATION            
from        (
                        select * from all_certificates
            union all   select * from certificate_split
            union all   select * from certificate_combinations
            )                           base
left join   HWHC_BI_ICB_POPULATION      pop on  base.FY                 =   pop.FINANCIAL_YEAR
                                            and base.SERVICE_AREA_NAME  =   pop.SERVICE_AREA_NAME
                                            and base.GEO_CODE           =   pop.GEO_CODE
;      
---------------------SCRIPT END-----------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------