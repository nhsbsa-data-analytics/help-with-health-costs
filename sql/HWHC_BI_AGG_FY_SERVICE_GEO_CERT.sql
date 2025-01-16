/*
NHSBSA Official Statistics: Health Exemption Services
PowerBI dataset creation: Aggregated figures for trend reporting
Version 1.0

AMENDMENTS:
	2024-05-23  : Steven Buckley    : Initial script created

DESCRIPTION:
    Aggregate the dataset to support requirements for trend reporting
    
    Data should be aggregated by:
        GEO_CLASSIFICATION  :   geography classification 
        GEO_CODE            :   geography classification code
        FY                  :   financial year
        SERVICE_AREA_NAME   :   service area
        CERTIFICATE_SUBTYPE :   certificate subtype
    
    Figures should be reported, with disclosure controls applied, for:
        APPLICATION_COUNT   :   number of applications received
        ISSUED_COUNT        :   number of issued certificates

DEPENDENCIES:
	HWHC_BI_OUTPUT  :   Summarised dataset for HWHC
*/
------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------SCRIPT START----------------------------------------------------------------------------------------------------------------------

create table HWHC_BI_AGG_FY_SERVICE_GEO_CERT compress for query high as 

with 
certificate_split as
--split by certificate subtype
(
select      GEO_CLASSIFICATION,
            GEO_CODE,
            FY,
            SERVICE_AREA_NAME,
            CERTIFICATE_SUBTYPE,
            sum(APPLICATION_COUNT)  as APPLICATION_COUNT,
            sum(ISSUED_COUNT)       as ISSUED_COUNT
from        HWHC_BI_OUTPUT
group by    GEO_CLASSIFICATION,
            GEO_CODE,
            FY,
            SERVICE_AREA_NAME,
            CERTIFICATE_SUBTYPE
)
,
all_certificates as
--aggregate totals for services with certificate breakdowns
(
select      GEO_CLASSIFICATION,
            GEO_CODE,
            FY,
            SERVICE_AREA_NAME,
            'All'                   as CERTIFICATE_SUBTYPE,
            sum(APPLICATION_COUNT)  as APPLICATION_COUNT,
            sum(ISSUED_COUNT)       as ISSUED_COUNT
from        HWHC_BI_OUTPUT
where       1=1
    and     SERVICE_AREA_NAME in ('NHS Low Income Scheme', 'NHS Prescription Prepayment Certificate')
group by    GEO_CLASSIFICATION,
            GEO_CODE,
            FY,
            SERVICE_AREA_NAME
)
--,
--certificate_combinations as
----include totals for custom combinations of certificate subtype
--(
--select      GEO_CLASSIFICATION,
--            GEO_CODE,
--            FY,
--            SERVICE_AREA_NAME,
--            case 
--                when    SERVICE_AREA_NAME = 'NHS Low Income Scheme'
--                    and CERTIFICATE_SUBTYPE in ('HC2','HC3')
--                then    'HC2 + HC3'
--                else    'N/A'
--            end                     as CERTIFICATE_SUBTYPE,
--            sum(APPLICATION_COUNT)  as APPLICATION_COUNT,
--            sum(ISSUED_COUNT)       as ISSUED_COUNT
--from        HWHC_BI_OUTPUT
--where       1=1
--    and     (   SERVICE_AREA_NAME = 'NHS Low Income Scheme'
--            and CERTIFICATE_SUBTYPE in ('HC2','HC3')
--            )
--group by    GEO_CLASSIFICATION,
--            GEO_CODE,
--            FY,
--            SERVICE_AREA_NAME,
--            case 
--                when    SERVICE_AREA_NAME = 'NHS Low Income Scheme'
--                    and CERTIFICATE_SUBTYPE in ('HC2','HC3')
--                then    'HC2 + HC3'
--                else    'N/A'
--            end
--)
--apply suppression rules;
select      GEO_CLASSIFICATION,
            GEO_CODE,
            FY,
            SERVICE_AREA_NAME,
            CERTIFICATE_SUBTYPE,
            case
                when APPLICATION_COUNT between 1 and 4 
                then 5
                else round(APPLICATION_COUNT / 5) * 5
            end as APPLICATION_COUNT,
            case
                when ISSUED_COUNT between 1 and 4 
                then 5
                else round(ISSUED_COUNT / 5) * 5
            end as ISSUED_COUNT
from        (
                        select * from all_certificates
            union all   select * from certificate_split
--            union all   select * from certificate_combinations
            )
;

---------------------SCRIPT END-----------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------