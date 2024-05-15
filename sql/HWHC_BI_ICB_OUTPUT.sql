/*
NHSBSA Official Statistics: Health Exemption Services
PowerBI dataset creation: ICB Summary
Version 1.0

AMENDMENTS:
	2024-05-15  : Steven Buckley    : Initial script created

DESCRIPTION:
    Aggregate the datasets to get a summary at ICB level
    This will be used as a child table to allow a ICB to be highlighted on a visualisation

DEPENDENCIES:
	HWHC_BI_OUTPUT :   Summarised dataset all activity split by geography, IMD , age and duration
*/
------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------SCRIPT START----------------------------------------------------------------------------------------------------------------------

create table HWHC_BI_ICB_OUTPUT compress for query high as 
select      FY, 
            YM,
            YEAR_MONTH,
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
            YM,
            YEAR_MONTH,
            SERVICE_AREA_NAME,
            GEO_CLASSIFICATION,
            GEO_CODE,
            CERTIFICATE_SUBTYPE
;

---------------------SCRIPT END-----------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------