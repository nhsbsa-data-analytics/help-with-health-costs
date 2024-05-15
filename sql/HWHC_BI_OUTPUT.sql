/*
NHSBSA Official Statistics: Health Exemption Services
PowerBI dataset creation: Combine service area datasets
Version 1.0

AMENDMENTS:
	2024-05-15  : Steven Buckley    : Initial script created

DESCRIPTION:
    Aggregate the datasets for each service area into a single data table
    
    SERVICE_AREA_NAME has been trim to remove whitespace that was appearing during the union

DEPENDENCIES:
	HWHC_BI_LIS     :   Summarised datase for NHS Low Income Scheme
    HWHC_BI_HES     :   Summarised datase for HES services from the PPC system (MATEX, MEDEX, PPC, Tax Credits)
    HWHC_BI_HRTPPC  :   Summarised datase for HRT PPC
*/
------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------SCRIPT START----------------------------------------------------------------------------------------------------------------------

create table HWHC_BI_OUTPUT compress for query high as 

with
combine_data as
(
            select * from HWHC_BI_LIS
union all   select * from HWHC_BI_HES
union all   select * from HWHC_BI_HRTPPC
)
select      GEO_CLASSIFICATION,
            GEO_CODE,
            FY,
            YM,
            YEAR_MONTH,
            trim(SERVICE_AREA_NAME)     as SERVICE_AREA_NAME,
            CERTIFICATE_SUBTYPE,
            CERTIFICATE_DURATION,
            IMD_QUINTILE,
            CUSTOM_AGE_BAND,
            APPLICATION_COUNT,
            ISSUED_COUNT
from        combine_data
;

---------------------SCRIPT END-----------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------