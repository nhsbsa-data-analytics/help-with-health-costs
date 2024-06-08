/*
NHSBSA Official Statistics: Health Exemption Services
PowerBI dataset creation: Geography Dimension
Version 1.0

AMENDMENTS:
	2024-05-15  : Steven Buckley    : Initial script created

DESCRIPTION:
    Create a reference table containing a list of values from GEO_CLASSIFICATION

DEPENDENCIES:
	HWHC_BI_OUTPUT :   Summarised dataset all activity split by geography, IMD , age and duration
*/
------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------SCRIPT START----------------------------------------------------------------------------------------------------------------------

create table HWHC_BI_DIM_GEO_CLASSIFICATION compress for query high as 
select  distinct
            GEO_CLASSIFICATION,
            case
                when GEO_CLASSIFICATION like 'OVERALL:%'            then '1:'||GEO_CLASSIFICATION
                when GEO_CLASSIFICATION like 'COUNTRY:%'            then '2:'||GEO_CLASSIFICATION
                when GEO_CLASSIFICATION like 'ICB: NHS%'            then '3:'||GEO_CLASSIFICATION
                when GEO_CLASSIFICATION like 'ICB: Not Available'   then '4:'||GEO_CLASSIFICATION
                                                                    else '9'
            end as SORTORDER
from        HWHC_BI_OUTPUT
where       1=1
;

---------------------SCRIPT END-----------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------