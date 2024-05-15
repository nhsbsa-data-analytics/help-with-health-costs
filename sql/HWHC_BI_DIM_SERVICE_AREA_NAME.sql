/*
NHSBSA Official Statistics: Health Exemption Services
PowerBI dataset creation: Service Area Dimension
Version 1.0

AMENDMENTS:
	2024-05-15  : Steven Buckley    : Initial script created

DESCRIPTION:
    Create a reference table containing a list of values from SERVICE_AREA_NAME

DEPENDENCIES:
	HWHC_BI_OUTPUT :   Summarised dataset all activity split by geography, IMD , age and duration
*/
------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------SCRIPT START----------------------------------------------------------------------------------------------------------------------

create table HWHC_BI_DIM_SERVICE_AREA_NAME compress for query high as 
select  distinct
            SERVICE_AREA_NAME
from        HWHC_BI_OUTPUT
where       1=1
;

---------------------SCRIPT END-----------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------