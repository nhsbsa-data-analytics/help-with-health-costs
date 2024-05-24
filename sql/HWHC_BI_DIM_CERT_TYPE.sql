/*
NHSBSA Official Statistics: Health Exemption Services
PowerBI dataset creation: Certificate Type Dimension
Version 1.0

AMENDMENTS:
	2024-05-15  : Steven Buckley    : Initial script created

DESCRIPTION:
    Create a reference table containing a list of values from SERVICE_AREA_NAME and CERTIFICATE_SUBTYPE

DEPENDENCIES:
	HWHC_BI_AGG_FY_SERVICE_GEO_CERT :   Summarised dataset all activity split by service area, and certificate type
*/
------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------SCRIPT START----------------------------------------------------------------------------------------------------------------------

create table HWHC_BI_DIM_CERT_TYPE compress for query high as 
select  distinct
            SERVICE_AREA_NAME,
            CERTIFICATE_SUBTYPE
from        HWHC_BI_AGG_FY_SERVICE_GEO_CERT
where       1=1
;

---------------------SCRIPT END-----------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------