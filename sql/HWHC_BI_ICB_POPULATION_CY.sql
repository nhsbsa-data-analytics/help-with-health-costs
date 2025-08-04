/*
NHSBSA Official Statistics: Health Exemption Services
Base Population Figures
Version 1.0


AMENDMENTS:
	2024-05-15  : Steven Buckley    : Initial script created
    2024-06-13  : Steven Buckley    : Added SERVICE_AREA to script to map to other tables
    2025-06-09  : Grace Libby       : Created calendar year script based on HWHC_BI_ICB_POPULATION.sql financial year script
    

DESCRIPTION:
    Identify the relevant base population figures for each service area:
        NHS Low Income Scheme   : ONS population estimates for people aged 16+
        NHS Tax Credits         : ONS population estimates for people aged 16+
        Maternity Exemption     : ONS population estimates for females aged 15-45
        Medical Exemption       : Estimated patient counts for people aged 16-59 receiving NHS prescribing
        PPC                     : Estimated patient counts for people aged 16-59 receiving NHS prescribing
        HRT PPC                 : Estimated patient counts for people aged 16-59 receiving NHS prescribing of HRT qualifying medication


DEPENDENCIES:
    DIM.YEAR_MONTH_DIM      :   "Dimension" table containing time period classifications
    
    HWHC_PX_PAT_CY_ICB      :   Reference table produced from scripts to aggregate patient counts from prescription data
                                Includes patient counts aggregated by calendar year and ICB
    
    HWHC_ONS_ICB_POPULATION :   Reference table sourced from ONS published population statistics
                                Includes population figures for ICB (GEOGRAPHY_TYPE = 'ICB2023')
                                Include population aggregated to relevant age bands
                                Population is mid-calendar-year estimates 
*/
------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------SCRIPT START----------------------------------------------------------------------------------------------------------------------

create table HWHC_BI_ICB_POPULATION_CY compress for query high as
with
-----SECTION START: TIME PERIOD MAPPING---------------------------------------------------------------------------------------------------------------
--create a mapping between financial year and ONS population year
--for a financial year the closet available year should be applied (e.g. 2020 for 2020/2021)
time_period_mapping as
(
select      CALENDAR_YEAR,
            POPULATION_YEAR
from        (
            select      cy.CALENDAR_YEAR,
                        py.POPULATION_YEAR,
                        rank() over (
                                    partition by    cy.CALENDAR_YEAR
                                    order by        py.POPULATION_YEAR desc
                                    )   as RNK
            from        (
                        select  distinct
                                    POPULATION_YEAR
                        from        HWHC_ONS_ICB_POPULATION pop
                        where       1=1
                            and     GEOGRAPHY_TYPE = 'ICB2023'
                        )   py,
                        (
                        select      CALENDAR_YEAR
                        from        DIM.YEAR_MONTH_DIM
                        where       1=1
                            and     YEAR_MONTH between &&p_min_ym and &&p_max_ym
                        group by    CALENDAR_YEAR
                        )   cy
            where       1=1
                and     cy.CALENDAR_YEAR >= py.POPULATION_YEAR
            )
where       1=1
    and     RNK = 1
)
--select * from time_period_mapping;
-----SECTION END: TIME PERIOD MAPPING-----------------------------------------------------------------------------------------------------------------

,

-----SECTION START: BASE POPULATION: NHS Low Income Scheme--------------------------------------------------------------------------------------------
--For NHS Low Income Scheme the population is based on ONS population estimates for people aged 16+
lis_population as
(
select      tpm.CALENDAR_YEAR,                                                
            'LIS'                                                           as SERVICE_AREA,
            'NHS Low Income Scheme'                                         as SERVICE_AREA_NAME,
            'ONS mid-year estimate '||pop.POPULATION_YEAR||' (aged 16+)'    as POP_TYPE,
            pop.GEOGRAPHY_ONS_CODE                                          as GEO_CODE,
            pop.POPULATION_16PLUS                                           as BASE_POPULATION
from        HWHC_ONS_ICB_POPULATION pop
inner join  time_period_mapping     tpm on  pop.POPULATION_YEAR = tpm.POPULATION_YEAR
where       1=1
    and     pop.GEOGRAPHY_TYPE = 'ICB2023'
)
--select * from lis_population;
-----SECTION END: BASE POPULATION: NHS Low Income Scheme----------------------------------------------------------------------------------------------

,

-----SECTION START: BASE POPULATION: Maternity Exemption----------------------------------------------------------------------------------------------
--For Maternity Exemption the population is based on ONS population estimates for females aged 15-45
matex_population as
(
select      tpm.CALENDAR_YEAR,
            'MAT'                                                                   as SERVICE_AREA,
            'Maternity exemption certificate'                                       as SERVICE_AREA_NAME,
            'ONS mid-year estimate '||pop.POPULATION_YEAR||' (females aged 15-45)'  as POP_TYPE,
            pop.GEOGRAPHY_ONS_CODE                                                  as GEO_CODE,
            pop.FEMALE_POPULATION_15_TO_45                                          as BASE_POPULATION
from        HWHC_ONS_ICB_POPULATION pop
inner join  time_period_mapping     tpm on  pop.POPULATION_YEAR = tpm.POPULATION_YEAR
where       1=1
    and     pop.GEOGRAPHY_TYPE = 'ICB2023'
)
--select * from matex_population;
-----SECTION END: BASE POPULATION: NHS Low Income Scheme----------------------------------------------------------------------------------------------

,

-----SECTION START: BASE POPULATION: NHS tax credit exemption-----------------------------------------------------------------------------------------
--For NHS tax credit exemption the population is based on ONS population estimates for people aged 16+
tax_population as
(
select      tpm.CALENDAR_YEAR,
            'TAX'                                                           as SERVICE_AREA,
            'NHS tax credit exemption certificate'                          as SERVICE_AREA_NAME,
            'ONS mid-year estimate '||pop.POPULATION_YEAR||' (aged 16+)'    as POP_TYPE,
            pop.GEOGRAPHY_ONS_CODE                                          as GEO_CODE,
            pop.POPULATION_16PLUS                                           as BASE_POPULATION
from        HWHC_ONS_ICB_POPULATION pop
inner join  time_period_mapping     tpm on  pop.POPULATION_YEAR = tpm.POPULATION_YEAR
where       1=1
    and     pop.GEOGRAPHY_TYPE = 'ICB2023'
)
--select * from tax_population;
-----SECTION END: BASE POPULATION: NHS tax credit exemption-------------------------------------------------------------------------------------------

,

-----SECTION START: BASE POPULATION: Medical exemption------------------------------------------------------------------------------------------------
--For Medical Exemption the population is based on patient counts from prescription data where the patient is 16-59 (not age exempt)
medex_population as
(
select      tpm.CALENDAR_YEAR,
            'MED'                                                                                   as SERVICE_AREA,
            'Medical exemption certificate'                                                         as SERVICE_AREA_NAME,
            'Estimated patients (aged 16-59) receiving NHS prescribing ('||tpm.CALENDAR_YEAR||')'  as POP_TYPE,
            pop.ICB                                                                                 as GEO_CODE,
            pop.PATIENT_COUNT_16_59                                                                 as BASE_POPULATION
from        time_period_mapping     tpm
inner join  HWHC_PX_PAT_CY_ICB      pop on  tpm.CALENDAR_YEAR = pop.CALENDAR_YEAR
where       1=1
    and     pop.ICB != 'Not Available'

)
--select * from medex_population;
-----SECTION END: BASE POPULATION: Medical exemption--------------------------------------------------------------------------------------------------

,

-----SECTION START: BASE POPULATION: PPC--------------------------------------------------------------------------------------------------------------
--For PPC the population is based on patient counts from prescription data where the patient is 16-59 (not age exempt)
ppc_population as
(
select      tpm.CALENDAR_YEAR,
            'PPC'                                                                                   as SERVICE_AREA,
            'NHS Prescription Prepayment Certificate'                                               as SERVICE_AREA_NAME,
            'Estimated patients (aged 16-59) receiving NHS prescribing ('||tpm.CALENDAR_YEAR||')'  as POP_TYPE,
            pop.ICB                                                                                 as GEO_CODE,
            pop.PATIENT_COUNT_16_59                                                                 as BASE_POPULATION
from        time_period_mapping     tpm
inner join  HWHC_PX_PAT_CY_ICB      pop on  tpm.CALENDAR_YEAR = pop.CALENDAR_YEAR
where       1=1
    and     pop.ICB != 'Not Available'

)
--select * from ppc_population;
-----SECTION END: BASE POPULATION: PPC----------------------------------------------------------------------------------------------------------------

,

-----SECTION START: BASE POPULATION: HRT PPC----------------------------------------------------------------------------------------------------------
--For PPC the population is based on patient counts from prescription data where the patient is 16-59 (not age exempt)
hrtppc_population as
(
select      tpm.CALENDAR_YEAR,
            'HRTPPC'                                                                                                                as SERVICE_AREA,
            'NHS Hormone Replacement Therapy Prescription Prepayment Certificate (HRT PPC)'                                         as SERVICE_AREA_NAME,
            'Estimated patients (aged 16-59) receiving NHS prescribing of HRT PPC eligible medicines ('||tpm.CALENDAR_YEAR||')'    as POP_TYPE,
            pop.ICB                                                                                                                 as GEO_CODE,
            pop.HRT_PATIENT_COUNT_16_59                                                                                             as BASE_POPULATION
from        time_period_mapping     tpm
inner join  HWHC_PX_PAT_CY_ICB      pop on  tpm.CALENDAR_YEAR = pop.CALENDAR_YEAR
where       1=1
    and     pop.ICB != 'Not Available'

)
--select * from hrtppc_population;
-----SECTION END: BASE POPULATION: HRT PPC------------------------------------------------------------------------------------------------------------


-----OUTPUT-------------------------------------------------------------------------------------------------------------------------------------------
            select * from lis_population
union all   select * from matex_population
union all   select * from tax_population
union all   select * from medex_population
union all   select * from ppc_population
union all   select * from hrtppc_population
;

---------------------SCRIPT END-----------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------