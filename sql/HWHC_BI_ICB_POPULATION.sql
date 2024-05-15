/*
NHSBSA Official Statistics: Health Exemption Services
Base Population Figures
Version 1.0


AMENDMENTS:
	2024-05-15  : Steven Buckley    : Initial script created
    

DESCRIPTION:
    Identify the relevant base population figures for each service area:
        NHS Low Income Scheme   : ONS population estimates for people aged 16+
        NHS Tax Credits         : ONS population estimates for people aged 16+
        Maternity Exemption     : ONS population estimates for females aged 15-45
        Medical Exemption       : Estimated patient counts for people aged 16-59 receiving NHS prescribing
        PPC                     : Estimated patient counts for people aged 16-59 receiving NHS prescribing
        HRT PPC                 : Estimated patient counts for people aged 16-59 receiving NHS prescribing of HRT qualifying medication


DEPENDENCIES:
    DIM.YEAR_MONTH_DIM  :   "Dimension" table containing time period classifications
    
    HWHC_PX_PAT_FY_ICB  :   Reference table produced from scripts to aggregate patient counts from prescription data
                            Includes patient counts aggregated by financial year and ICB
    
    ONS_POPULATION      :   Reference table sourced from ONS published population statistics
                            Includes population figures for ICB (GEOGRAPHY_TYPE = 'ICB2023')
                            Include population split by single year of age and gender
                            Population is mid-calendar-year estimates so needs to be aligned with financial years
                            
    
*/
------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------SCRIPT START----------------------------------------------------------------------------------------------------------------------

create table HWHC_BI_ICB_POPULATION compress for query high as
with
-----SECTION START: TIME PERIOD MAPPING---------------------------------------------------------------------------------------------------------------
--create a mapping between financial year and ONS population year
--for a financial year the closet available year should be applied (e.g. 2020 for 2020/2021)
time_period_mapping as
(
select      FINANCIAL_YEAR,
            POPULATION_YEAR
from        (
            select      fy.FINANCIAL_YEAR,
                        py.POPULATION_YEAR,
                        rank() over (
                                    partition by    fy.FINANCIAL_YEAR
                                    order by        py.POPULATION_YEAR desc
                                    )   as RNK
            from        (
                        select  distinct
                                    POPULATION_YEAR
                        from        ONS_POPULATION pop
                        where       1=1
                            and     GEOGRAPHY_TYPE = 'ICB2023'
                        )   py,
                        (
                        select      FINANCIAL_YEAR,
                                    min(CALENDAR_YEAR)  as CALENDAR_YEAR
                        from        DIM.YEAR_MONTH_DIM
                        where       1=1
                            and     YEAR_MONTH between &&p_min_ym and &&p_max_ym
                        group by    FINANCIAL_YEAR
                        )   fy
            where       1=1
                and     fy.CALENDAR_YEAR >= py.POPULATION_YEAR
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
select      tpm.FINANCIAL_YEAR,
            'NHS Low Income Scheme'                                         as SERVICE_AREA_NAME,
            'ONS mid-year estimate '||pop.POPULATION_YEAR||' (aged 16+)'    as POP_TYPE,
            pop.GEOGRAPHY_ONS_CODE                                          as GEO_CODE,
            sum(pop.TOTAL_POPULATION)                                       as BASE_POPULATION
from        ONS_POPULATION          pop
inner join  time_period_mapping     tpm on  pop.POPULATION_YEAR = tpm.POPULATION_YEAR
where       1=1
    and     pop.GEOGRAPHY_TYPE = 'ICB2023'
    and     pop.AGE >= 16
group by    tpm.FINANCIAL_YEAR,
            pop.POPULATION_YEAR,
            pop.GEOGRAPHY_ONS_CODE
)
--select * from lis_population;
-----SECTION END: BASE POPULATION: NHS Low Income Scheme----------------------------------------------------------------------------------------------

,

-----SECTION START: BASE POPULATION: Maternity Exemption----------------------------------------------------------------------------------------------
--For Maternity Exemption the population is based on ONS population estimates for females aged 15-45
matex_population as
(
select      tpm.FINANCIAL_YEAR,
            'Maternity exemption certificate'                                       as SERVICE_AREA_NAME,
            'ONS mid-year estimate '||pop.POPULATION_YEAR||' (females aged 15-45)'  as POP_TYPE,
            pop.GEOGRAPHY_ONS_CODE                                                  as GEO_CODE,
            sum(pop.FEMALE_POPULATION)                                              as BASE_POPULATION
from        ONS_POPULATION          pop
inner join  time_period_mapping     tpm on  pop.POPULATION_YEAR = tpm.POPULATION_YEAR
where       1=1
    and     pop.GEOGRAPHY_TYPE = 'ICB2023'
    and     pop.AGE between 15 and 45
group by    tpm.FINANCIAL_YEAR,
            pop.POPULATION_YEAR,
            pop.GEOGRAPHY_ONS_CODE
)
--select * from matex_population;
-----SECTION END: BASE POPULATION: NHS Low Income Scheme----------------------------------------------------------------------------------------------

,

-----SECTION START: BASE POPULATION: NHS tax credit exemption-----------------------------------------------------------------------------------------
--For Maternity Exemption the population is based on ONS population estimates for females aged 15-45
tax_population as
(
select      tpm.FINANCIAL_YEAR,
            'NHS tax credit exemption certificate'                          as SERVICE_AREA_NAME,
            'ONS mid-year estimate '||pop.POPULATION_YEAR||' (aged 16+)'    as POP_TYPE,
            pop.GEOGRAPHY_ONS_CODE                                          as GEO_CODE,
            sum(pop.TOTAL_POPULATION)                                       as BASE_POPULATION
from        ONS_POPULATION          pop
inner join  time_period_mapping     tpm on  pop.POPULATION_YEAR = tpm.POPULATION_YEAR
where       1=1
    and     pop.GEOGRAPHY_TYPE = 'ICB2023'
    and     pop.AGE >= 16
group by    tpm.FINANCIAL_YEAR,
            pop.POPULATION_YEAR,
            pop.GEOGRAPHY_ONS_CODE
)
--select * from tax_population;
-----SECTION END: BASE POPULATION: NHS tax credit exemption-------------------------------------------------------------------------------------------

,

-----SECTION START: BASE POPULATION: Medical exemption------------------------------------------------------------------------------------------------
--For Medical Exemption the population is based on patient counts from prescription data where the patient is 16-59 (not age exempt)
medex_population as
(
select      tpm.FINANCIAL_YEAR,
            'Medical exemption certificate'                             as SERVICE_AREA_NAME,
            'Estimated patients (aged 16-59) receiving NHS prescribing' as POP_TYPE,
            pop.ICB                                                     as GEO_CODE,
            pop.PATIENT_COUNT_16_59                                     as BASE_POPULATION
from        time_period_mapping     tpm
inner join  HWHC_PX_PAT_FY_ICB      pop on  tpm.FINANCIAL_YEAR = pop.FINANCIAL_YEAR
where       1=1

)
--select * from medex_population;
-----SECTION END: BASE POPULATION: Medical exemption--------------------------------------------------------------------------------------------------

,

-----SECTION START: BASE POPULATION: PPC--------------------------------------------------------------------------------------------------------------
--For PPC the population is based on patient counts from prescription data where the patient is 16-59 (not age exempt)
ppc_population as
(
select      tpm.FINANCIAL_YEAR,
            'NHS Prescription Prepayment Certificate'                   as SERVICE_AREA_NAME,
            'Estimated patients (aged 16-59) receiving NHS prescribing' as POP_TYPE,
            pop.ICB                                                     as GEO_CODE,
            pop.PATIENT_COUNT_16_59                                     as BASE_POPULATION
from        time_period_mapping     tpm
inner join  HWHC_PX_PAT_FY_ICB      pop on  tpm.FINANCIAL_YEAR = pop.FINANCIAL_YEAR
where       1=1

)
--select * from ppc_population;
-----SECTION END: BASE POPULATION: PPC----------------------------------------------------------------------------------------------------------------

,

-----SECTION START: BASE POPULATION: HRT PPC----------------------------------------------------------------------------------------------------------
--For PPC the population is based on patient counts from prescription data where the patient is 16-59 (not age exempt)
hrtppc_population as
(
select      tpm.FINANCIAL_YEAR,
            'NHS Hormone Replacement Therapy Prescription Prepayment Certificate (HRT PPC)'             as SERVICE_AREA_NAME,
            'Estimated patients (aged 16-59) receiving NHS prescribing of HRT PPC eligible medicines'   as POP_TYPE,
            pop.ICB                                                                                     as GEO_CODE,
            pop.HRT_PATIENT_COUNT_16_59                                                                 as BASE_POPULATION
from        time_period_mapping     tpm
inner join  HWHC_PX_PAT_FY_ICB      pop on  tpm.FINANCIAL_YEAR = pop.FINANCIAL_YEAR
where       1=1

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