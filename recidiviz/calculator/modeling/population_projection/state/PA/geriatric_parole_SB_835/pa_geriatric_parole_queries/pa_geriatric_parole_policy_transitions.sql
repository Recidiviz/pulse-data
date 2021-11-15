-- Takes the base query, counts population thats 55 or older, who are past their date of eligibility
-- minimum_elig_date is the earlier of 2 dates: date when half the minimum sentence is served and date when 25 years are served
WITH prison_pop AS (
        select
                population_date,
                'prison' AS compartment,
                count(*) as total_population
        from (
                select *,
                        CAST(FLOOR(DATE_DIFF(population_date, birthdate, DAY) / 365.25) AS INT64) AS age,
                from `recidiviz-staging.analyst_data_scratch_space.pa_geriatric_parole_base_query`,
                UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(CURRENT_DATE, INTERVAL 10 YEAR), CURRENT_DATE, INTERVAL 1 DAY)) AS population_date
                WHERE population_date BETWEEN start_date AND COALESCE(end_date, '9999-01-01')
        )
        where extract(day from population_date) = 1
        and minimum_elig_date < population_date
        and age > 55
        group by 1
        order by 1
), parole_pop AS (
        select
                population_date,
                'parole' AS compartment,
                count(*) as total_population
        from (
                select *
                FROM `recidiviz-staging.analyst_data.compartment_sessions_materialized`,
                UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(CURRENT_DATE, INTERVAL 10 YEAR), CURRENT_DATE, INTERVAL 1 DAY)) AS population_date
                WHERE population_date BETWEEN start_date AND COALESCE(end_date, '9999-01-01')
                AND state_code = 'US_PA'
                AND compartment_level_2 IN ('PAROLE','DUAL')
        )
        where extract(day from population_date) = 1
        and age_start > 55
        group by 1
        order by 1

)
select * from prison_pop union all select * from parole_pop order by compartment, population_date