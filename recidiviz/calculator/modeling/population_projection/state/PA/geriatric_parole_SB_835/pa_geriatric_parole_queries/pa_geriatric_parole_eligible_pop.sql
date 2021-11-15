WITH flows_into_prison as (
    select date_trunc(start_date, month) as month_year,
            CASE WHEN inflow_from_level_1 = 'RELEASE' THEN 'pretrial'
                WHEN inflow_from_level_1 = 'PENDING_CUSTODY' THEN 'supervision'
                ELSE coalesce(lower(inflow_from_level_1), 'pretrial') END as compartment,
            'prison' as outflow_to,
        count(*) as total_population
    from `recidiviz-staging.analyst_data_scratch_space.pa_geriatric_parole_base_query`
    where (age_at_elig_date > 55 or (age_at_elig_date*12 + min_expected_los_after_eligibility > 55*12))
    and COALESCE(inflow_from_level_1,'PRETRIAL') in ('RELEASE','PRETRIAL')
    and start_date between DATE_TRUNC(DATE_SUB(current_date, interval 10 year),year)
        and LAST_DAY(DATE_SUB(current_date, interval 1 month), MONTH)
    group by 1,2,3
    order by 1,2,3
)
-- flows_from_prison AS (
--     select date_trunc(end_date, month) as month_year,
--             'prison' as compartment,
--             CASE WHEN outflow_to_level_1 = 'RELEASE' THEN 'release'
--                 WHEN outflow_to_level_1 = 'PENDING_CUSTODY' THEN 'supervision'
--                 ELSE lower(outflow_to_level_1) END as outflow_to,
--         count(*) as total_population
--     from `recidiviz-staging.analyst_data_scratch_space.pa_geriatric_parole_base_query`
--     where age_at_elig_date >= 55
--     and COALESCE(outflow_to_level_1,'PRETRIAL') not in ('INTERNAL_UNKNOWN','PENDING_SUPERVISION')
--     and end_date > DATE_TRUNC(DATE_SUB(current_date, interval 3 year),year)
--     and end_date is not null
--     group by 1,2,3
--     order by 1,2,3
-- )
select *
from flows_into_prison
-- union all
-- select *
-- from flows_from_prison
order by month_year, compartment, outflow_to