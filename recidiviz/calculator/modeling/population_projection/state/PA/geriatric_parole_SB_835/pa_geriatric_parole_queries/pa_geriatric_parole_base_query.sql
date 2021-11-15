-- For general incarceration sessions ending in the last 3 years,
-- where age at end is >55,
-- that started as new admissions,
-- what % transition to supervision? what % transition to release?

-- select case when outflow_to_level_1 = 'PENDING_SUPERVISION' then 'SUPERVISION' else outflow_to_level_1 end, count(*)
-- from `recidiviz-staging.analyst_data_scratch_space.pa_geriatric_parole_base_query`
-- where age_end > 55
-- and end_date >= DATE_TRUNC(DATE_SUB(current_date, interval 3 year),year)
-- and outflow_to_level_1 != 'INTERNAL_UNKNOWN'
-- group by 1

-- Prison to Release - Baseline / Policy
select avg(session_length_months)
from `recidiviz-staging.analyst_data_scratch_space.pa_geriatric_parole_base_query`
where age_end > 55
and outflow_to_level_1  = "RELEASE"
and end_date >= DATE_TRUNC(DATE_SUB(current_date, interval 3 year),year)

-- select sum(case when min_expected_los is null then 1 else 0 end)/count(*), sum(case when max_expected_los is null then 1 else 0 end)/count(*)
-- from `recidiviz-staging.analyst_data_scratch_space.pa_geriatric_parole_base_query`
-- where age_at_elig_date > 55
-- where age_end > 55
-- and outflow_to_level_1  = "RELEASE"
-- and end_date >= DATE_TRUNC(DATE_SUB(current_date, interval 3 year),year)


-- select count(*)
-- from `recidiviz-staging.analyst_data_scratch_space.pa_geriatric_parole_base_query`
-- where outflow_to_level_1  = "SUPERVISION"
-- and end_date BETWEEN '2018-01-01' AND '2019-01-01'




-- and sentence_completion_date is null
-- and COALESCE(outflow_to_level_1,'OPEN_SESSION') != 'RELEASE'
-- and coalesce(end_date,'9999-01-01') >= DATE_TRUNC(DATE_SUB(current_date, interval 3 year),year)
-- group by 1
-- having count(*)>1
-- group by 1,2