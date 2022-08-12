select avg(percent_min_served), avg(percent_max_served)
from `recidiviz-staging.analyst_data_scratch_space.pa_geriatric_parole_base_query`
where age_at_sentence_completion >= 55;

-- select avg(percent_min_served_before_session_end), avg(percent_max_served_before_session_end)
-- from `recidiviz-staging.analyst_data_scratch_space.pa_geriatric_parole_base_query`
-- where age_end >= 55
-- and sentence_start_date >='2002-01-01'
-- and COALESCE(inflow_from_level_1,'PRETRIAL') in ('LIBERTY','PRETRIAL')
-- and coalesce(outflow_to_level_1,'OPEN') in ('SUPERVISION')