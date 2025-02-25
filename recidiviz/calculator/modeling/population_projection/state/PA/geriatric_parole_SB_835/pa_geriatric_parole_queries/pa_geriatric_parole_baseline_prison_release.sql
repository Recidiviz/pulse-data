-- Prison to Supervision - Baseline
select  'prison' as compartment,
        'parole' as outflow_to,
        FLOOR(min_expected_los_adj * 0.72) as compartment_duration,
      count(*) as total_population
from `recidiviz-staging.analyst_data_scratch_space.pa_geriatric_parole_base_query`
 where (age_at_elig_date > 55 or (age_at_elig_date*12 + min_expected_los_after_eligibility > 55*12))
    and minimum_elig_date < DATE_ADD(CURRENT_DATE('US/Eastern'), interval 10 year)
    and sentence_completion_date is null
    and min_expected_los_adj is not null
group by 1,2,3
order by 4 desc;