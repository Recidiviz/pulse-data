-- Prison to Supervision - Policy 56% grant rate
with prison_to_supervision_policy_duration as (
    select *,
        CASE WHEN age_current<=55 THEN GREATEST(55*12 - age_current*12,time_till_eligibility)
             WHEN age_current>55 and time_till_eligibility <= 0 then 0
             WHEN age_current>55 then time_till_eligibility
             END as compartment_duration_prison_to_supervision,
    --   count(*) as total_population
    from `recidiviz-staging.analyst_data_scratch_space.pa_geriatric_parole_base_query`
    where (age_at_elig_date > 55 or (age_at_elig_date*12 + min_expected_los_after_eligibility > 55*12))
    and minimum_elig_date < DATE_ADD(current_date(), interval 10 year)
    and sentence_completion_date is null
), prison_to_supervision_policy as (
    select 'prison' as compartment,
        'parole' as outflow_to,
        compartment_duration_prison_to_supervision as compartment_duration,
        count(*) * 0.56 as total_population
    from prison_to_supervision_policy_duration
    group by 1,2,3
),
    supervision_to_release_policy as (
    -- Supervision to Release - Policy 56% of pop
    select  'parole' as compartment,
            'release' as outflow_to,
            FLOOR(min_expected_los_adj - compartment_duration_prison_to_supervision) as compartment_duration,
            -- *
        count(*) * 0.56 as total_population
    from prison_to_supervision_policy_duration
    where min_expected_los_adj is not null
    -- where (age_at_elig_date > 55 or (age_at_elig_date*12 + min_expected_los_after_eligibility > 55*12))
    -- and minimum_elig_date < DATE_ADD(current_date(), interval 10 year)
    -- and sentence_completion_date is null
    group by 1,2,3
),
prison_to_release AS (
    -- Prison to to Release - Case where parole is not granted (not policy eligible) 44% of pop
        select  'prison' as compartment,
            'release' as outflow_to,
            FLOOR(min_expected_los_adj) as compartment_duration,
            -- *
        count(*) * 0.44 as total_population
    from prison_to_supervision_policy_duration
    where min_expected_los_adj is not null
    group by 1,2,3
)


select * from prison_to_supervision_policy
union all
select * from supervision_to_release_policy
union all
select * from prison_to_release
union all (
    SELECT
        "prison" AS compartment,
        "release" AS outflow_to,
        FLOOR(min_expected_los_adj) as compartment_duration,
        count(*) AS total_population
    FROM `recidiviz-staging.analyst_data_scratch_space.pa_geriatric_parole_base_query`
    WHERE minimum_elig_date < DATE_ADD(current_date(), interval 10 year)
    and sentence_completion_date is null
    GROUP BY 1, 2, 3
);
