-- Start by limiting to general incarceration in PA, and bringing compartment_sentences
-- Note, right now, we can't get projected_end_date from compartment_sentences. Bringing it in to help match sentences to raw data
WITH us_pa_caseload AS (
  SELECT
    cs.*,
    sent.sentence_date_imposed,
    sent.sentence_start_date,
    sent.sentence_completion_date,
  FROM `sessions.compartment_sessions_materialized` cs
  -- Sandbox dataset contains a bug fix which improves sentence matches with incarceration_sessions by a lot. The bug fix basically keeps
  -- sentences if they have a min_length_days or max_length_days, since those are the fields currently captured projected min/max date information in PA
  LEFT JOIN `sessions.compartment_sentences_materialized` sent
      USING(person_id,state_code,session_id)
  WHERE cs.state_code = "US_PA"
  AND compartment_level_2 = "GENERAL"
  AND start_reason = 'NEW_ADMISSION'
),
-- Get external IDs for all the people with general incarceration sessions
external_ids AS (
    SELECT us_pa_caseload.*,
        external_id,
    FROM `state.state_person_external_id`
    INNER JOIN us_pa_caseload
        USING (person_id)
    WHERE id_type = 'US_PA_INMATE'
),
-- Query raw sentence table, joining on ID and on the same sentence that joins to a given session in compartment_sentences
-- Pull effective date, projected min date, and projected max date
-- DISTINCT is needed because there are many external IDs that map to a single person ID, so at this point we have perfect duplicates on everything except external_id
sentences AS (
  SELECT DISTINCT external_ids.* EXCEPT(external_id),
    SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y%m%d', effective_date) AS DATE) AS effective_date,
    SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y%m%d', min_expir_date) AS DATE) AS projected_minimum_date,
    SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y%m%d', max_expir_date) AS DATE) AS projected_maximum_date,
  FROM `us_pa_raw_data_up_to_date_views.dbo_Senrec_latest` senrec
  INNER JOIN external_ids
    ON senrec.curr_inmate_num = external_ids.external_id
    AND SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y%m%d', sent_start_date) AS DATE) = external_ids.sentence_start_date
),
-- This CTE makes various date calculations, e.g. the date when half the sentence will be served, date when 25 years will be served, etc
-- It takes the minimum of those two dates, as that will be used to determine eligibility
-- Here, effective date is used rather than sentence start date. Effective Date is the field that actually seems to correspond to the projected_end_dates, since
-- effective date + min/max length days = min/max projected end date
-- Finally, everything is collapsed to the compartment_level_0 level (i.e. collapsing across incarceration out of state and in state)
-- There are a handful of cases where the same sentence start date (and session start date) have different projected_minimum_date. In this case the latest projected_minimum_date is kept
final as (
    SELECT ss.person_id,
    compartment_level_0_super_session_id,
    ss.start_date,
    ss.end_date,
    ss.inflow_from_level_1,
    ss.inflow_from_level_2,
    ss.outflow_to_level_1,
    ss.outflow_to_level_2,
    ss.session_length_days,
    sentence_start_date,
    effective_date ,
    sentence_completion_date,
    projected_minimum_date,
    projected_maximum_date,
    DATE_DIFF(COALESCE(ss.end_date,current_date) , ss.start_date , MONTH) as session_length_months,
    DATE_DIFF(projected_minimum_date , effective_date , MONTH) as min_expected_los,
    DATE_DIFF(projected_maximum_date , effective_date , MONTH) as max_expected_los,
    DATE_DIFF(projected_minimum_date , effective_date , MONTH) * 1.93 as min_expected_los_adj,
    DATE_DIFF(projected_maximum_date , effective_date , MONTH) * 0.71 as max_expected_los_adj,

    DATE_DIFF(sentence_completion_date , effective_date , MONTH) as actual_los,
    -- FLOOR(DATE_DIFF(projected_minimum_date , effective_date , MONTH)/2) as half_min_expected_los,
    -- CASE WHEN ss.session_length_days / 365.25 >= 25 THEN 1 ELSE 0 END as incarcerated_25_years,
    DATE_ADD(effective_date, interval CAST(FLOOR(DATE_DIFF(projected_minimum_date , effective_date , MONTH) * 0.5) AS INT) month) AS date_half_served,
    DATE_ADD(effective_date, interval 25*12 month) AS date_25_yrs_served,
    LEAST(COALESCE(DATE_ADD(effective_date, interval 25*12 month),'9999-01-01'),
          COALESCE(DATE_ADD(effective_date, interval CAST(FLOOR(DATE_DIFF(projected_minimum_date , effective_date , MONTH) * 0.5) AS INT) month),'9999-01-01')
        ) AS minimum_elig_date,
FROM sentences
JOIN `sessions.compartment_level_0_super_sessions` ss
    ON sentences.person_id = ss.person_id
    AND sentences.session_id BETWEEN ss.session_id_start AND ss.session_id_end
WHERE TRUE
QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, start_date, effective_date ORDER BY projected_minimum_date DESC) = 1
)
-- This CTE brings in birthdate information and calculates age at the start of an incarceration session, age at start of effective_date, and age at eligibility date
select *
from (
    select final.*,
    birthdate,
    IFNULL(SAFE_DIVIDE(final.session_length_months , final.min_expected_los),1) as percent_min_served_before_session_end,
    IFNULL(SAFE_DIVIDE(final.session_length_months , final.max_expected_los),1) as percent_max_served_before_session_end,
    IFNULL(SAFE_DIVIDE(final.actual_los , final.min_expected_los),1) as percent_min_served,
    IFNULL(SAFE_DIVIDE(final.actual_los , final.max_expected_los),1) as percent_max_served,
    CAST(FLOOR(DATE_DIFF('2021-11-01', birthdate, DAY) / 365.25) AS INT64) AS age_current,
    CAST(FLOOR(DATE_DIFF(start_date, birthdate, DAY) / 365.25) AS INT64) AS age_start,
    CAST(FLOOR(DATE_DIFF(end_date, birthdate, DAY) / 365.25) AS INT64) AS age_end,
    CAST(FLOOR(DATE_DIFF(effective_date, birthdate, DAY) / 365.25) AS INT64) AS age_effective_date,
    CAST(FLOOR(DATE_DIFF(minimum_elig_date, birthdate, DAY) / 365.25) AS INT64) AS age_at_elig_date,
    DATE_ADD(birthdate, interval 55*12 month) AS date_turn_55,
    CAST(FLOOR(DATE_DIFF(final.sentence_completion_date , birthdate, DAY) / 365.25) AS INT64) AS age_at_sentence_completion,
    DATE_DIFF(minimum_elig_date , current_date() , MONTH) as time_till_eligibility,
    DATE_DIFF(final.projected_minimum_date  , minimum_elig_date , MONTH) as min_expected_los_after_eligibility,


    from final
    JOIN `sessions.person_demographics`
        USING(person_id)
)
