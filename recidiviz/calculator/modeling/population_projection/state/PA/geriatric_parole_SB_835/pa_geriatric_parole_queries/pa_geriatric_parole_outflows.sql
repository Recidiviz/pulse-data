WITH cohort AS
    (
    SELECT
        person_id,
        state_code,
        session_id AS cohort_session_id,
        start_date AS cohort_start_date,
        last_day_of_data,
    FROM
      (
      SELECT
        c.*,
        ss.person_id IS NOT NULL AS supervision_super_session_start,
      FROM `analyst_data.compartment_sessions_materialized` c
      LEFT JOIN `analyst_data.supervision_super_sessions_materialized` ss
        ON c.person_id = ss.person_id
        AND c.session_id = ss.session_id_start
      )
    WHERE compartment_level_2 in ('PAROLE','DUAL')
    and inflow_from_level_1 !='SUPERVISION'
    and age_start > 55
    and start_date BETWEEN '2011-11-01' AND '2014-11-01'

    )
    ,
    event AS
    (
    SELECT
        person_id,
        state_code,
        session_id AS event_session_id,
        end_date AS event_end_date,

    FROM
      (
      SELECT
        c.*,
        IF(compartment_level_1 LIKE 'INCARCERATION%', LAG(ss.person_id IS NOT NULL) OVER(PARTITION BY c.person_id ORDER BY c.session_id), FALSE) AS incarceration_from_supervision_super_session,
        c.start_reason LIKE '%REVOCATION' OR c.start_reason = 'SANCTION_ADMISSION' AS is_commitment,
        FROM `analyst_data.compartment_sessions_materialized` c
        LEFT JOIN `analyst_data.supervision_super_sessions_materialized` ss
            ON c.person_id = ss.person_id
            AND c.session_id = ss.session_id_end
      )
    WHERE outflow_to_level_1 = 'DEATH'
    )
    ,
    cohort_x_event AS
    (
    SELECT
        cohort.person_id,
        cohort_session_id,
        cohort_start_date,
        event_session_id,
        event_end_date,
        1 AS event,
        ROW_NUMBER() OVER(PARTITION BY cohort.person_id, cohort_session_id ORDER BY event_session_id) AS rank_of_event,
        ROW_NUMBER() OVER(PARTITION BY cohort.person_id, event_session_id ORDER BY cohort_session_id DESC) AS rank_of_cohort,
    FROM cohort
    JOIN event
        ON cohort.person_id = event.person_id
        AND event_end_date>cohort_start_date
    ORDER BY cohort_start_date, event_end_date
    )
    ,
    cte AS
    (
    SELECT
        cohort.person_id,
        cohort.cohort_start_date,
        cohort.cohort_session_id,
        cohort.state_code,
        COALESCE(cohort_x_event.event,0) AS event,
        cohort_x_event.event_session_id,
        cohort_x_event.event_end_date,
        DATE_DIFF(last_day_of_data, cohort.cohort_start_date, MONTH)
                    - IF(EXTRACT(DAY FROM cohort.cohort_start_date)>EXTRACT(DAY FROM last_day_of_data), 1, 0) cohort_months_to_mature,
        DATE_DIFF(cohort_x_event.event_end_date, cohort.cohort_start_date, MONTH)
                - IF(EXTRACT(DAY FROM cohort.cohort_start_date)>=EXTRACT(DAY FROM cohort_x_event.event_end_date), 1, 0) + 1 AS cohort_start_to_event_months,
    FROM cohort
    LEFT JOIN cohort_x_event
        ON cohort.person_id = cohort_x_event.person_id
        AND cohort.cohort_session_id = cohort_x_event.cohort_session_id
        AND rank_of_event = 1
        AND rank_of_cohort = 1
    ), final as (
            SELECT
        cte.person_id,
        cte.state_code,
        cte.cohort_start_date,
        cte.cohort_session_id,
        i.cohort_months,
        cte.cohort_months_to_mature,
        cte.event_end_date,
        cte.event_session_id,
        cte.cohort_start_to_event_months,
        CASE WHEN cte.cohort_start_to_event_months <= i.cohort_months THEN 1 ELSE 0 END AS event,
        MAX(i.cohort_months) OVER(PARTITION BY cte.person_id, cte.cohort_start_date) AS cohort_months_fully_mature,
    FROM cte
    JOIN `analyst_data.cohort_month_index` i
        ON cte.cohort_months_to_mature>=i.cohort_months
    ORDER BY person_id, cohort_start_date, cohort_months

    )
    select cohort_months, sum(event)/count(*) percent_death
    from final
    group by 1
    order by 1