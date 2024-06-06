# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Information about individual-level events for supervision clients."""
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.bq_utils import (
    get_pseudonymized_id_query_str,
    nonnull_end_date_clause,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.state_specific_query_strings import (
    state_specific_external_id_type,
)
from recidiviz.calculator.query.state.views.outliers.utils import (
    format_state_specific_person_events_filters,
)
from recidiviz.outliers.constants import (
    TREATMENT_REFERRALS,
    VIOLATION_RESPONSES,
    VIOLATIONS,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "supervision_client_events"

_DESCRIPTION = """Information about individual-level events for supervision clients."""

# NOTE: When adding new queries to this, if the event can be queried from person_events or similar,
# the list of event types corresponding to the event in question can be found in
# event.aggregated_metric.event_types where event is an OutliersClientEvent
#
# In order to be queried from the frontend, the relevant OutliersClientEvent must also be listed
# in the OutliersConfig for the given state.

_QUERY_TEMPLATE = f"""
WITH
latest_year_time_period AS (
    SELECT
        population_start_date,
        population_end_date,
    FROM
        `{{project_id}}.aggregated_metrics.metric_time_periods_materialized`
    WHERE 
        period = "YEAR"
    QUALIFY ROW_NUMBER() OVER (ORDER BY population_start_date DESC) = 1
),
events_with_metric_id AS (
  {format_state_specific_person_events_filters()}
),
violations AS (
  SELECT DISTINCT
    "US_MI" as state_code,
    "violations" AS metric_id,
    violation_date AS event_date,
    sv.person_id,
    TO_JSON_STRING(
        STRUCT(
            UPPER(COALESCE(omni_type_ref.description, coms_parole.Violation_Type, coms_probation.Case_Type)) AS code,
            UPPER(
                CONCAT( 
                NULLIF(SPLIT(condition_raw_text, "@@")[OFFSET(0)], 'NONE'),
                ": ",
                NULLIF(SPLIT(condition_raw_text, "@@")[OFFSET(2)], 'NONE')
                )
            ) AS description
        )
    ) AS attributes
    -- TODO(#29820): Query from `normalized_state` instead of `state` here
    FROM `{{project_id}}.state.state_supervision_violation` sv
    LEFT JOIN `{{project_id}}.state.state_supervision_violated_condition_entry` cond
        ON sv.supervision_violation_id = cond.supervision_violation_id
    -- NOTE: Let's use the raw tables instead of state tables for violation type becaue the ingested version has a ton of entity deletion issues.
    --       The entity deletion issues should be resolved with IID so we can refactor to use state tables after MI is switched over to using IID TODO(#25983)
    -- violation type for OMNI (only have data for parole violations):
    LEFT JOIN `{{project_id}}.us_mi_raw_data_up_to_date_views.ADH_SUPERVISION_VIOLATION_latest` type
        ON SPLIT(sv.external_id, "##")[OFFSET(0)] = type.supervision_violation_id 
        AND sv.external_id NOT LIKE 'COMS%' 
        AND sv.external_id NOT LIKE 'PROBATION%'
    LEFT JOIN `{{project_id}}.us_mi_raw_data_up_to_date_views.ADH_REFERENCE_CODE_latest` omni_type_ref
        ON type.violation_type_id = omni_type_ref.reference_code_id
    -- violation type for COMS:
    LEFT JOIN `{{project_id}}.us_mi_raw_data_up_to_date_views.COMS_Violation_Incidents_latest` coms_incidents
        ON SPLIT(sv.external_id, "##")[OFFSET(1)] = coms_incidents.Violation_Incident_Id 
        AND sv.external_id LIKE 'COMS%'
    LEFT JOIN `{{project_id}}.us_mi_raw_data_up_to_date_views.COMS_Parole_Violation_Violation_Incidents_latest` coms_parole_incidents 
        ON coms_parole_incidents.Violation_Incident_Id = coms_incidents.Violation_Incident_Id
    LEFT JOIN `{{project_id}}.us_mi_raw_data_up_to_date_views.COMS_Parole_Violations_latest` coms_parole 
        ON coms_parole.Parole_Violation_Id = coms_parole_incidents.Parole_Violation_Id
    LEFT JOIN `{{project_id}}.us_mi_raw_data_up_to_date_views.COMS_Probation_Violation_Violation_Incidents_latest` coms_probation_incidents 
        ON coms_probation_incidents.Violation_Incident_Id = coms_incidents.Violation_Incident_Id
    LEFT JOIN `{{project_id}}.us_mi_raw_data_up_to_date_views.COMS_Probation_Violations_latest` coms_probation 
        ON coms_probation.Probation_Violation_Id = coms_probation_incidents.Probation_Violation_Id
    WHERE
        sv.state_code = 'US_MI'
        AND violation_date IS NOT NULL

    UNION ALL

    SELECT
        "US_TN" AS state_code,
        "{VIOLATIONS.name}" AS metric_id,
        violation_date as event_date,
        sv.person_id,
        TO_JSON_STRING( 
            STRUCT(
                UPPER(type.violation_type_raw_text)  as code,
                UPPER(tct.Decode)  as description
            )) AS attributes
        FROM `{{project_id}}.normalized_state.state_supervision_violation` sv
        INNER JOIN `{{project_id}}.normalized_state.state_supervision_violation_type_entry` type
            USING(supervision_violation_id, state_code)
        INNER JOIN `{{project_id}}.us_tn_raw_data_up_to_date_views.TOMIS_CODESTABLE_latest` tct ON type.violation_type_raw_text = tct.Code AND CodesTable = 'TDCD050'
        WHERE
            sv.state_code = 'US_TN'
            AND violation_date IS NOT NULL 
            AND violation_type_raw_text != "INFERRED"
    
),
sanctions AS (
-- TODO(#24708): Add in MI sanction data from COMS once we receive it

    SELECT
        "US_TN" AS state_code,
        "{VIOLATION_RESPONSES.name}" AS metric_id,
        response_date as event_date,
        person_id,
        TO_JSON_STRING( 
            STRUCT(
                IFNULL(resp.response_type_raw_text, 'No Proposed Sanction')  as code,
                IFNULL(tct.Decode, JSON_VALUE(violation_response_metadata, '$.SanctionStatus'))  as description
            )) AS attributes
        FROM `{{project_id}}.normalized_state.state_supervision_violation_response` resp
        LEFT JOIN `{{project_id}}.us_tn_raw_data_up_to_date_views.TOMIS_CODESTABLE_latest` tct ON resp.response_type_raw_text = tct.Code AND CodesTable = 'TDCD340'
        WHERE
            state_code = 'US_TN'
            AND response_date IS NOT NULL
    
),
treatment_referrals AS (
    SELECT
        "US_MI" AS state_code,
        "{TREATMENT_REFERRALS.name}" AS metric_id,
        DATE(Referral_Date) AS event_date, 
        person_id,
        TO_JSON_STRING(
            STRUCT(
                NULL as code,
                UPPER(CONCAT(
                    IF(Program_Type = Service_Type, Program_Type, CONCAT(Program_Type, " (", Service_Type, ")")),
                    " -- ",
                    Provider
                )) as description
            )
        ) AS attributes
    FROM `{{project_id}}.us_mi_raw_data_up_to_date_views.COMS_Intervention_Referrals_latest` coms_treat
    INNER JOIN `{{project_id}}.us_mi_raw_data_up_to_date_views.COMS_Intervention_Referral_Program_and_Service_Type_Combinations_latest` coms_combo 
      USING(Intervention_Referral_Id, Offender_Number)
    LEFT JOIN `{{project_id}}.normalized_state.state_person_external_id` pei 
      ON LTRIM(coms_treat.Offender_Number, '0') = pei.external_id AND pei.id_type = 'US_MI_DOC'
    
    UNION ALL

    SELECT *
    FROM 
        (SELECT 
            "US_TN" AS state_code,
            "{TREATMENT_REFERRALS.name}" AS metric_id,
            CAST(StartDate AS DATETIME) as event_date,
            pid.person_id,
            TO_JSON_STRING(
                STRUCT(
                    NULL as code,
                    Program as description
                )
            ) AS attributes
        FROM `{{project_id}}.us_tn_raw_data_up_to_date_views.OffenderTreatment_latest` ot
        INNER JOIN `{{project_id}}.normalized_state.state_person_external_id` pid ON pid.external_id = ot.OffenderID
        
        UNION ALL
        
        SELECT 
            "US_TN" AS state_code,
            "{TREATMENT_REFERRALS.name}" AS metric_id,
            CAST(vpr.RecommendationDate AS DATETIME) as event_date,
            pid.person_id,
            TO_JSON_STRING(
                STRUCT(
                    NULL as code,
                    Recommendation as description
                )
            ) AS attributes
        FROM `{{project_id}}.us_tn_raw_data_up_to_date_views.VantagePointRecommendations_latest` vpr
        INNER JOIN `{{project_id}}.normalized_state.state_person_external_id` pid ON pid.external_id = vpr.OffenderID)
)
-- TREATMENT_STARTS events are treated differently than the other events, they represent when a client did NOT
-- have a treatment start within the time period (US_CA only)
, treatment_starts AS (
    SELECT
        "US_CA" AS state_code,
        "treatment_starts" AS metric_id,
        -- When there is not a treatment start, the event date is either the last day of the current time period
        DATE_SUB(population_end_date, INTERVAL 1 DAY) AS event_date,
        a.person_id,
        TO_JSON_STRING( 
            STRUCT(
                NULL  as code,
                "No treatment start within the past year"  as description
            )) AS attributes
    FROM `{{project_id}}.aggregated_metrics.supervision_officer_metrics_person_assignment_sessions_materialized` a
    CROSS JOIN
        latest_year_time_period period
    LEFT JOIN `{{project_id}}.normalized_state.state_program_assignment` spa
        USING(person_id)
    WHERE a.state_code = "US_CA"
        -- Find clients who do not have any treatment starts, or those who do not have a treatment start within the time period
        AND (spa.person_id IS NULL OR spa.start_date NOT BETWEEN population_start_date AND population_end_date)
        -- Find officer periods that fall within the current time period
        AND a.assignment_date <= population_end_date AND IFNULL(a.end_date, population_end_date) >= population_start_date
),
all_events AS (
    SELECT * FROM events_with_metric_id
        UNION ALL
    SELECT * FROM violations
        UNION ALL
    SELECT * FROM sanctions
        UNION ALL
    SELECT * FROM treatment_referrals
        UNION ALL
    SELECT * FROM treatment_starts
),
supervision_client_events AS (
    SELECT 
        e.state_code, 
        e.metric_id,
        e.event_date,
        pid.external_id AS client_id,
        p.full_name AS client_name,
        a.officer_id,
        a.assignment_date AS officer_assignment_date,
        a.end_date AS officer_assignment_end_date,
        -- TODO(#26843): Revisit these calculations / column names
        COALESCE(s.start_date, ss.start_date) AS supervision_start_date,
        ss.end_date AS supervision_end_date,
        CASE WHEN s.compartment_level_2 IN ("PROBATION", "INFORMAL_PROBATION") THEN "PROBATION"
             WHEN s.compartment_level_2 IN ("PAROLE", "DUAL", "COMMUNITY_CONFINEMENT") THEN s.compartment_level_2
            -- If they spent time on supervision but were never in one of the above supervision types,
            -- just say "supervision" so the frontend displays "supervision start date"
             ELSE "SUPERVISION"
             END AS supervision_type,
        e.attributes,
        {get_pseudonymized_id_query_str("IF(e.state_code = 'US_IX', 'US_ID', e.state_code) || pid.external_id")} AS pseudonymized_client_id,
        -- This pseudonymized_id will match the one for the user in the auth0 roster. Hashed
        -- attributes must be kept in sync with recidiviz.auth.helpers.generate_pseudonymized_id.
        {get_pseudonymized_id_query_str("IF(e.state_code = 'US_IX', 'US_ID', e.state_code) || a.officer_id")} AS pseudonymized_officer_id,
    FROM 
        `{{project_id}}.aggregated_metrics.supervision_officer_metrics_person_assignment_sessions_materialized` a
    CROSS JOIN
        latest_year_time_period period
    INNER JOIN `{{project_id}}.normalized_state.state_person` p 
        USING (person_id)
    INNER JOIN all_events e
        ON p.person_id = e.person_id
        -- Get the officer assignment information for the session leading up to the event
        AND event_date BETWEEN a.assignment_date AND {nonnull_end_date_clause('a.end_date')}
    INNER JOIN `{{project_id}}.normalized_state.state_person_external_id` pid
        ON p.person_id = pid.person_id
    INNER JOIN `{{project_id}}.sessions.compartment_level_1_super_sessions_materialized` ss  
        ON a.person_id = ss.person_id
        -- Get the supervision period information for the session leading up to the event
        -- Note: the event_date occurs in the INCARCERATION session after the SUPERVISION session we want
        -- details about, so we use nonnull_end_date_clause here instead of nonnull_end_date_exclusive_clause to
        -- ensure the SUPERVISION session is returned in this join 
        AND event_date BETWEEN ss.start_date AND {nonnull_end_date_clause('ss.end_date_exclusive')}
        AND ss.compartment_level_1 = "SUPERVISION"
    LEFT JOIN `{{project_id}}.sessions.compartment_sessions_materialized` s
        ON ss.person_id = s.person_id
        AND s.session_id BETWEEN ss.session_id_start AND ss.session_id_end
        AND s.compartment_level_2 IN ("PAROLE", "PROBATION", "INFORMAL_PROBATION", "DUAL", "COMMUNITY_CONFINEMENT")
        -- we want to show the supervision start date as being before the event. we don't need the
        -- end date to be before the compartment end, because if we have:
        --   date 1 - date 2: PAROLE
        --   date 2 - date 3: WARRANT_STATUS
        --   date 3: ABSCONSION
        -- then we want to show the supervision start date as date 1, despite the event date being
        -- after the compartment end.
        AND event_date >= s.start_date
    WHERE
        -- Get events for the latest year period only
        event_date >= population_start_date
        AND event_date < population_end_date
        AND pid.id_type = {{state_id_type}}
    -- Dedup to the latest compartment occurring within the super session and to the earliest
    -- officer assignment session that overlaps with the event. The latter helps us find the most
    -- likely actual officer assignment if there is some overlap around the event date.
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ss.person_id, event_date, metric_id ORDER BY s.start_date DESC, {nonnull_end_date_clause('a.end_date')}, a.assignment_date) = 1
)

SELECT 
    {{columns}}
FROM supervision_client_events
"""

SUPERVISION_CLIENT_EVENTS_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=dataset_config.OUTLIERS_VIEWS_DATASET,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=_DESCRIPTION,
    should_materialize=True,
    state_id_type=state_specific_external_id_type("pid"),
    columns=[
        "state_code",
        "metric_id",
        "event_date",
        "client_id",
        "client_name",
        "officer_id",
        "officer_assignment_date",
        "officer_assignment_end_date",
        "supervision_start_date",
        "supervision_end_date",
        "supervision_type",
        "attributes",
        "pseudonymized_client_id",
        "pseudonymized_officer_id",
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_CLIENT_EVENTS_VIEW_BUILDER.build_and_print()
