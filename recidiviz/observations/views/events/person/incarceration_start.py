# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""View with transitions to incarceration"""
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
)
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = "Transitions to incarceration"

_SOURCE_DATA_QUERY_TEMPLATE = f"""
SELECT
    a.state_code,
    a.person_id,
    a.start_date,
    a.inflow_from_level_1,
    a.inflow_from_level_2,
    a.start_reason,
    -- Returning to incarceration for the weekend is not at an officer's discretion, but usually pre-determined by a
    -- judge, parole board, etc
    CASE WHEN a.start_reason IN ('WEEKEND_CONFINEMENT') THEN FALSE 
         ELSE TRUE 
         END AS is_discretionary,
    -- Get the first non-null violation type among incarceration starts occurring during the super session
    COALESCE(d.most_severe_violation_type, "INTERNAL_UNKNOWN") AS most_severe_violation_type,
    viol.violation_date AS most_severe_violation_date,
    COALESCE(JSON_EXTRACT_SCALAR(viol.violation_metadata, '$.ViolationType'), '') = 'INFERRED' as violation_is_inferred,
    COUNT(DISTINCT c.referral_date)
        OVER (PARTITION BY a.person_id, a.start_date) AS prior_treatment_referrals_1y,
    css_active.compartment_level_2 AS latest_active_supervision_type,
    css_active.correctional_level_end AS latest_active_supervision_level,
    asmt.assessment_level AS latest_assessment_level,
FROM
    `{{project_id}}.sessions.compartment_level_1_super_sessions_materialized` a
-- Get treatment referrals within 1 year of incarceration
LEFT JOIN
    `{{project_id}}.normalized_state.state_program_assignment` c
ON
    a.person_id = c.person_id
    AND DATE_SUB(a.start_date, INTERVAL 365 DAY) <= c.referral_date
-- Get all incarceration commitments during the super session
LEFT JOIN
    `{{project_id}}.dataflow_metrics_materialized.most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population_materialized` d
ON
    a.person_id = d.person_id
    AND d.admission_date BETWEEN a.start_date AND {nonnull_end_date_clause("a.end_date")}
LEFT JOIN
    `{{project_id}}.normalized_state.state_supervision_violation` viol
ON
    d.person_id = viol.person_id
    AND d.most_severe_violation_id = viol.supervision_violation_id
LEFT JOIN
    `{{project_id}}.sessions.compartment_level_1_super_sessions_materialized` s
ON s.person_id = a.person_id
  --join to the previous super session preceding the incarceration start if it's a supervision super session  
    AND a.compartment_level_1_super_session_id = s.compartment_level_1_super_session_id + 1 
    AND s.compartment_level_1 = "SUPERVISION"
-- This join with compartment sessions brings active compartments from the previous SUPERVISION super session
LEFT JOIN 
    `{{project_id}}.sessions.compartment_sessions_materialized` css_active
ON css_active.person_id = s.person_id
    AND css_active.session_id BETWEEN s.session_id_start AND s.session_id_end
    --only keep active compartment level 2 values
    AND css_active.compartment_level_2 IN ("PAROLE", "PROBATION", "INFORMAL_PROBATION", "DUAL", "COMMUNITY_CONFINEMENT") 
    --the incarceration start should be after the active compartment starts 
    AND a.start_date >= css_active.start_date
LEFT JOIN
    `{{project_id}}.sessions.system_sessions_materialized` ss
ON
    a.person_id = ss.person_id
    AND css_active.session_id BETWEEN ss.session_id_start AND ss.session_id_end
/* TODO(#39399): Determine whether/how we want to deduplicate/aggregate when there might
be multiple 'RISK' assessments with open spans coming out of the sessionized assessments
view. */
LEFT JOIN 
    `{{project_id}}.sessions.assessment_score_sessions_materialized` asmt
ON
    asmt.person_id = a.person_id
    -- Pull assessment session during incarceration start
    AND a.start_date BETWEEN asmt.assessment_date AND {nonnull_end_date_exclusive_clause("asmt.score_end_date")}    
    -- Ensure assessment is within the same system session
    AND asmt.assessment_date BETWEEN ss.start_date AND {nonnull_end_date_exclusive_clause("ss.end_date_exclusive")}
    -- Filter out incarceration-only assessments
    AND NOT asmt.is_incarceration_only_assessment_type
    -- Restrict to risk assessments
    AND asmt.assessment_class = 'RISK'
WHERE
    a.compartment_level_1 = "INCARCERATION"
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY a.person_id, a.start_date
    -- Prioritize the first known violation type and the latest active supervision type 
    ORDER BY
        IF(COALESCE(d.most_severe_violation_type, "INTERNAL_UNKNOWN") != "INTERNAL_UNKNOWN", 0, 1),
        d.admission_date,
        css_active.start_date DESC,
        asmt.assessment_date DESC 
) = 1
"""

VIEW_BUILDER: EventObservationBigQueryViewBuilder = EventObservationBigQueryViewBuilder(
    event_type=EventType.INCARCERATION_START,
    description=_VIEW_DESCRIPTION,
    sql_source=_SOURCE_DATA_QUERY_TEMPLATE,
    attribute_cols=[
        "inflow_from_level_1",
        "inflow_from_level_2",
        "start_reason",
        "most_severe_violation_date",
        "most_severe_violation_type",
        "violation_is_inferred",
        "prior_treatment_referrals_1y",
        "is_discretionary",
        "latest_active_supervision_type",
        "latest_active_supervision_level",
        "latest_assessment_level",
    ],
    event_date_col="start_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
