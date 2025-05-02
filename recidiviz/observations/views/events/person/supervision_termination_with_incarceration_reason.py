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
"""View with supervision terminations with an end reason indicating subsequent
incarceration, but without an observed compartment outflow to incarceration.
"""
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = (
    "Supervision terminations with an end reason indicating subsequent incarceration, "
    "but without an observed compartment outflow to incarceration"
)

_SOURCE_DATA_QUERY_TEMPLATE = f"""
SELECT
    a.state_code,
    a.person_id,
    a.end_date_exclusive,
    a.compartment_level_1,
    a.compartment_level_2,
    a.end_reason,
    TRUE AS is_discretionary,
    -- Get the first non-null violation type among supervision terminations occurring during the super session
    COALESCE(c.most_severe_violation_type, "INTERNAL_UNKNOWN") AS most_severe_violation_type,
    COALESCE(JSON_EXTRACT_SCALAR(viol.violation_metadata, '$.ViolationType'), '') = 'INFERRED' as violation_is_inferred,
    viol.violation_date AS most_severe_violation_date,
    c.termination_date AS most_severe_violation_type_termination_date,
    COUNT(DISTINCT d.referral_date)
        OVER (PARTITION BY a.person_id, a.end_date_exclusive) AS prior_treatment_referrals_1y,
    css.compartment_level_2 AS latest_active_supervision_type,
    css.correctional_level_end AS latest_active_supervision_level,
    asmt.assessment_level AS latest_assessment_level,
FROM
    `{{project_id}}.sessions.compartment_sessions_materialized` a
LEFT JOIN
    `{{project_id}}.sessions.compartment_level_1_super_sessions_materialized` b
ON
    a.person_id = b.person_id
    -- If person outflows to another supervision compartment, this join will consider all terminations
    -- during the remainder of that supervision compartment super session. Otherwise,
    -- it takes the next compartment level 1 super session.
    AND a.end_date_exclusive BETWEEN b.start_date AND {nonnull_end_date_exclusive_clause("b.end_date_exclusive")}
LEFT JOIN
    `{{project_id}}.sessions.compartment_level_1_super_sessions_materialized` s
ON
    a.person_id = s.person_id
    -- get the compartment_level_1_super_session that includes the supervision compartment that ends unsuccessfully
    AND a.end_date BETWEEN s.start_date AND {nonnull_end_date_exclusive_clause("b.end_date_exclusive")}
LEFT JOIN 
    `{{project_id}}.sessions.compartment_sessions_materialized` css
ON css.person_id = s.person_id
    AND css.session_id BETWEEN s.session_id_start AND s.session_id_end
    --only join active compartment level 2 values 
    AND css.compartment_level_2 IN ("PAROLE", "PROBATION", "INFORMAL_PROBATION", "DUAL", "COMMUNITY_CONFINEMENT")
    --the unsuccessful termination should be after the active compartment starts 
    AND a.end_date >= css.start_date
LEFT JOIN
    `{{project_id}}.dataflow_metrics_materialized.most_recent_supervision_termination_metrics_materialized` c
ON
    a.person_id = c.person_id
    AND c.termination_date BETWEEN a.end_date_exclusive AND {nonnull_end_date_exclusive_clause("b.end_date_exclusive")}
LEFT JOIN
    `{{project_id}}.normalized_state.state_supervision_violation` viol
ON
    c.person_id = viol.person_id
    AND c.most_severe_violation_id = viol.supervision_violation_id
-- Referrals within one year of supervision termination
LEFT JOIN
    `{{project_id}}.normalized_state.state_program_assignment` d
ON
    a.person_id = d.person_id
    AND DATE_SUB(a.end_date_exclusive, INTERVAL 365 DAY) <= d.referral_date
LEFT JOIN
    `{{project_id}}.sessions.system_sessions_materialized` ss
ON
    a.person_id = ss.person_id
    AND a.session_id BETWEEN ss.session_id_start AND ss.session_id_end
/* TODO(#39399): Determine whether/how we want to deduplicate/aggregate when there might
be multiple 'RISK' assessments with open spans coming out of the sessionized assessments
view. */
LEFT JOIN 
    `{{project_id}}.sessions.assessment_score_sessions_materialized` asmt
ON
    asmt.person_id = a.person_id
    -- Pull assessment session during supervision termination
    AND a.end_date_exclusive BETWEEN asmt.assessment_date AND {nonnull_end_date_exclusive_clause("asmt.score_end_date")}
    -- Ensure assessment is within the same system session
    AND asmt.assessment_date BETWEEN ss.start_date AND {nonnull_end_date_exclusive_clause("ss.end_date_exclusive")}
    -- Filter out incarceration-only assessments
    AND NOT asmt.is_incarceration_only_assessment_type
    -- Restrict to risk assessments
    AND asmt.assessment_class = 'RISK'
WHERE
    a.compartment_level_1 = "SUPERVISION"
    AND a.end_reason IN ("ADMITTED_TO_INCARCERATION", "REVOCATION")
    AND a.outflow_to_level_1 != "INCARCERATION"
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY a.person_id, a.end_date_exclusive
    -- Prioritize the first known violation type and the latest active supervision type 
    ORDER BY 
        IF(COALESCE(c.most_severe_violation_type, "INTERNAL_UNKNOWN") != "INTERNAL_UNKNOWN", 0, 1), 
        c.termination_date,
        css.start_date DESC,
        asmt.assessment_date DESC
) = 1
"""

VIEW_BUILDER: EventObservationBigQueryViewBuilder = EventObservationBigQueryViewBuilder(
    event_type=EventType.SUPERVISION_TERMINATION_WITH_INCARCERATION_REASON,
    description=_VIEW_DESCRIPTION,
    sql_source=_SOURCE_DATA_QUERY_TEMPLATE,
    attribute_cols=[
        "compartment_level_1",
        "compartment_level_2",
        "end_reason",
        "violation_is_inferred",
        "most_severe_violation_date",
        "most_severe_violation_type",
        "most_severe_violation_type_termination_date",
        "prior_treatment_referrals_1y",
        "is_discretionary",
        "latest_active_supervision_type",
        "latest_active_supervision_level",
        "latest_assessment_level",
    ],
    event_date_col="end_date_exclusive",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
