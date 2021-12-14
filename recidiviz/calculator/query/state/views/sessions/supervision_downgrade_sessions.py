# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Sessionized view of each individual. Session defined as continuous supervision downgrade recommendation of the same value.

To generate the BQ view, run:
python -m recidiviz.calculator.query.state.views.sessions.supervision_downgrade_sessions
"""

from operator import itemgetter

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.pipeline.utils.calculator_utils import (
    PRIMARY_PERSON_EXTERNAL_ID_TYPES_TO_INCLUDE,
)
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
)
from recidiviz.case_triage.opportunities.types import OpportunityType
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_supervision_levels_ascending = [
    item[0].value
    for item in sorted(
        StateSupervisionLevel.get_comparable_level_rankings().items(), key=itemgetter(1)
    )
]

SUPERVISION_DOWNGRADE_SESSIONS_VIEW_NAME = "supervision_downgrade_sessions"

SUPERVISION_DOWNGRADE_SESSIONS_VIEW_DESCRIPTION = """Sessionized view of each individual.
Session defined as continuous supervision downgrade recommendation of the same value.
A mismatch is considered "corrected" if the person's supervision level was reduced to match
the recommendation without a reassessment.
"""

_supervision_id_types = PRIMARY_PERSON_EXTERNAL_ID_TYPES_TO_INCLUDE[
    "supervision"
].values()

SUPERVISION_DOWNGRADE_SESSIONS_QUERY_TEMPLATE = f"""
    /*{{description}}*/
    WITH 
    day_zero_reports AS (
        SELECT
            day_zero_reports.*,
            person_id,
        FROM `{{project_id}}.{{static_reference_dataset}}.day_zero_reports` day_zero_reports
        INNER JOIN `{{project_id}}.{{state_dataset}}.state_person_external_id` pei
            ON day_zero_reports.state_code = pei.state_code
            AND day_zero_reports.person_external_id = pei.external_id
            AND pei.id_type IN {tuple(_supervision_id_types)}
        WHERE opportunity_type = '{OpportunityType.OVERDUE_DOWNGRADE.value}'
    ),
    # identify contiguous blocks of the same downgrade recommendation in daily dataflow observations;
    # we will group these into sessions in the next step
    recommendation_grouped AS (
        SELECT 
            compliance_metrics.state_code,
            compliance_metrics.person_id,
            date_of_supervision,
            supervision_level,
            recommended_supervision_downgrade_level,
            assessment_date,
            # date_of_supervision is eligible to be considered a "surfaced date" if: 
            # 1. there is a recommended downgrade AND
            # 2. the person was supervised by someone with line staff tool access OR was included in a day zero report on that day
            IF(
                recommended_supervision_downgrade_level IS NULL 
                OR (
                    # convoluted logic but this just translates to "no line staff tool access"
                    NOT IFNULL(has_case_triage_access OR has_po_report_access, FALSE)
                    AND day_zero_reports.report_date IS NULL
                ),
                NULL, 
                date_of_supervision
            ) AS date_of_surface_eligibility,
            # this variable will be used for grouping sessions, its actual value is not necessarily meaningful;
            # it happens to be the start date of a contiguous block of the same recommendation
            DATE_SUB(
                date_of_supervision, 
                INTERVAL 
                    ROW_NUMBER() OVER (
                        PARTITION BY 
                            compliance_metrics.person_id, 
                            IFNULL(recommended_supervision_downgrade_level, 'NONE')
                        ORDER BY date_of_supervision ASC
                    ) 
                DAY 
            ) AS group_by_status,
        FROM `{{project_id}}.{{materialized_metrics_dataset}}.most_recent_supervision_case_compliance_metrics_materialized` compliance_metrics
        LEFT JOIN `{{project_id}}.{{sessions_dataset}}.assessment_score_sessions_materialized` assessment_score_sessions
            ON compliance_metrics.person_id = assessment_score_sessions.person_id
            AND compliance_metrics.date_of_supervision BETWEEN assessment_score_sessions.assessment_date AND IFNULL(assessment_score_sessions.score_end_date, CURRENT_DATE())
        LEFT JOIN `{{project_id}}.{{sessions_dataset}}.supervision_tool_access_sessions_materialized` supervision_tool_access_sessions
            ON compliance_metrics.person_id = supervision_tool_access_sessions.person_id 
            AND date_of_supervision BETWEEN supervision_tool_access_sessions.start_date AND IFNULL(supervision_tool_access_sessions.end_date, CURRENT_DATE())
        LEFT JOIN day_zero_reports
            ON compliance_metrics.person_id = day_zero_reports.person_id
            AND date_of_supervision = day_zero_reports.report_date
    ),
    # this will sessionize the downgrade recommendations themselves;
    # determining the reason why a session ended will require another pass comparing adjacent sessions
    mismatch_sessions_base AS (
        select
            state_code,
            person_id,
            recommended_supervision_downgrade_level,
            MIN(date_of_supervision) AS start_date,
            MAX(date_of_supervision) AS end_date,
            MIN(date_of_surface_eligibility) AS surfaced_date,
            ARRAY_AGG(assessment_date ORDER BY date_of_supervision ASC LIMIT 1)[OFFSET(0)] AS assessment_date_start,
            ARRAY_AGG(assessment_date ORDER BY date_of_supervision DESC LIMIT 1)[OFFSET(0)] AS assessment_date_end,
            ARRAY_AGG(supervision_level ORDER BY date_of_supervision ASC LIMIT 1)[OFFSET(0)] AS supervision_level_start,
            ARRAY_AGG(supervision_level ORDER BY date_of_supervision DESC LIMIT 1)[OFFSET(0)] AS supervision_level_end,
        FROM recommendation_grouped
        GROUP BY 
            state_code, 
            person_id, 
            recommended_supervision_downgrade_level, 
            group_by_status
    ),
    # gives us numeric values we can compare to identify downgrades
    levels_for_comparison AS (
        SELECT
            level,
            severity,
        FROM UNNEST({_supervision_levels_ascending}) AS level
        WITH OFFSET as severity
    ),
    mismatch_sessions_with_severity AS (
        SELECT
            *,
            comp_recommended.severity AS recommended_supervision_downgrade_level_severity,
            comp_start.severity AS supervision_level_start_severity,
        FROM mismatch_sessions_base
        LEFT JOIN levels_for_comparison comp_recommended
            ON recommended_supervision_downgrade_level = comp_recommended.level
        LEFT JOIN levels_for_comparison comp_start
            ON supervision_level_start = comp_start.level
    ),
    # will use this to identify open periods
    last_day_of_data_by_state AS (
        SELECT 
            state_code,
            MAX(date_of_supervision) last_day_of_data
        FROM recommendation_grouped 
        GROUP BY state_code
    )
    SELECT 
        mismatch_sessions_with_severity.state_code,
        mismatch_sessions_with_severity.person_id,
        recommended_supervision_downgrade_level,
        mismatch_sessions_with_severity.start_date,
        # gives open sessions a NULL end date
        IF(mismatch_sessions_with_severity.end_date < last_day_of_data, mismatch_sessions_with_severity.end_date, NULL) AS end_date,
        surfaced_date,
        # the mismatch is only considered "corrected" if the client was downgraded to (or beyond) the recommended level without reassessment
        recommended_supervision_downgrade_level_severity >= LEAD(supervision_level_start_severity) OVER person_sessions_chronological
            AND assessment_date_end = LEAD(assessment_date_start) OVER person_sessions_chronological
            AND end_date = DATE_SUB(LEAD(start_date) OVER person_sessions_chronological, INTERVAL 1 DAY)
        AS mismatch_corrected,
    FROM mismatch_sessions_with_severity
    LEFT JOIN last_day_of_data_by_state USING (state_code)
    WINDOW person_sessions_chronological AS (
        PARTITION BY mismatch_sessions_with_severity.person_id 
        ORDER BY mismatch_sessions_with_severity.start_date
    )
"""

SUPERVISION_DOWNGRADE_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=SUPERVISION_DOWNGRADE_SESSIONS_VIEW_NAME,
    view_query_template=SUPERVISION_DOWNGRADE_SESSIONS_QUERY_TEMPLATE,
    description=SUPERVISION_DOWNGRADE_SESSIONS_VIEW_DESCRIPTION,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
    materialized_metrics_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    state_dataset=STATE_BASE_DATASET,
    static_reference_dataset=STATIC_REFERENCE_TABLES_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_DOWNGRADE_SESSIONS_VIEW_BUILDER.build_and_print()
