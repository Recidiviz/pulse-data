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
"""Defines a view that shows all custody level upgrade events, specifically for upgrades
happening under the new 2026 classification policy in TN.
"""
from recidiviz.calculator.query.state import dataset_config
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_QUERY_TEMPLATE = """
WITH custody_level_upgrade AS (
    SELECT
        state_code,
        person_id,
        start_date AS completion_event_date,
    FROM
        `{project_id}.{sessions_dataset}.custody_level_sessions_materialized`
    WHERE
        state_code = "US_TN"
        AND custody_upgrade = 1
),
last_assessment_before_upgrade AS (
    SELECT 
        clu.state_code,
        clu.person_id,
        completion_event_date,
        assessment_type as last_classification_type
    FROM custody_level_upgrade clu
    LEFT JOIN `{project_id}.{sessions_dataset}.assessment_score_sessions_materialized` as_
    ON 
        clu.state_code = as_.state_code
        AND clu.person_id = as_.person_id 
        AND clu.completion_event_date >= as_.assessment_date 
    WHERE assessment_type in ('CAF', 'RCAF', 'DCAF')
    QUALIFY 
        ROW_NUMBER() OVER(
            PARTITION BY clu.person_id, clu.completion_event_date 
            ORDER BY assessment_date DESC, assessment_score DESC
        ) = 1
)
SELECT
    state_code,
    person_id,
    completion_event_date
FROM last_assessment_before_upgrade
WHERE last_classification_type IN ('RCAF', 'DCAF')
"""

VIEW_BUILDER: StateSpecificTaskCompletionEventBigQueryViewBuilder = (
    StateSpecificTaskCompletionEventBigQueryViewBuilder(
        state_code=StateCode.US_TN,
        completion_event_type=TaskCompletionEventType.CUSTODY_LEVEL_UPGRADE_2026_POLICY,
        description=__doc__,
        completion_event_query_template=_QUERY_TEMPLATE,
        sessions_dataset=dataset_config.SESSIONS_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
