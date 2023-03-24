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
"""Defines a criteria span view that shows spans of time during which clients
have a 'MEDIUM' supervision_level or lower ('MEDIUM', 'MINIMUM', 'LIMITED')
"""

from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "ON_MEDIUM_SUPERVISION_LEVEL_OR_LOWER"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which clients
have a 'MEDIUM' supervision_level or lower ('MEDIUM', 'MINIMUM', 'LIMITED')"""


_QUERY_TEMPLATE = """
SELECT 
    state_code,
    person_id,
    start_date, 
    end_date_exclusive AS end_date, 
    TRUE AS meets_criteria, 
    -- ME specific wording
    IF(state_code = 'US_ME',
        TO_JSON(STRUCT(
            CASE supervision_level
                WHEN 'MEDIUM' THEN 'Moderate Risk'
                WHEN 'MINIMUM' THEN 'Low Risk'
                WHEN 'LIMITED' THEN 'Administrative Risk'
                END
            AS supervision_level)),
        TO_JSON(STRUCT(supervision_level AS supervision_level)))
    AS reason,
FROM `{project_id}.{sessions_dataset}.supervision_level_sessions_materialized`
WHERE supervision_level IN ('MINIMUM', 'LIMITED', 'MEDIUM')
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        meets_criteria_default=False,
        sessions_dataset=SESSIONS_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
