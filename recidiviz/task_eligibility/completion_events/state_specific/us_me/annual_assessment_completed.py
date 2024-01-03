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
"""Defines a view that shows all reclassification events
for any person across ME
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.calculator.query.state import dataset_config
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """Defines a view that shows all reclassification events
for any person, across ME.
"""
# TODO(#26361): Fill this stub with the right query, using CIS_140_CLASSIFICATION_REVIEW, CIS_1401_CLASSIFICATION_CD
_QUERY_TEMPLATE = """
SELECT
    state_code,
    person_id,
    CAST("1000-01-01" AS DATE) AS completion_event_date,
FROM
    `{project_id}.{sessions_dataset}.custody_level_sessions_materialized`
"""

VIEW_BUILDER: StateSpecificTaskCompletionEventBigQueryViewBuilder = (
    StateSpecificTaskCompletionEventBigQueryViewBuilder(
        state_code=StateCode.US_ME,
        completion_event_type=TaskCompletionEventType.ANNUAL_ASSESSMENT_COMPLETED,
        description=_DESCRIPTION,
        completion_event_query_template=_QUERY_TEMPLATE,
        sessions_dataset=dataset_config.SESSIONS_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
