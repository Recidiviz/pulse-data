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
"""Defines a view that shows all releases from incarceration to parole for any
person, across all states.
"""

from recidiviz.calculator.query.state import dataset_config
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    TaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """Defines a view that shows all releases from incarceration to parole
for any person, across all states."""

_QUERY_TEMPLATE = """
SELECT
  state_code,
  person_id,
  start_date AS completion_event_date,
FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized`
WHERE compartment_level_1 IN ("SUPERVISION", "SUPERVISION_OUT_OF_STATE")
  AND compartment_level_2 = "PAROLE"
  AND inflow_from_level_1 IN ("INCARCERATION", "PENDING_SUPERVISION")
"""

VIEW_BUILDER: TaskCompletionEventBigQueryViewBuilder = (
    TaskCompletionEventBigQueryViewBuilder(
        completion_event_type=TaskCompletionEventType.RELEASE_TO_PAROLE,
        description=_DESCRIPTION,
        completion_event_query_template=_QUERY_TEMPLATE,
        sessions_dataset=dataset_config.SESSIONS_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
