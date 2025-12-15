# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Defines a view that shows all releases from solitary confinement.
"""

from recidiviz.calculator.query.state import dataset_config
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateAgnosticTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_QUERY_TEMPLATE = """
/* Get any SOLITARY_CONFINEMENT housing_unit_type_collapsed_solitary_session that ends. An end with a date_gap will
indicate that the resident was discharged out of incarceration, and a new adjacent session will indicate the 
housing_unit_type_collapsed_solitary_session has changed to a different type. */
#TODO(#27658) Account for false transitions
SELECT 
    state_code,
    person_id,
    end_date_exclusive AS completion_event_date
FROM `{project_id}.{sessions_dataset}.housing_unit_type_non_protective_custody_solitary_sessions_materialized` 
WHERE housing_unit_type_collapsed_solitary LIKE "%SOLITARY%"
AND end_date_exclusive IS NOT NULL
"""

VIEW_BUILDER: StateAgnosticTaskCompletionEventBigQueryViewBuilder = StateAgnosticTaskCompletionEventBigQueryViewBuilder(
    completion_event_type=TaskCompletionEventType.TRANSFER_OUT_OF_SOLITARY_CONFINEMENT,
    description=__doc__,
    completion_event_query_template=_QUERY_TEMPLATE,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
