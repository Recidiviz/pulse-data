# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
""" Defines a view that shows all releases from administrative solitary confinement to a
non-solitary compartment. If a client goes from ADMIN --> TEMPORARY --> ADMIN --> GENERAL only the last
transition will be counted.
"""

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state import dataset_config
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateAgnosticTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_QUERY_TEMPLATE = f"""
/* Get any ADMIN solitary confinement housing_unit_type_session that ends in a housing_unit_type
that is not solitary (including nulls). */
#TODO(#27658) Account for false transitions
WITH housing_units AS (
    SELECT 
    state_code,
    person_id,
    end_date_exclusive AS completion_event_date,
    housing_unit_type,
    LEAD (housing_unit_type) OVER (PARTITION BY person_id, date_gap_id ORDER BY start_date ASC) AS next_housing_type,
FROM `{{project_id}}.{{sessions_dataset}}.housing_unit_type_sessions_materialized` 
)
SELECT 
    state_code,
    person_id,
    completion_event_date
FROM housing_units
WHERE housing_unit_type = 'ADMINISTRATIVE_SOLITARY_CONFINEMENT'
    -- do not include open periods 
    AND {nonnull_end_date_clause('completion_event_date')}<= CURRENT_DATE('US/Eastern')
    -- only chose sub sessions that transfer to a non solitary housing_unit_type
    AND COALESCE(next_housing_type, '') NOT LIKE '%SOLITARY%'
"""

VIEW_BUILDER: StateAgnosticTaskCompletionEventBigQueryViewBuilder = StateAgnosticTaskCompletionEventBigQueryViewBuilder(
    completion_event_type=TaskCompletionEventType.TRANSFER_OUT_OF_ADMINISTRATIVE_SOLITARY_CONFINEMENT,
    description=__doc__,
    completion_event_query_template=_QUERY_TEMPLATE,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
