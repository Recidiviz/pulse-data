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
"""Defines a criteria span view that shows the most recent span of time
for which someone has been moved to liberty
"""
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ID_NOT_AT_LIBERTY"

_DESCRIPTION = """Defines a criteria span view that shows the most recent span of time
for which someone has been moved to liberty"""

_QUERY_TEMPLATE = """
SELECT
  state_code,
  person_id,
  latest_movement_date AS start_date,
  CAST(NULL AS DATE) as end_date,
  FALSE AS meets_criteria,
  TO_JSON(STRUCT(latest_movement_date AS moved_to_history)) AS reason,
FROM (
  SELECT
    docno AS person_external_id,
    move_typ,
    CAST(LEFT(move_dtd, 10) AS DATE) AS latest_movement_date,
  FROM `{project_id}.{raw_data_up_to_date_views_dataset}.movement_latest`
  WHERE TRUE
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY docno
    ORDER BY move_dtd DESC
  ) = 1
)
INNER JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id`
  ON person_external_id = external_id
  AND id_type = 'US_ID_DOC'
WHERE move_typ = 'H'
"""
VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_ID,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_ID,
            instance=DirectIngestInstance.PRIMARY,
        ),
        meets_criteria_default=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
