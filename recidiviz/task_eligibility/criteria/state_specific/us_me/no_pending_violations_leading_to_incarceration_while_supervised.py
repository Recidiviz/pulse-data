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

"""Defines a criteria view that shows spans of time where
clients are NOT currently incarcerated because of a pending violation. 

Context: clients on supervision can be subject to violations. These violations sometimes
result in incarceration. In cases where they result in incarceration, clients are ineligible
for ET until the resolution for that violation is availabl; i.e. they are ineligible 
while they are pending.
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

_CRITERIA_NAME = "US_ME_NO_PENDING_VIOLATIONS_LEADING_TO_INCARCERATION_WHILE_SUPERVISED"

_DESCRIPTION = """Defines a criteria view that shows spans of time where
clients are NOT currently incarcerated because of a pending violation.

Context: clients on supervision can be subject to violations. These violations sometimes
result in incarceration. In cases where they result in incarceration, clients are ineligible
for ET until the resolution for that violation is availabl; i.e. they are ineligible 
while they are pending.
"""

_QUERY_TEMPLATE = """
WITH pending_violations_resulting_in_arrest AS (
  SELECT 
    peid.state_code,
    peid.person_id,
    MIN(SAFE_CAST(LEFT(v.Toll_Start_Date, 10) AS DATE)) AS start_date,
  FROM `{project_id}.{raw_data_up_to_date_views_dataset}.CIS_480_VIOLATION_latest` v
  INNER JOIN {project_id}.{normalized_state_dataset}.state_person_external_id peid
    ON peid.external_id = v.Cis_100_Client_Id
      AND id_type ='US_ME_DOC'
  -- Violation is pending
  WHERE v.Cis_4800_Violation_Finding_Cd IS NULL
  -- Violation resulted in an arrest
    AND v.Cis_4009_Toll_Violation_Cd = '60'
    AND Logical_Delete_Ind != 'Y'
  GROUP BY 1,2
)

SELECT 
  state_code,
  person_id,
  start_date,
  # Pending violations only valid for 3 years
  DATE_ADD(start_date, INTERVAL 3 YEAR) AS end_date,
  False AS meets_criteria,
  TO_JSON(STRUCT('Pending Violation - Incarcerated' AS current_status, 
                 start_date AS violation_date)) AS reason
FROM pending_violations_resulting_in_arrest
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_ME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_ME, instance=DirectIngestInstance.PRIMARY
        ),
        meets_criteria_default=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
