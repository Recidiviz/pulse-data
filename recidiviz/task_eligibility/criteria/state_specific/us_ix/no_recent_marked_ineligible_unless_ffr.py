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

"""Defines a criteria view that shows spans of time for which folks on supervision 
are marked ineligible in the Workflows dashboard for reasons other than fines and fees (FFR)"""

from google.cloud import bigquery

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_IX_NO_RECENT_MARKED_INELIGIBLE_UNLESS_FFR"

_QUERY_TEMPLATE = """
WITH denial_reasons_spans AS (
  SELECT 
    pei.state_code,
    pei.person_id,
    snooze_start_date AS start_date,
    IF( # if the snooze was not still active as of the last day we have data for (presumably today) 
        MAX(as_of)<(SELECT MAX(as_of) FROM `{project_id}.export_archives.workflows_snooze_status_archive`),
        # then take the last day it was active 
        MAX(as_of),
        # else assume it is still active and assign an end date of null 
        NULL) AS end_date,
    STRING_AGG(DISTINCT denial_reason, ' - ' ORDER BY denial_reason) AS denial_reasons_str,
  FROM `{project_id}.export_archives.workflows_snooze_status_archive` a,
  UNNEST(denial_reasons) AS denial_reason
  INNER JOIN `{project_id}.workflows_views.person_id_to_external_id_materialized` pei
    ON pei.state_code = 'US_IX'
    AND UPPER(pei.person_external_id) = UPPER(a.person_external_id)
    AND pei.system_type = "SUPERVISION"
  WHERE a.state_code = 'US_ID'
    AND opportunity_type = 'LSU'
  GROUP BY 1,2,3
)

SELECT 
  state_code,
  person_id,
  start_date,
  end_date,
  (denial_reasons_str = 'FFR') AS meets_criteria,
  TO_JSON(STRUCT(
    denial_reasons_str AS denial_reasons_str
  )) AS reason,
  denial_reasons_str,
FROM denial_reasons_spans"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=__doc__,
        state_code=StateCode.US_IX,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="denial_reasons_str",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="List of distinct denial reasons for the latest snooze",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
