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
# ============================================================================
"""Describes spans of time when someone is enrolled in a functional literacy program"""
from google.cloud import bigquery

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_AZ_ENROLLED_IN_FUNCTIONAL_LITERACY"

_DESCRIPTION = """Describes spans of time when someone is enrolled in a functional literacy program"""

_QUERY_TEMPLATE = """
    SELECT
      state_code,
      person_id,
      start_date,
      COALESCE(discharge_date, CAST(NULL AS DATE)) AS end_date,
      TRUE AS meets_criteria,
      TO_JSON(STRUCT(
                start_date AS enrollment_date
            )) AS reason,
      start_date AS enrollment_date,
    FROM
    #TODO(#33858): Ingest into state task deadline or find some way to view this historically
      `{project_id}.{normalized_state_dataset}.state_program_assignment`
    WHERE state_code = 'US_AZ'
    AND participation_status_raw_text IN ('PARTICIPATING')
    AND program_id LIKE '%MAN%LIT%'
    AND start_date != COALESCE(discharge_date, CAST(NULL AS DATE))
    #TODO(#33737): Look into multiple span cases for residents participating in MAN-LIT programs
    QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code, person_id ORDER BY start_date ASC) = 1
"""

_REASONS_FIELDS = [
    ReasonsField(
        name="enrollment_date",
        type=bigquery.enums.StandardSqlTypeNames.DATE,
        description="Date at which a resident enrolled in a functional literacy program",
    ),
]

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        reasons_fields=_REASONS_FIELDS,
        state_code=StateCode.US_AZ,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
