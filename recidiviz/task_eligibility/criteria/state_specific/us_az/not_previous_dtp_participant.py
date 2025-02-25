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
"""Describes spans of time when someone was a DTP participant"""
from google.cloud import bigquery

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_AZ_NOT_PREVIOUS_DTP_PARTICIPANT"

_DESCRIPTION = """Describes spans of time when someone was a DTP participant"""

_QUERY_TEMPLATE = """
    SELECT
      state_code,
      person_id,
      release_date AS start_date,
      CAST(NULL AS DATE) AS end_date,
      FALSE AS meets_criteria,
      TO_JSON(STRUCT(
                release_date AS dtp_transition_date
            )) AS reason,
      release_date AS dtp_transition_date,
    FROM
      `{project_id}.{normalized_state_dataset}.state_incarceration_period`
    WHERE
      state_code = 'US_AZ'
      AND release_reason_raw_text = 'DRUG TRANSITION RELEASE'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code, person_id ORDER BY start_date) = 1
"""

_REASONS_FIELDS = [
    ReasonsField(
        name="dtp_transition_date",
        type=bigquery.enums.StandardSqlTypeNames.DATE,
        description="The date at which someone was enrolled in DTP previously",
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
        meets_criteria_default=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
