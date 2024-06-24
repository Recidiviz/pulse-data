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
# ============================================================================
"""This criteria view builder defines spans of time that clients are not on supervision after participating in
 SAI (Special Alternative Incarceration) based on supervision level raw text values that contain SAI.
"""
from google.cloud import bigquery

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_criteria_state_specific_dataset,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_MI_SUPERVISION_OR_SUPERVISION_OUT_OF_STATE_LEVEL_IS_NOT_SAI"

_DESCRIPTION = """This criteria view builder defines spans of time that clients are not on supervision after participating
in SAI (Special Alternative Incarceration).
"""

_QUERY_TEMPLATE = """
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    NOT meets_criteria AS meets_criteria,
    reason,
    CAST(JSON_VALUE(reason, "$.supervision_level_is_sai") AS BOOL) AS supervision_level_is_sai,
FROM `{project_id}.{criteria_dataset}.supervision_or_supervision_out_of_state_level_is_sai_materialized`
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        state_code=StateCode.US_MI,
        meets_criteria_default=True,
        criteria_dataset=task_eligibility_criteria_state_specific_dataset(
            StateCode.US_MI
        ),
        reasons_fields=[
            ReasonsField(
                name="supervision_level_is_sai",
                type=bigquery.enums.SqlTypeNames.BOOLEAN,
                description="#TODO(#29059): Add reasons field description",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
