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
"""Defines a criteria span view that shows spans of time during which someone is not
    eligible for traditional admin supervision """
from google.cloud import bigquery

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_PA_NOT_ELIGIBLE_FOR_ADMIN_SUPERVISION"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which someone is not
    eligible for traditional admin supervision """

_QUERY_TEMPLATE = """
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    NOT is_eligible AS meets_criteria,
    TO_JSON(STRUCT(start_date AS admin_eligibility_date)) AS reason,
    start_date AS admin_eligibility_date,
FROM `{project_id}.{spans_dataset}.complete_transfer_to_administrative_supervision_request_materialized`
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    state_code=StateCode.US_PA,
    spans_dataset="""task_eligibility_spans_us_pa""",
    reasons_fields=[
        ReasonsField(
            name="admin_eligibility_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date that someone became eligible for administrative supervision",
        ),
    ],
    meets_criteria_default=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
