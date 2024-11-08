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
"""Defines a criteria span view that shows spans of time during which someone is
    eligible or almost eligible for overdue for ACIS DTP"""
from google.cloud import bigquery

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_AZ_ELIGIBLE_OR_ALMOST_ELIGIBLE_FOR_OVERDUE_FOR_ACIS_DTP"

_QUERY_TEMPLATE = """
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    NOT (is_eligible OR is_almost_eligible) AS meets_criteria,
    TO_JSON(STRUCT(is_eligible AS is_eligible,
                   is_almost_eligible AS is_almost_eligible)) AS reason,
    is_eligible AS is_eligible,
    is_almost_eligible AS is_almost_eligible
FROM `{project_id}.{spans_dataset}.overdue_for_acis_dtp_request_materialized`
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=__doc__,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        state_code=StateCode.US_AZ,
        spans_dataset="""task_eligibility_spans_us_az""",
        reasons_fields=[
            ReasonsField(
                name="is_eligible",
                type=bigquery.enums.StandardSqlTypeNames.BOOL,
                description="Is this person eligible for overdue_for_acis_dtp?",
            ),
            ReasonsField(
                name="is_almost_eligible",
                type=bigquery.enums.StandardSqlTypeNames.BOOL,
                description="Is this person almost_eligible for overdue_for_acis_dtp?",
            ),
        ],
        meets_criteria_default=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
