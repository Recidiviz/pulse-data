# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""
Defines a criteria span view that shows spans of time during which a client
requires quarterly schedule home contacts.
"""

from google.cloud import bigquery

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TX_QUARTERLY_HOME_CONTACT_REQUIRED"

_DESCRIPTION = """
Defines a criteria span view that shows spans of time during which a client
requires quarterly schedule home contacts.
"""

_REASON_QUERY = """
SELECT
    person_id,
    state_code,
    start_date,
    end_date,
    TRUE AS meets_criteria,
    TO_JSON(STRUCT(
        contact_type,
        frequency_in_months
    )) AS reason,
    contact_type,
    frequency_in_months,
FROM
    `{project_id}.analyst_data.us_tx_contact_cadence_spans_materialized`
WHERE
    frequency_in_months = 3
    AND contact_type = "SCHEDULED HOME"
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_REASON_QUERY,
        state_code=StateCode.US_TX,
        meets_criteria_default=False,
        reasons_fields=[
            ReasonsField(
                name="contact_type",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Type of contact due.",
            ),
            ReasonsField(
                name="frequency_in_months",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Contact cadence in months.",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
