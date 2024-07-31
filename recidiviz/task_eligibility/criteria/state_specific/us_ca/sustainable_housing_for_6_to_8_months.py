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
# =============================================================================
"""Defines a criteria span view that shows spans of time during which someone on
supervision in California had sustainable housing for between 6 and 8 months.
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
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    spans_within_x_and_y_months_of_start_date,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_US_CA_CRITERIA_DATASET = task_eligibility_criteria_state_specific_dataset(
    StateCode.US_CA
)

_CRITERIA_NAME = "US_CA_SUSTAINABLE_HOUSING_FOR_6_TO_8_MONTHS"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which
someone on supervision in California had sustainable housing for between 6 and 8 months.
"""

_QUERY_TEMPLATE = f"""
WITH {spans_within_x_and_y_months_of_start_date(
    x_months=6,
    y_months=8,
    start_date_plus_x_months_name_in_reason_blob='six_months_sustainable_housing',
    dataset = 'us_ca_criteria_dataset',
    table_view= "housing_type_is_not_transient_materialized")}
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    state_code=StateCode.US_CA,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    us_ca_criteria_dataset=_US_CA_CRITERIA_DATASET,
    reasons_fields=[
        ReasonsField(
            name="six_months_sustainable_housing",
            type=bigquery.enums.SqlTypeNames.DATE,
            description="Date when the person attained sustainable housing for 6 months.",
        )
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
