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
"""
Defines a criteria span view that shows spans of time during which
someone in ID is in a Community Reentry Center facility
"""
from google.cloud import bigquery

from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_ix_query_fragments import (
    IX_CRC_FACILITIES,
    ix_crc_facilities_in_location_sessions,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_IX_IN_CRC_FACILITY"

_DESCRIPTION = """
Defines a criteria span view that shows spans of time during which
someone in ID is in a Community Reentry Center facility
"""

_ADDITIONAL_COLUMNS = """TRUE AS meets_criteria,
    TO_JSON(STRUCT(start_date AS crc_start_date, facility_name AS facility_name)) AS reason,
    start_date AS crc_start_date,
    facility_name"""

_QUERY_TEMPLATE = ix_crc_facilities_in_location_sessions(
    crc_facilities_list=IX_CRC_FACILITIES, additional_columns=_ADDITIONAL_COLUMNS
)

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_IX,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        sessions_dataset=SESSIONS_DATASET,
        reasons_fields=[
            ReasonsField(
                name="crc_start_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date when the person started in the CRC facility",
            ),
            ReasonsField(
                name="facility_name",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Name of the CRC facility",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
