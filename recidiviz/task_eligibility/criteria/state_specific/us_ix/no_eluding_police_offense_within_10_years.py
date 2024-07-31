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
"""
This file creates a spans of time where a person is ineligible if they have an IX eluding
police offense within the last 10 years.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    x_time_from_ineligible_offense,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_IX_NO_ELUDING_POLICE_OFFENSE_WITHIN_10_YEARS"

_DESCRIPTION = """
This file creates a spans of time where a person is ineligible if they have an IX eluding 
police offense within the last 10 years.
"""
_INELIGIBLE_STATUTES = [
    "I49-1404(1)",
    "I49-1404 {{F}}",
    "I49-1404(2)",
    "I49-1404 {{F}} AB",
    "I49-1404(2)(A)",
]
_WHERE_CLAUSE = "AND state_code = 'US_IX'"

_QUERY_TEMPLATE = f"""WITH {x_time_from_ineligible_offense(statutes_list = _INELIGIBLE_STATUTES,
                                                              date_part='YEAR',
                                                              date_interval=10,
                                                              additional_where_clause = _WHERE_CLAUSE,
                                                              start_date_column='date_imposed',
                                                              statute_date_name="most_recent_eluding_police_date"
                                                           )}
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_IX,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        sessions_dataset=SESSIONS_DATASET,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="ineligible_offenses",
                type=bigquery.enums.SqlTypeNames.RECORD,
                description="Offenses that make the person ineligible for the task",
            ),
            ReasonsField(
                name="ineligible_offenses_descriptions",
                type=bigquery.enums.SqlTypeNames.RECORD,
                description="Descriptions of the ineligible offenses",
            ),
            ReasonsField(
                name="most_recent_eluding_police_date",
                type=bigquery.enums.SqlTypeNames.DATE,
                description="Most recent eluding police date",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
