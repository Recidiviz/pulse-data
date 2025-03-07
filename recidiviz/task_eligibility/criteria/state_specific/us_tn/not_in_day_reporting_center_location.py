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
# ============================================================================
"""Spans of time during which a client in TN is not at a DRC (Day Reporting Center) site
"""

from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TN_NOT_IN_DAY_REPORTING_CENTER_LOCATION"

_QUERY_TEMPLATE = f"""
WITH drc_sessions AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive,
        FALSE AS meets_criteria,
        supervision_office
    FROM `{{project_id}}.sessions.compartment_sub_sessions_materialized`
    WHERE supervision_office LIKE 'DRC%'
        AND state_code = "US_TN"
)
SELECT
    state_code,
    person_id,
    start_date,
    end_date_exclusive AS end_date,
    FALSE AS meets_criteria,
    TO_JSON(STRUCT(supervision_office AS drc_location)) AS reason,
    supervision_office AS drc_location
FROM ({aggregate_adjacent_spans(table_name='drc_sessions',
                                    attribute=['supervision_office'],
                                    end_date_field_name="end_date_exclusive")})

"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=__doc__,
    state_code=StateCode.US_TN,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    # Set default to True because we only created spans of *ineligibility* in the query
    # above, and we want to assume that folks are eligible by default otherwise.
    meets_criteria_default=True,
    reasons_fields=[
        ReasonsField(
            name="drc_location",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="DRC supervision office name",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
