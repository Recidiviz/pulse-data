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
"""
Defines a criteria span view that shows spans of time during which someone has
been incarcerated at least 30 days in the same facility.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "INCARCERATED_AT_LEAST_30_DAYS_IN_SAME_FACILITY"

_QUERY_TEMPLATE = f"""
WITH loc_sessions AS (
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date_exclusive AS end_date,
        facility,
    FROM `{{project_id}}.{{sessions_dataset}}.location_sessions_materialized`
    WHERE facility IS NOT NULL
),

{create_sub_sessions_with_attributes(
    table_name="loc_sessions", 
)},

loc_sessions_no_duplicates AS (
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        STRING_AGG(facility ORDER BY facility) AS facility,
    FROM sub_sessions_with_attributes
    GROUP BY 1,2,3,4
),

critical_date_spans AS (
    SELECT
        state_code,
        person_id,
        start_date AS start_datetime,
        end_date AS end_datetime,
        DATE_ADD(start_date, INTERVAL 30 DAY) AS critical_date,
    FROM ({aggregate_adjacent_spans(table_name='loc_sessions_no_duplicates',
                                    attribute='facility',)})
),
{critical_date_has_passed_spans_cte()}

SELECT
    cd.state_code,
    cd.person_id,
    cd.start_date,
    cd.end_date,
    cd.critical_date_has_passed AS meets_criteria,
    TO_JSON(STRUCT(
        cd.critical_date AS thirty_days_in_same_facility_date
    )) AS reason,
    cd.critical_date AS thirty_days_in_same_facility_date,
FROM critical_date_has_passed_spans cd
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = StateAgnosticTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    description=__doc__,
    sessions_dataset=SESSIONS_DATASET,
    reasons_fields=[
        ReasonsField(
            name="thirty_days_in_same_facility_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date when a client will have spent 30 days in the same facility",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
