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
"""Defines a criteria span view that shows spans of time during which someone has
 served at least one year on parole.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
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

_CRITERIA_NAME = "ON_PAROLE_AT_LEAST_ONE_YEAR"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which someone has
 served at least one year on parole"""

_QUERY_TEMPLATE = f"""
WITH filtered_prio_supervision_sessions AS (
      SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive as end_date,
      FROM `{{project_id}}.{{sessions_dataset}}.prioritized_supervision_sessions_materialized`
      WHERE compartment_level_2 IN ('PAROLE', 'DUAL')
    ),
    critical_date_spans AS (
      SELECT
        state_code,
        person_id,
        start_date AS start_datetime,
        end_date AS end_datetime,
        DATE_ADD(start_date, INTERVAL 1 YEAR) AS critical_date,
      FROM ({aggregate_adjacent_spans(table_name='filtered_prio_supervision_sessions')})
    ),
    {critical_date_has_passed_spans_cte()}
    SELECT
        cd.state_code,
        cd.person_id,
        cd.start_date,
        cd.end_date,
        cd.critical_date_has_passed AS meets_criteria,
        TO_JSON(STRUCT(
            cd.critical_date AS eligible_date
        )) AS reason,
        cd.critical_date AS minimum_time_served_date,
    FROM critical_date_has_passed_spans cd
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        sessions_dataset=SESSIONS_DATASET,
        reasons_fields=[
            ReasonsField(
                name="minimum_time_served_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="#TODO(#29059): Add reasons field description",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
