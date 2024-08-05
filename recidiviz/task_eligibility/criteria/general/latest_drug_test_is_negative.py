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
"""Describes the spans of time when a client's latest drug screen is negative."""

from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "LATEST_DRUG_TEST_IS_NEGATIVE"

_DESCRIPTION = (
    """Describes the spans of time when a client's latest drug screen is negative."""
)


_QUERY_TEMPLATE = f"""
    WITH screens AS (
        SELECT
            state_code,
            person_id,
            drug_screen_date AS start_date,
            LEAD(drug_screen_date) OVER(PARTITION BY person_id ORDER BY drug_screen_date ASC) AS end_date,
            NOT is_positive_result AS meets_criteria,
            result_raw_text_primary AS latest_drug_screen_result,
            drug_screen_date AS latest_drug_screen_date,
        FROM
            (
                SELECT *
                FROM `{{project_id}}.{{sessions_dataset}}.drug_screens_preprocessed_materialized`
                -- The preprocessed view can have multiple tests per person-day if there are different sample types.
                -- For the purposes of this criteria we just want to keep 1 test per person-day and prioritize positive
                -- results
                QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, drug_screen_date ORDER BY is_positive_result DESC,
                                                                                             sample_type) = 1
            )
    ),
    sessionized_cte AS (
    {aggregate_adjacent_spans(table_name='screens',
                       attribute=['latest_drug_screen_result','latest_drug_screen_date','meets_criteria'],
                       end_date_field_name='end_date')}
    )
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        meets_criteria,
        TO_JSON(STRUCT(latest_drug_screen_result AS latest_drug_screen_result,
                        latest_drug_screen_date AS latest_drug_screen_date
        )) AS reason,
        latest_drug_screen_result,
        latest_drug_screen_date,
    FROM sessionized_cte
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        sessions_dataset=SESSIONS_DATASET,
        reasons_fields=[
            ReasonsField(
                name="latest_drug_screen_result",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Result of latest drug screen",
            ),
            ReasonsField(
                name="latest_drug_screen_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date of latest drug screen",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
