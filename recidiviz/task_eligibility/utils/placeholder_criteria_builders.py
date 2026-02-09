# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Helper methods that return criteria view builders with placeholder/stubbed data to
be used for populating a new criteria before the criteria query is developed.
"""
from typing import Dict, List

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import MAGIC_END_DATE
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)

# Default values to use for the placeholder reason columns
DEFAULT_REASON_BY_TYPE: Dict[bigquery.enums.StandardSqlTypeNames, str] = {
    bigquery.enums.StandardSqlTypeNames.BOOL: "FALSE",
    bigquery.enums.StandardSqlTypeNames.INT64: "1",
    bigquery.enums.StandardSqlTypeNames.STRING: "'placeholder'",
    bigquery.enums.StandardSqlTypeNames.ARRAY: "['placeholder1', 'placeholder2']",
    bigquery.enums.StandardSqlTypeNames.DATE: f"'{MAGIC_END_DATE}'",
}


def state_agnostic_placeholder_query_template(
    reasons_fields: List[ReasonsField],
) -> str:
    """Returns a query template for state-agnostic eligibility criteria spans with the
    provided |reason_query| set and the whole population set to meets_criteria = True.

    Args:
        reasons_fields (List[ReasonsField]): List of ReasonsField objects defining the
            reason columns to include in the query output.

    Returns:
        str: A SQL query template string that produces placeholder criteria spans.
    """
    reason_query = ",\n    ".join(
        [
            f"{DEFAULT_REASON_BY_TYPE[reason.type]} AS {reason.name}"
            for reason in reasons_fields
        ]
    )
    return f"""
SELECT
    state_code,
    person_id,
    start_date,
    DATE_ADD(end_date, INTERVAL 1 DAY) AS end_date,
    TRUE AS meets_criteria,
    TO_JSON(STRUCT({reason_query})) AS reason,
    {reason_query},
FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized`
"""


def state_specific_placeholder_criteria_view_builder(
    criteria_name: str,
    description: str,
    reasons_fields: List[ReasonsField],
    state_code: StateCode,
) -> StateSpecificTaskCriteriaBigQueryViewBuilder:
    """Returns a state specific criteria view builder to be used as a placeholder
    criteria during development.

    Args:
        criteria_name (str): Name of the criteria.
        description (str): Description of the criteria.
        reasons_fields (List[ReasonsField]): List of ReasonsField objects defining the
            reason columns.
        state_code (StateCode): The state code this criteria applies to.

    Returns:
        StateSpecificTaskCriteriaBigQueryViewBuilder: A view builder with placeholder
        query logic.
    """
    query_template = state_agnostic_placeholder_query_template(reasons_fields)
    return StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=state_code,
        criteria_name=criteria_name,
        criteria_spans_query_template=query_template,
        description=description,
        sessions_dataset=SESSIONS_DATASET,
        reasons_fields=reasons_fields,
    )


def state_agnostic_placeholder_criteria_view_builder(
    criteria_name: str, description: str, reasons_fields: List[ReasonsField]
) -> StateAgnosticTaskCriteriaBigQueryViewBuilder:
    """Returns a state agnostic criteria view builder to be used as a placeholder
    criteria during development.

    Args:
        criteria_name (str): Name of the criteria.
        description (str): Description of the criteria.
        reasons_fields (List[ReasonsField]): List of ReasonsField objects defining the
            reason columns.

    Returns:
        StateAgnosticTaskCriteriaBigQueryViewBuilder: A view builder with placeholder
        query logic.
    """
    query_template = state_agnostic_placeholder_query_template(reasons_fields)
    return StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        criteria_spans_query_template=query_template,
        description=description,
        sessions_dataset=SESSIONS_DATASET,
        reasons_fields=reasons_fields,
    )
