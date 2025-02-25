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
"""Describes the spans of time when a client has a fines/fees balance below 500"""
from google.cloud import bigquery

from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.preprocessed_views_query_fragments import (
    has_unpaid_fines_fees_balance,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "HAS_FINES_FEES_BALANCE_BELOW_500"

_DESCRIPTION = """Describes the spans of time when a client has a fines/fees balance below 500.
    """

_QUERY_TEMPLATE = f"""
    {has_unpaid_fines_fees_balance(
        fee_type = "SUPERVISION_FEES",
        unpaid_balance_criteria = "<= 500",
        unpaid_balance_field= "unpaid_balance_within_supervision_session")}
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        analyst_dataset=ANALYST_VIEWS_DATASET,
        reasons_fields=[
            ReasonsField(
                name="amount_owed",
                type=bigquery.enums.StandardSqlTypeNames.FLOAT64,
                description="Amount that a client owes in fines/fees",
            ),
        ],
    )
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
