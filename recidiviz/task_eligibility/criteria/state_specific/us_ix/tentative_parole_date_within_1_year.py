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
someone is within 1 year of their tentative parole date.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_ix_query_fragments import date_within_time_span
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_IX_TENTATIVE_PAROLE_DATE_WITHIN_1_YEAR"

_DESCRIPTION = """
Defines a criteria span view that shows spans of time during which
someone is within 1 year of their tentative parole date.
"""

_QUERY_TEMPLATE = f"""
{date_within_time_span(meets_criteria_leading_window_time=1,
                        critical_date_column='tentative_parole_date',
                        date_part='YEAR')}
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    description=_DESCRIPTION,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    state_code=StateCode.US_IX,
    reasons_fields=[
        ReasonsField(
            name="tentative_parole_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="The date on which the individual is tentatively scheduled to be released on parole.",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
