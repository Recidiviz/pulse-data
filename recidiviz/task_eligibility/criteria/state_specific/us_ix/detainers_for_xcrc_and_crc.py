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
Defines a criteria view that currently incarcerated individuals
who do have an active detainer or hold for Idaho XCRC.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_ix_query_fragments import (
    DETAINER_TYPE_LST,
    HOLD_TYPE_LST,
    detainer_span,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_IX_DETAINERS_FOR_XCRC_AND_CRC"

_DESCRIPTION = """
Defines a criteria view that currently incarcerated individuals
who do have an active detainer or hold for Idaho XCRC and CRC.
"""

_QUERY_TEMPLATE = f"""
{detainer_span(DETAINER_TYPE_LST + HOLD_TYPE_LST)}
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        analyst_dataset=ANALYST_VIEWS_DATASET,
        state_code=StateCode.US_IX,
        reasons_fields=[
            ReasonsField(
                name="latest_detainer_start_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Latest detainer start date",
            ),
            ReasonsField(
                name="latest_detainer_type",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Latest detainer type",
            ),
            ReasonsField(
                name="latest_detainer_status",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Latest detainer status",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
