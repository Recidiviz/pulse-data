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
"""Spans during which someone is in Restrictive Housing and has been subject to a D1 sanction
since their Restrictive Housing placement began.
"""
from google.cloud import bigquery

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_MO_NO_D1_SANCTION_AFTER_RESTRICTIVE_HOUSING_START"

_DESCRIPTION = """Spans during which someone is in Restrictive Housing and has not been
subject to a D1 sanction since their Restrictive Housing placement began.
"""

_QUERY_TEMPLATE = """
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        NOT meets_criteria AS meets_criteria,
        reason,
        JSON_EXTRACT(reason, "$.latest_d1_sanction_start_date") AS latest_d1_sanction_start_date,
        JSON_EXTRACT(reason, "$.restrictive_housing_start_date") AS restrictive_housing_start_date,
    FROM `{project_id}.task_eligibility_criteria_us_mo.d1_sanction_after_restrictive_housing_start_materialized`
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    state_code=StateCode.US_MO,
    criteria_name=_CRITERIA_NAME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    description=_DESCRIPTION,
    meets_criteria_default=True,
    reasons_fields=[
        ReasonsField(
            name="latest_d1_sanction_start_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Latest effective date of a D1 sanction for the person.",
        ),
        ReasonsField(
            name="restrictive_housing_start_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Start date of the person's current Restrictive Housing assignment.",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
