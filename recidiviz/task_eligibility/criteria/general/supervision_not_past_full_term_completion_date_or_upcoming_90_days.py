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
"""Defines a criteria span view that shows spans of time during which someone is not
past their supervision full term completion date (sentence projected max completion date) or within 90 days of it.
However, this criteria will be FALSE if no projected max completion date is set.

This criteria is therefore not fully the logical opposite of the
`SUPERVISION_PAST_FULL_TERM_COMPLETION_DATE_OR_UPCOMING_90_DAYS` view, since both criteria will be FALSE if there
is no projected max completion date"""
from google.cloud import bigquery

from recidiviz.task_eligibility.criteria.general.supervision_past_full_term_completion_date_or_upcoming_90_days import (
    VIEW_BUILDER as past_full_term_completion_builder,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "SUPERVISION_NOT_PAST_FULL_TERM_COMPLETION_DATE_OR_UPCOMING_90_DAYS"

_DESCRIPTION = __doc__

_QUERY_TEMPLATE = f"""
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    NOT meets_criteria AS meets_criteria,
    reason,
    JSON_EXTRACT(reason, "$.eligible_date") AS eligible_date,
FROM `{{project_id}}.{{criteria_dataset}}.{past_full_term_completion_builder.view_id}_materialized`
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = StateAgnosticTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    description=_DESCRIPTION,
    criteria_dataset=past_full_term_completion_builder.dataset_id,
    reasons_fields=[
        ReasonsField(
            name="eligible_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date of client's projected supervision full term completion",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
