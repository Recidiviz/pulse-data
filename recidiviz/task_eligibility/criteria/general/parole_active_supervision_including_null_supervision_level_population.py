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
# =============================================================================
"""Selects all spans of time in which a person is on supervision, and are actively supervised
as defined by excluding certain compartments and supervision levels such as in custody,
bench warrant, absconsion, or unknown, and including null supervision levels. Uses and
mimics the candidate population of the same name.
"""

from google.cloud import bigquery

from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(#45794): Deprecate this view once candidate populations have been incorporated into Tasks
_CRITERIA_NAME = "PAROLE_ACTIVE_SUPERVISION_INCLUDING_NULL_SUPERVISION_LEVEL_POPULATION"

_QUERY_TEMPLATE = """
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    TRUE AS meets_criteria,
    TO_JSON(STRUCT(
        TRUE AS on_parole_active_supervision_including_null_supervision_level
    )) AS reason,
    TRUE AS on_parole_active_supervision_including_null_supervision_level,
FROM `{project_id}.task_eligibility_candidates_general.parole_active_supervision_including_null_supervision_level_population_materialized`
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = StateAgnosticTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    description=__doc__,
    meets_criteria_default=False,
    reasons_fields=[
        ReasonsField(
            name="on_parole_active_supervision_including_null_supervision_level",
            type=bigquery.enums.StandardSqlTypeNames.BOOL,
            description="Specifies whether client is on parole and actively supervised.",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
