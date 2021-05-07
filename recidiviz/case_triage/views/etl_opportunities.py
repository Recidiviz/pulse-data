# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Implements BQ View to fetch information on new top opportunities.

To start, this view only identifies people who are overdue for supervision downgrades.

TODO(#6615): This view is currently specific to Idaho, and it should be evolved
"""
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.case_triage.opportunities.types import OpportunityType
from recidiviz.case_triage.state_utils.us_id import US_ID_ASSESSMENT_SCORE_RANGE
from recidiviz.case_triage.views.dataset_config import VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


TOP_OPPORTUNITIES_QUERY_VIEW = f"""
WITH overdue_downgrades AS (
  SELECT
    state_code,
    supervising_officer_external_id,
    person_external_id,
    '{OpportunityType.OVERDUE_DOWNGRADE.value}' AS opportunity_type,
    TO_JSON_STRING(STRUCT(assessment_score as assessmentScore, most_recent_assessment_date AS latestAssessmentDate)) AS opportunity_metadata
  FROM
    `{{project_id}}.{{case_triage_dataset}}.etl_clients`
  WHERE
    state_code = 'US_ID'
    AND case_type = 'GENERAL'
    AND assessment_score IS NOT NULL
    AND supervision_type NOT IN ('INTERNAL_UNKNOWN', 'INFORMAL_PROBATION')
    AND ({{assessment_scores_clause}})
)
SELECT
  {{columns}}
FROM
  overdue_downgrades
"""


def _get_assessment_score_clause() -> str:
    """This outputs a clause that checks whether a person is at a supervision level that
    is above what would be suggested by virtue of their assessment score and the state's
    known policies.
    """
    return "\n  OR ".join(
        [
            f"(gender = '{gender.value}' AND supervision_level = '{level.value}' AND assessment_score < {score_range[0]})"
            for gender, subdict in US_ID_ASSESSMENT_SCORE_RANGE.items()
            for level, score_range in subdict.items()
        ]
    )


TOP_OPPORTUNITIES_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=VIEWS_DATASET,
    view_id="etl_opportunities",
    view_query_template=TOP_OPPORTUNITIES_QUERY_VIEW,
    case_triage_dataset=VIEWS_DATASET,
    assessment_scores_clause=_get_assessment_score_clause(),
    columns=[
        "state_code",
        "supervising_officer_external_id",
        "person_external_id",
        "opportunity_type",
        "opportunity_metadata",
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        TOP_OPPORTUNITIES_VIEW_BUILDER.build_and_print()
