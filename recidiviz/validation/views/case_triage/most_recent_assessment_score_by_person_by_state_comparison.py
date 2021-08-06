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
"""A view which provides a comparison of the most recent assessment score from
the state table vs. the etl_clients tables"""


from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import STATE_BASE_DATASET
from recidiviz.case_triage.views.dataset_config import VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config
from recidiviz.validation.views.case_triage.most_recent_assessment_by_person_by_state_comparison_query_template import (
    MOST_RECENT_ASSESSMENT_BY_PERSON_BY_STATE_COMPARISON_QUERY_TEMPLATE,
)

MOST_RECENT_ASSESSMENT_SCORE_BY_PERSON_BY_STATE_COMPARISON_VIEW_NAME = (
    "most_recent_assessment_score_by_person_by_state_comparison"
)

MOST_RECENT_ASSESSMENT_SCORE_BY_PERSON_BY_STATE_COMPARISON_DESCRIPTION = """ Comparison of the most recent assessment score within etl clients to the state tables."""

MOST_RECENT_ASSESSMENT_SCORE_BY_PERSON_BY_STATE_COMPARISON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=MOST_RECENT_ASSESSMENT_SCORE_BY_PERSON_BY_STATE_COMPARISON_VIEW_NAME,
    view_query_template=MOST_RECENT_ASSESSMENT_BY_PERSON_BY_STATE_COMPARISON_QUERY_TEMPLATE,
    description=MOST_RECENT_ASSESSMENT_SCORE_BY_PERSON_BY_STATE_COMPARISON_DESCRIPTION,
    state_dataset=STATE_BASE_DATASET,
    case_triage_dataset=VIEWS_DATASET,
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        MOST_RECENT_ASSESSMENT_SCORE_BY_PERSON_BY_STATE_COMPARISON_VIEW_BUILDER.build_and_print()
