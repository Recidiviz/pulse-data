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
"""Queries information pertinent to supervision overrides to Low in Nebraska."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import WORKFLOWS_VIEWS_DATASET
from recidiviz.calculator.query.state.views.workflows.us_ne.shared_ctes import (
    supervision_level_override_opportunity_query_template,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.eligibility_spans.us_ne.moderate_to_low_supervision_override import (
    VIEW_BUILDER as US_NE_MODERATE_TO_LOW_SUPERVISION_OVERRIDE_VIEW_BUILDER,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_NE_OVERRIDE_MODERATE_TO_LOW_RECORD_VIEW_NAME = (
    "us_ne_override_moderate_to_low_record"
)

US_NE_OVERRIDE_MODERATE_TO_LOW_RECORD_QUERY_TEMPLATE = supervision_level_override_opportunity_query_template(
    tes_task_query_view_builder=US_NE_MODERATE_TO_LOW_SUPERVISION_OVERRIDE_VIEW_BUILDER,
)

US_NE_OVERRIDE_MODERATE_TO_LOW_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_NE_OVERRIDE_MODERATE_TO_LOW_RECORD_VIEW_NAME,
    view_query_template=US_NE_OVERRIDE_MODERATE_TO_LOW_RECORD_QUERY_TEMPLATE,
    description=__doc__,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    workflows_views_dataset=WORKFLOWS_VIEWS_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_NE
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_NE_OVERRIDE_MODERATE_TO_LOW_RECORD_VIEW_BUILDER.build_and_print()
