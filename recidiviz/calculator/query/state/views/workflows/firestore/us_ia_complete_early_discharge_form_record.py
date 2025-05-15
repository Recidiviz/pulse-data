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
"""Query for relevant metadata needed to support early discharge opportunity in IA"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    join_current_task_eligibility_spans_with_external_id,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.collapsed_task_eligibility_spans import (
    build_collapsed_tes_spans_view_materialized_address,
)
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.eligibility_spans.us_ia.complete_early_discharge_form import (
    VIEW_BUILDER as TES_VIEW_BUILDER,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_IA_COMPLETE_EARLY_DISCHARGE_FORM_RECORD_VIEW_NAME = (
    "us_ia_complete_early_discharge_form_record"
)

US_IA_COMPLETE_EARLY_DISCHARGE_FORM_RECORD_DESCRIPTION = """
Query for relevant metadata needed to support early discharge opportunity in IA"""

_COLLAPSED_TES_SPANS_ADDRESS = build_collapsed_tes_spans_view_materialized_address(
    TES_VIEW_BUILDER
)

US_IA_COMPLETE_EARLY_DISCHARGE_FORM_RECORD_QUERY_TEMPLATE = f"""
{join_current_task_eligibility_spans_with_external_id(
    state_code="'US_IA'",
    tes_task_query_view='complete_early_discharge_form_materialized',
    id_type="'US_IA_OFFENDERCD'",
    eligible_and_almost_eligible_only=True,
    tes_collapsed_view_for_eligible_date=_COLLAPSED_TES_SPANS_ADDRESS,
)}
"""

US_IA_COMPLETE_EARLY_DISCHARGE_FORM_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_IA_COMPLETE_EARLY_DISCHARGE_FORM_RECORD_VIEW_NAME,
    view_query_template=US_IA_COMPLETE_EARLY_DISCHARGE_FORM_RECORD_QUERY_TEMPLATE,
    description=US_IA_COMPLETE_EARLY_DISCHARGE_FORM_RECORD_DESCRIPTION,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_IA
    ),
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_IA_COMPLETE_EARLY_DISCHARGE_FORM_RECORD_VIEW_BUILDER.build_and_print()
