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
Queries information to inform JII about ID LSU eligibility
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    join_current_task_eligibility_spans_with_external_id,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.utils.us_ix_query_fragments import lsir_spans
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_IX_TRANSFER_TO_LIMITED_SUPERVISION_VIEW_NAME = (
    "us_ix_transfer_to_limited_supervision_jii_record"
)

US_IX_TRANSFER_TO_LIMITED_SUPERVISION_DESCRIPTION = """
    Queries information to inform JII about ID LSU eligibility
    """

US_IX_TRANSFER_TO_LIMITED_SUPERVISION_QUERY_TEMPLATE = f"""
WITH eligible_and_almost_eligible AS (
  {join_current_task_eligibility_spans_with_external_id(
    state_code="'US_IX'",
    tes_task_query_view='complete_transfer_to_limited_supervision_jii_form_materialized',
    id_type="'US_IX_DOC'",
    eligible_and_almost_eligible_only=True,
  )}
),

{lsir_spans()},

current_lsir_span AS (
    SELECT
        *
    FROM lsir_spans
    WHERE CURRENT_DATE BETWEEN score_span_start_date AND score_span_end_date
)

SELECT
    eae.external_id,
    eae.state_code,
    eae.ineligible_criteria,
    eae.is_eligible,
    JSON_QUERY_ARRAY(reasons) AS array_reasons,
    cls.lsir_level,
FROM eligible_and_almost_eligible eae
LEFT JOIN current_lsir_span cls
    USING(person_id)
"""

US_IX_TRANSFER_TO_LIMITED_SUPERVISION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_IX_TRANSFER_TO_LIMITED_SUPERVISION_VIEW_NAME,
    view_query_template=US_IX_TRANSFER_TO_LIMITED_SUPERVISION_QUERY_TEMPLATE,
    description=US_IX_TRANSFER_TO_LIMITED_SUPERVISION_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_IX
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_IX_TRANSFER_TO_LIMITED_SUPERVISION_VIEW_BUILDER.build_and_print()
