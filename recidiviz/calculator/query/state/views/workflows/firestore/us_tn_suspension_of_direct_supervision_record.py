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
"""Collect relevant metadata to support Suspension of Direct Supervision in TN."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    join_current_task_eligibility_spans_with_external_id,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_SUSPENSION_OF_DIRECT_SUPERVISION_RECORD_VIEW_NAME = (
    "us_tn_suspension_of_direct_supervision_record"
)

# TODO(#33644): Finish adding necessary information to opportunity record.
US_TN_SUSPENSION_OF_DIRECT_SUPERVISION_RECORD_QUERY_TEMPLATE = f"""
    WITH base AS (
        {join_current_task_eligibility_spans_with_external_id(
            state_code="'US_TN'",
            tes_task_query_view="suspension_of_direct_supervision_materialized",
            id_type="'US_TN_DOC'",
            eligible_and_almost_eligible_only=True,
        )}
    )
    SELECT
        base.state_code,
        base.external_id,
        base.reasons,
        base.ineligible_criteria,
        base.is_eligible,
        base.is_almost_eligible,
        -- form information
        CAST(NULL AS ARRAY<STRING>) AS form_information_conviction_counties,
        CAST(NULL AS STRING) AS form_information_conviction_charge,
        CAST(NULL AS DATE) AS form_information_sentence_date,
        CAST(NULL AS STRING) AS form_information_supervision_duration,
        CAST(NULL AS STRING) AS form_information_supervision_office_location,
        STRUCT(
            CAST(NULL AS DATE) AS contact_date,
            CAST(NULL AS STRING) AS contact_type,
            CAST(NULL AS STRING) AS contact_comment
        ) AS form_information_latest_ncic,
    FROM base
"""

US_TN_SUSPENSION_OF_DIRECT_SUPERVISION_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_TN_SUSPENSION_OF_DIRECT_SUPERVISION_RECORD_VIEW_NAME,
    view_query_template=US_TN_SUSPENSION_OF_DIRECT_SUPERVISION_RECORD_QUERY_TEMPLATE,
    description=__doc__,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_TN,
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_SUSPENSION_OF_DIRECT_SUPERVISION_RECORD_VIEW_BUILDER.build_and_print()
