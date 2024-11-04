# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Query for relevant metadata needed to support Work Release opportunity in Arkansas
"""
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

US_AR_WORK_RELEASE_RECORD_VIEW_NAME = "us_ar_work_release_record"

US_AR_WORK_RELEASE_RECORD_QUERY_TEMPLATE = f"""
    WITH base AS (
        {join_current_task_eligibility_spans_with_external_id(
            state_code= "'US_AR'", 
            tes_task_query_view = 'work_release_materialized',
            id_type = "'US_AR_OFFENDERID'",
            eligible_only=True,
        )}
    )
    SELECT
        base.external_id,
        base.state_code,
        base.reasons,
        base.is_eligible,
        base.is_almost_eligible,
        resident_metadata.* EXCEPT (person_id)
    FROM base
    LEFT JOIN `{{project_id}}.workflows_views.us_ar_resident_metadata_materialized` resident_metadata
    USING(person_id)
"""

US_AR_WORK_RELEASE_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_AR_WORK_RELEASE_RECORD_VIEW_NAME,
    view_query_template=US_AR_WORK_RELEASE_RECORD_QUERY_TEMPLATE,
    description=__doc__,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_AR
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_AR_WORK_RELEASE_RECORD_VIEW_BUILDER.build_and_print()
