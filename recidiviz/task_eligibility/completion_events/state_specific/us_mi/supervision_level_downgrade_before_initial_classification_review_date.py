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
"""Defines a view that shows supervision level downgrades before the initial
classification review date (ie. the first 4-12 months of supervision depending on the case).
"""
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_criteria_state_specific_dataset,
)
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_QUERY_TEMPLATE = f"""
SELECT
    sls.state_code,
    sls.person_id,
    sls.start_date AS completion_event_date,
FROM `{{project_id}}.{{sessions_dataset}}.supervision_level_sessions_materialized` sls
LEFT JOIN `{{project_id}}.{{criteria_dataset}}.not_past_initial_classification_review_date_materialized` tes
    ON sls.person_id = tes.person_id 
    AND sls.start_date BETWEEN tes.start_date AND {nonnull_end_date_exclusive_clause('tes.end_date')}
LEFT JOIN `{{project_id}}.{{criteria_dataset}}.supervision_or_supervision_out_of_state_level_is_sai_materialized` sai
    ON sls.person_id = sai.person_id 
    AND sls.start_date = sai.end_date
    AND sai.meets_criteria
WHERE sls.supervision_downgrade = 1
    AND sls.supervision_level != "LIMITED"
    AND sls.state_code = 'US_MI'
    --exclude transitions from SAI to a lower supervision level 
    AND NOT COALESCE(sai.meets_criteria, FALSE)
    --only take rows where a client is NOT past their initial review date 
    AND tes.meets_criteria
"""

VIEW_BUILDER: StateSpecificTaskCompletionEventBigQueryViewBuilder = StateSpecificTaskCompletionEventBigQueryViewBuilder(
    state_code=StateCode.US_MI,
    completion_event_type=TaskCompletionEventType.SUPERVISION_LEVEL_DOWNGRADE_BEFORE_INITIAL_CLASSIFICATION_REVIEW_DATE,
    description=__doc__,
    completion_event_query_template=_QUERY_TEMPLATE,
    sessions_dataset=SESSIONS_DATASET,
    criteria_dataset=task_eligibility_criteria_state_specific_dataset(StateCode.US_MI),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
