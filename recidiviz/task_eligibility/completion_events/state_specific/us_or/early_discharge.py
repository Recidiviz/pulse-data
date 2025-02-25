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
"""Identifies (at the person-sentence level) when clients in OR have been granted earned discharge"""

from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """Identifies (at the person-sentence level) when clients in OR have been granted earned discharge"""

_QUERY_TEMPLATE = """
    SELECT
    /* NOTE: Because earned discharge in OR is granted at the sentence level, here, we
    are selecting completion events at the person-sentence level (i.e., we're
    identifying when a particular person is granted earned discharge for a particular
    sentence). Consequently, we may see multiple events per person per supervision
    period (if someone has multiple sentences that were terminated via EDIS). This
    differs from other completion events, which are at the person level. */
    /* TODO(#28207): Adjust downstream state-agnostic infrastructure
    (all_completion_events, person_events, and any others, as needed) to account for the
    fact that this completion event is at the person-sentence level, unlike other events
    that are at the person level.*/
        state_code,
        person_id,
        completion_date AS completion_event_date,
        TO_JSON(STRUCT(sentence_id AS sentence_id)) AS completion_event_metadata,
    FROM `{project_id}.{sessions_dataset}.sentences_preprocessed_materialized`
    WHERE state_code='US_OR'
        AND sentence_type='SUPERVISION'
        -- select cases closed out via EDIS (by selecting raw-text statuses starting with 'EDIS')
        AND REGEXP_SUBSTR(status_raw_text, '^[^@@]+')='EDIS'   
"""

VIEW_BUILDER: StateSpecificTaskCompletionEventBigQueryViewBuilder = (
    StateSpecificTaskCompletionEventBigQueryViewBuilder(
        state_code=StateCode.US_OR,
        completion_event_type=TaskCompletionEventType.EARLY_DISCHARGE,
        description=_DESCRIPTION,
        completion_event_query_template=_QUERY_TEMPLATE,
        sessions_dataset=SESSIONS_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
