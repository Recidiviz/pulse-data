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
"""Identify when clients in TN have been transferred to Compliant Reporting.

In TN, changes to supervision levels are generally backdated or postdated to the 1st of
the month. To work around the backdating (which can make it appear in the data as if a
client's level was changed before they actually experienced that change in real life),
we use an analyst view here which incorporates data from `SupervisionPlan` to infer the
date on which an officer actually initiated the transfer in cases of backdating. We
don't try to work around postdating because we are interested in tracking when the
client actually experienced the transition, and in cases of postdating, the client still
wouldn't experience the transition until the later date.
"""

from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(#40144): If we decide to undo the backdating workarounds for TN, we'll need to
# revise this completion event.
_QUERY_TEMPLATE = """
    SELECT
        state_code,
        person_id,
        start_date AS completion_event_date,
    FROM `{project_id}.{analyst_dataset}.us_tn_supervision_level_raw_text_sessions_inferred_materialized`
    WHERE supervision_level='LIMITED'
        /* Filter out CR supervision-level session starts that are coming immediately
        after an existing CR session. This is done to handle cases in which a person has
        back-to-back sessions at 'LIMITED' supervision, which can happen when the raw-
        text level changes although the ingested level does not (e.g., if the raw-text
        level goes from the usual '4TR' to 'EXTERNAL_UNKNOWN', which could happen due to
        upstream normalization/inference that determines someone's still on 'LIMITED'
        even if we don't have a raw-text level for them at that time. */
        AND COALESCE(previous_supervision_level, 'UNKNOWN')!='LIMITED'
"""

VIEW_BUILDER: StateSpecificTaskCompletionEventBigQueryViewBuilder = (
    StateSpecificTaskCompletionEventBigQueryViewBuilder(
        state_code=StateCode.US_TN,
        completion_event_type=TaskCompletionEventType.TRANSFER_TO_LIMITED_SUPERVISION,
        description=__doc__,
        completion_event_query_template=_QUERY_TEMPLATE,
        analyst_dataset=ANALYST_VIEWS_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
