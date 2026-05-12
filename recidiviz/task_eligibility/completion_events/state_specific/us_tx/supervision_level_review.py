# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Defines a view that shows completed SISP supervision-level reviews for
clients in US_TX (initial or recurring)."""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(#78423): Replace this zero-row placeholder with the real SISP-review
# completion source once known. The matching contact-event placeholder lives
# in `sisp_review_contact_event_query` in us_tx_query_fragments.py.
_QUERY_TEMPLATE = """
SELECT *
FROM (
    SELECT
        CAST(NULL AS STRING) AS state_code,
        CAST(NULL AS INT64) AS person_id,
        CAST(NULL AS DATE) AS completion_event_date,
)
WHERE FALSE
"""

VIEW_BUILDER: StateSpecificTaskCompletionEventBigQueryViewBuilder = (
    StateSpecificTaskCompletionEventBigQueryViewBuilder(
        state_code=StateCode.US_TX,
        completion_event_type=TaskCompletionEventType.SUPERVISION_LEVEL_REVIEW,
        description=__doc__,
        completion_event_query_template=_QUERY_TEMPLATE,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
