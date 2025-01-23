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
# ============================================================================
"""Describes the spans of time during which someone in TN is not on Community
Supervision for Life, a specific type of lifetime supervision (for sex offenses).
"""

from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TN_NOT_ON_COMMUNITY_SUPERVISION_FOR_LIFE"

# TODO(#35808): Account for scenarios in which a CSL sentence might actually end (i.e.,
# commutation, if the sentence is vacated, or if someone under Community Supervision for
# Life successfully petitions for discharge).
_QUERY_TEMPLATE = f"""
    SELECT
        state_code,
        person_id,
        -- person becomes ineligible once any CSL sentence is imposed for them
        MIN(date_imposed) AS start_date,
        -- person will remain ineligible forever after a CSL sentence is imposed
        CAST(NULL AS DATE) AS end_date,
        FALSE AS meets_criteria,
        CAST(NULL AS JSON) AS reason,
    FROM `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized`
    WHERE state_code='US_TN'
        /* Only consider CSL sentences, which are indicated by `CSL_FLAG` in the
        sentence metadata. */
        AND COALESCE(JSON_VALUE(sentence_metadata, '$.CSL_FLAG'), 'UNKNOWN')='Y'
        /* Drop zero-day sentences and sentences where the `date_imposed` is after the
        `projected_completion_date_max`. */
        AND {nonnull_end_date_exclusive_clause('projected_completion_date_max')}>date_imposed
    GROUP BY 1, 2
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    state_code=StateCode.US_TN,
    criteria_name=_CRITERIA_NAME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    description=__doc__,
    sessions_dataset=SESSIONS_DATASET,
    # Set default to True because we only created spans of *ineligibility* in the query
    # above, and we want to assume that folks are eligible by default otherwise.
    meets_criteria_default=True,
    reasons_fields=[],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
