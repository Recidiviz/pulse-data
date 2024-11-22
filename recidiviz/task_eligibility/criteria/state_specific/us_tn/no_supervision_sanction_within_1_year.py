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
# ============================================================================
"""Spans of time during which a client in TN has not had any supervision sanctions in
the past year.
"""

from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TN_NO_SUPERVISION_SANCTION_WITHIN_1_YEAR"

# TODO(#35405): Move this criterion logic into a general criterion builder or shared
# query fragment, where it can be generalized/parameterized and made state-agnostic.
_QUERY_TEMPLATE = f"""
    WITH supervision_sanctions AS (
        SELECT
            state_code,
            person_id,
            /* For our selected violation responses, we create spans of *ineligibility*
            by setting span start and end dates here, covering the periods of time
            during which someone will not meet this criterion because the response in
            question remains relevant/valid. */
            vr.response_date AS sanction_date,
            DATE_ADD(vr.response_date, INTERVAL 1 YEAR) AS sanction_expiration_date,
            vr.response_date AS start_date,
            DATE_ADD(vr.response_date, INTERVAL 1 YEAR) AS end_date,
            FALSE AS meets_criteria,
        FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation_response` vr
        /* NB: while (as of the time of writing this) in some states there are violation
        responses with multiple decision entries, in TN there are not instances where a
        single violation response is associated with multiple decision entries. As a
        result, even though we are joining in the response-decision data here, because
        we are only considering TN data in this query, we won't end up introducing any
        excess rows (where the same response is joined to multiple decisions) via this
        LEFT JOIN. */
        LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation_response_decision_entry` vrde
            USING (state_code, person_id, supervision_violation_response_id)
        WHERE state_code='US_TN'
            /* Here, we want to exclude violation responses that did not result in
            changes to a client's supervision. Again, because there is currently a 1:1
            relationship between responses and decision entries in TN, we can simply
            filter by decision here without needing to worry about aggregating across
            decisions. */
            AND COALESCE(vrde.decision, 'NO_DECISION') NOT IN ('CONTINUANCE', 'DELAYED_ACTION', 'VIOLATION_UNFOUNDED')
    ),
    /* Sub-sessionize in case there are overlapping spans (i.e., if someone has multiple
    still-relevant sanctions at once). */
    {create_sub_sessions_with_attributes('supervision_sanctions')}
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        LOGICAL_AND(meets_criteria) AS meets_criteria,
        TO_JSON(STRUCT(
            ARRAY_AGG(sanction_date IGNORE NULLS ORDER BY sanction_date DESC) AS sanction_dates,
            MAX(sanction_expiration_date) AS latest_sanction_expiration_date
        )) AS reason,
        ARRAY_AGG(sanction_date IGNORE NULLS ORDER BY sanction_date DESC) AS sanction_dates,
        MAX(sanction_expiration_date) AS latest_sanction_expiration_date,
    FROM sub_sessions_with_attributes
    GROUP BY 1, 2, 3, 4
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=__doc__,
    state_code=StateCode.US_TN,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    # Set default to True because we only created spans of *ineligibility* in the query
    # above, and we want to assume that folks are eligible by default otherwise.
    meets_criteria_default=True,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    reasons_fields=[
        ReasonsField(
            name="sanction_dates",
            type=bigquery.enums.StandardSqlTypeNames.ARRAY,
            description="Date(s) of any sanction(s) within the past year",
        ),
        ReasonsField(
            name="latest_sanction_expiration_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date on which latest sanction(s) will age out of lookback period",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
