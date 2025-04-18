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
"""Spans of time when a client in TX has not had a warrant that sustained a violation within the past 2 years."""

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    get_supervision_violations_sans_unfounded,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_QUERY_TEMPLATE = f"""
    WITH 
    supervision_violations_sans_unfounded AS (
        -- Returns a query fragment that only contains violations where the LATEST violation response DOES NOT contain a
        -- VIOLATION_UNFOUNDED decision, indicating that the violation is unfounded.
        {get_supervision_violations_sans_unfounded()}
        ),
    ineligibility_spans AS (
        SELECT 
            pei.state_code,
            pei.person_id,
            CAST(CAST(warrants.warrant_date AS DATETIME) AS DATE) AS warrant_date,
            CAST(CAST(warrants.warrant_date AS DATETIME) AS DATE) AS start_date,
            -- Criteria specifies "of the current parole supervision period" so ineligibility ends at the end of the
            -- relevant supervision period 
            LEAST(DATE_ADD(CAST(CAST(warrants.warrant_date AS DATETIME) AS DATE), INTERVAL 2 YEAR), {nonnull_end_date_clause('sessions.end_date')}) AS end_date,
            sessions.end_date as supervision_end_date,
            FALSE as meets_criteria
        FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.Warrants_latest` as warrants
        INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
            ON warrants.sid_number=pei.external_id
            AND pei.id_type='US_TX_SID'
        INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation` as norm_violations
            ON warrants.wnf_vltn_id = norm_violations.external_id
            AND warrants.sid_number = pei.external_id
        INNER JOIN supervision_violations_sans_unfounded as sustained_violations
            ON norm_violations.state_code = sustained_violations.state_code
            AND norm_violations.person_id = sustained_violations.person_id
            AND norm_violations.supervision_violation_id = sustained_violations.supervision_violation_id
        LEFT JOIN `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` as sessions
            ON sessions.state_code = 'US_TX' 
            AND compartment_level_2 = 'PAROLE'
            AND sessions.person_id = pei.person_id
            AND sessions.start_date <= CAST(CAST(warrants.warrant_date AS DATETIME) AS DATE)
            AND sessions.end_date > CAST(CAST(warrants.warrant_date AS DATETIME) AS DATE)
    ),
    /* Sub-sessionize in case there are overlapping spans (i.e., if someone has multiple
    still-relevant warrants at once). */
    {create_sub_sessions_with_attributes("ineligibility_spans")},
    ineligibility_spans_aggregated AS (
        /* Aggregate across sub-sessions to get attributes for each span of time for
        each person. */
        SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            LOGICAL_AND(meets_criteria) AS meets_criteria,
            MAX(warrant_date) AS latest_warrant_date,
            MAX(supervision_end_date) AS supervision_end_date
        FROM sub_sessions_with_attributes
        GROUP BY 1, 2, 3, 4
    )
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        meets_criteria,
        TO_JSON(STRUCT(
            latest_warrant_date AS latest_warrant_date,
            supervision_end_date AS supervision_end_date
        )) AS reason,
        latest_warrant_date,
        supervision_end_date
    FROM ineligibility_spans_aggregated
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name="US_TX_NO_WARRANT_WITH_SUSTAINED_VIOLATION_WITHIN_2_YEARS",
    description=__doc__,
    state_code=StateCode.US_TX,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    # Set default to True because we only created spans of *ineligibility* in the query
    # above, and we want to assume that folks are eligible by default otherwise.
    meets_criteria_default=True,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_TX,
        instance=DirectIngestInstance.PRIMARY,
    ),
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    reasons_fields=[
        ReasonsField(
            name="latest_warrant_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date of latest warrant",
        ),
        ReasonsField(
            name="supervision_end_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="End date of the supervision period in which the warrant occurred",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
