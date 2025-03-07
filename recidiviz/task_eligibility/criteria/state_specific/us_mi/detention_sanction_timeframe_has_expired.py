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
"""This criteria view builder defines spans of time where the detention sanction timeframe
for a resident has expired. Policy dictates that detention should not exceed 10 days for
each violation or 20 days for all violations arising from a specific incident.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    revert_nonnull_start_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_MI_DETENTION_SANCTION_TIMEFRAME_HAS_EXPIRED"

_DESCRIPTION = """This criteria view builder defines spans of time where the detention sanction timeframe
for a resident has expired. Policy dictates that detention should not exceed 10 days for 
each violation or 20 days for all violations arising from a specific incident. 
"""

_QUERY_TEMPLATE = f"""
WITH sanctions_and_locations AS (
/* This cte queries incarceration incidents to identify the detention sanctions and the spans of time they are effective.
 It then unions all disciplinary solitary confinement sessions and sets a default sanction expiration date of the 
 start date of that session. (This ensures that a resident is surfaced as eligible if they are in disciplinary 
 solitary confinement with no sanction). */
 #TODO(#39224) revert once punishment length days is hydrated for restrictions
    SELECT DISTINCT
        state_code,
        person_id,
        date_effective AS start_date, 
        LEAST(projected_end_date, DATE_ADD(date_effective, INTERVAL 20 DAY)) AS end_date,
        --set the critical date to the projected end date or 20 days after the start, whichever comes earlier
        LEAST(projected_end_date, DATE_ADD(date_effective, INTERVAL 20 DAY))  AS critical_date
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_incident_outcome`
    WHERE outcome_type = 'RESTRICTED_CONFINEMENT'
        AND external_id LIKE '%RESTRICTION%'
        AND date_effective IS NOT NULL
        AND state_code = 'US_MI'
        --do not include zero day sanctions
        AND date_effective != {nonnull_end_date_clause('projected_end_date')}

    UNION ALL

    SELECT DISTINCT
        state_code,
        person_id,
        date_effective AS start_date, 
        LEAST(projected_end_date, DATE_ADD(date_effective, INTERVAL LEAST(punishment_length_days, 20) DAY)) AS end_date,
        --if the sanction is greater than 20 days, we convert it to 20 as per policy
        DATE_ADD(date_effective, INTERVAL LEAST(punishment_length_days, 20) DAY) AS critical_date
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_incident_outcome`
    WHERE outcome_type = 'RESTRICTED_CONFINEMENT'
        AND punishment_length_days IS NOT NULL
        AND external_id NOT LIKE '%RESTRICTION%'
        AND date_effective IS NOT NULL
        AND state_code = 'US_MI'
        --do not include zero day sanctions
        AND date_effective != {nonnull_end_date_clause('projected_end_date')}
    
    UNION ALL 
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive AS end_date,
        start_date AS critical_date
    FROM `{{project_id}}.{{sessions_dataset}}.housing_unit_type_sessions_materialized` 
    WHERE state_code = 'US_MI'
        AND housing_unit_type = 'DISCIPLINARY_SOLITARY_CONFINEMENT' 
),
/* Sub sessions are created where all detention sanction spans are associated with their expiration dates and all 
disciplinary solitary confinement spans are associated with a default expiration date */
{create_sub_sessions_with_attributes('sanctions_and_locations')},
critical_date_spans AS (
/* The MAX of all expiration dates that are active for a given span is used as the critical date */
    SELECT 
        state_code,
        person_id,
        start_date AS start_datetime,
        end_date AS end_datetime,
        MAX({revert_nonnull_start_date_clause('critical_date')}) AS critical_date
    FROM sub_sessions_with_attributes
    GROUP BY 1,2,3,4
),
{critical_date_has_passed_spans_cte()}
SELECT
    cd.state_code,
    cd.person_id,
    cd.start_date,
    cd.end_date,
    cd.critical_date_has_passed AS meets_criteria,
    TO_JSON(STRUCT(
        cd.critical_date AS eligible_date
    )) AS reason,
    cd.critical_date AS expiration_date,
FROM critical_date_has_passed_spans cd
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        state_code=StateCode.US_MI,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        sessions_dataset=SESSIONS_DATASET,
        reasons_fields=[
            ReasonsField(
                name="expiration_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Sanction expiration date",
            )
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
