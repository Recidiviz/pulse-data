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
"""
Defines a criteria view that shows spans of time when clients have not incurred a medium sanction
for 12 months
"""
# note - ideally the reason below should be called latest medium sanction date instead of latest high
# however i would like to aggregate no_medium_sanctions & no_high_sanctions via the new
# AndCriteriaGroup, and display the latest medium OR high sanction. for that functionality
# i think they have to be named the same thing, but i'll see once that logic is implemented if there's
# a better way

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

_CRITERIA_NAME = "US_PA_NO_MEDIUM_SANCTIONS_IN_PAST_YEAR"

_DESCRIPTION = """Defines a criteria view that shows spans of time when clients have not incurred a medium sanction
for 12 months"""

_REASON_QUERY = f"""
WITH sanctions_sessions_cte AS (
        SELECT
          v.person_id,
          v.state_code,
          COALESCE(violation_date, response_date) AS start_date,
          DATE_ADD(COALESCE(violation_date, response_date), INTERVAL 12 MONTH) AS end_date,
          COALESCE(violation_date, response_date) AS latest_sanction_date,
          FALSE as meets_criteria,
        FROM
          `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation` v
        LEFT JOIN
          `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation_response` vr
        USING
          (person_id, state_code, supervision_violation_id)
        LEFT JOIN
          `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation_response_decision_entry` vrd
        USING
          (person_id, state_code, supervision_violation_response_id)
        WHERE
          vrd.decision_raw_text IN (
            'DFSE', -- Deadline for Securing Employment
            'URIN', -- Imposition of Increased Urinalysis Testing
            'ICRF', -- Imposition of Increased Curfew
            'GVPB', -- Refer to Violence Prevention Booster
            'COMS', -- Imposition of Community Service
            'OPAT', -- Placement in Out-Patient D&A Treatment
            'RECT', -- Refer to Re-Entry Program
            'EMOS', -- Imposition of Electronic Monitoring
            'MOTR', -- Other
            'DRPT' -- Day Reporting Center
            # WIP - there may be other medium sanction codes that will need to be added 
            )
          AND state_code = 'US_PA'
        ),
    /*
    A person can have multiple sanctions within a 12 month period, we create sub sessions to deal with this
    */
        {create_sub_sessions_with_attributes('sanctions_sessions_cte')},
    /*
    If a person has more than 1 sanction in a 12 month period, they will have duplicate sub-sessions for the period of
    time where there were more than 1 sanction. For example, if a person has a sanction on Jan 1 and March 1
    there would be duplicate sessions for the period March 1 - Dec 31 because both sanctions are relevant at that time.
    We deduplicate below so that we surface the most-recent sanction that is relevant at each time. 
    */
    dedup_cte AS (
        SELECT
            *,
        FROM sub_sessions_with_attributes
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, state_code, start_date, end_date 
            ORDER BY latest_sanction_date DESC) = 1
        )
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        meets_criteria,
        TO_JSON(STRUCT(latest_sanction_date)) AS reason,
        latest_sanction_date,
    FROM dedup_cte"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_PA,
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        criteria_spans_query_template=_REASON_QUERY,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="latest_sanction_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date of latest medium sanction",
            )
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
