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
# ============================================================================
"""Describes the spans of time when a TN client has not had a sanction for 12 months."""
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TN_NO_HIGH_SANCTIONS_IN_PAST_YEAR"

_DESCRIPTION = """Describes the spans of time when a TN client has not had a sanction for 12 months.
"""

_REASON_QUERY = f"""
    WITH sanction_sessions_cte AS
    (
    SELECT DISTINCT
        person_id,
        "US_TN" AS state_code,
        CAST(CAST(ProposedDate AS DATETIME) AS DATE) AS start_date,
        DATE_ADD(CAST(CAST(ProposedDate AS DATETIME) AS DATE), INTERVAL 12 MONTH) AS end_date,
        --create this field to keep track of the actual sanction date even after we sub-sessionize to handle overlapping
        --periods (cases when a person has more than sanction in a 12 month period)
        CAST(CAST(ProposedDate AS DATETIME) AS DATE) AS latest_high_sanction_date,
        FALSE as meets_criteria,
    FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.Violations_latest` a
    INNER JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.Sanctions_latest` 
        USING (TriggerNumber)
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON pei.external_id = a.OffenderID
        AND id_type = "US_TN_DOC"
    WHERE CAST(SanctionLevel AS INT) > 1
    )
    ,
    /*
    If a person has more than 1 sanction in a 12 month period, they will have overlapping sessions created in the above
    CTE. Therefore we use `create_sub_sessions_with_attributes` to break these up
    */
    {create_sub_sessions_with_attributes('sanction_sessions_cte')}
    ,
    dedup_cte AS
    /*
    If a person has more than 1 sanction in a 12 month period, they will have duplicate sub-sessions for the period of
    time where there were more than 1 sanction. For example, if a person has a sanction on Jan 1 and March 1
    there would be duplicate sessions for the period March 1 - Dec 31 because both sanctions are relevant at that time.
    We deduplicate below so that we surface the most-recent sanction that as relevant at each time. 
    */
    (
    SELECT
        *,
    FROM sub_sessions_with_attributes
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, state_code, start_date, end_date 
        ORDER BY latest_high_sanction_date DESC) = 1
    )
    ,
    sessionized_cte AS 
    /*
    Sessionize so that we have continuous periods of time for which a person is not eligible due to a high sanction. A
    new session exists either when a person becomes eligible, or if a person has an additional sanction within a 12-month
    period which changes the "latest_high_sanction_date" value.
    */
    (
    {aggregate_adjacent_spans(table_name='dedup_cte',
                       attribute=['latest_high_sanction_date','meets_criteria'],
                       end_date_field_name='end_date')}
    )
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        meets_criteria,
        TO_JSON(STRUCT(latest_high_sanction_date AS latest_high_sanction_date)) AS reason
    FROM sessionized_cte
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_TN,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_REASON_QUERY,
        description=_DESCRIPTION,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_TN,
            instance=DirectIngestInstance.PRIMARY,
        ),
        meets_criteria_default=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
