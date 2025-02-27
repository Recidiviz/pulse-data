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
"""This criteria view builder defines spans of time that clients do not have a disqualifying supervision
level modifier.
"""
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_MI_SUPERVISION_LEVEL_IS_NOT_MODIFIED"

_DESCRIPTION = """This criteria view builder defines spans of time that clients do not have a disqualifying supervision
level modifier.
"""

_QUERY_TEMPLATE = f"""
#TODO(#25323) Simplify once we receive end dates
WITH modifiers_preprocessed AS (
--This cte groups by person_id, start_date and Modifier to take the MAX update_datetime at which this 
--person_id, start_date, modifier entry was seen 
    SELECT
        pei.state_code,
        pei.person_id,
        DATE(m.start_date) AS start_date,
        m.Modifier,
        MAX(m.update_datetime) AS update_datetime,
    FROM `{{project_id}}.{{raw_tables_dataset_for_region_dataset}}.COMS_Modifiers` m
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON LTRIM(m.Offender_Number, '0')= pei.external_id
        AND pei.state_code = 'US_MI'
        AND pei.id_type = "US_MI_DOC"
    WHERE UPPER(modifier) LIKE '%MPVU%'
       OR UPPER(modifier) LIKE '%PA223%'
       OR UPPER(modifier) LIKE '%ESCAPED%'
       OR UPPER(modifier) LIKE '%IN JAIL%'
       OR UPPER(modifier) LIKE '%ABSCONDED%'
       OR UPPER(modifier) LIKE '%IN PRISON%'
       OR UPPER(modifier) LIKE '%WARRANT STATUS%'
       OR UPPER(modifier) LIKE '%#2 WARRANT ISSUED%'
       OR UPPER(modifier) LIKE '%PAROLED TO CUSTODY%'
       OR UPPER(modifier) LIKE '%ARRESTED OUT OF STATE%'
       OR UPPER(modifier) LIKE '%PENDING REVOCATION HEARING%'
       OR UPPER(modifier) LIKE '%TEMPORARY RELEASE TO COURT%'
       OR UPPER(modifier) LIKE '%IN JAIL - #2 WARRANT ISSUED%'
       OR UPPER(modifier) LIKE '%PAROLED TO CUSTODY (FED/OUTSTATE)%'
       OR UPPER(modifier) LIKE '%#2 WARRANT ISSUED SENTENCED OVER 90 DAYS%'
       OR UPPER(modifier) LIKE '%ARRESTED OUT OF STATE SENTENCED OVER 90 DAYS%'
    GROUP BY 1,2,3,4
),
modifier_spans AS (
--this cte than compares that max update_datetime for that person_id, start_date, modifier entry to the
-- MAX update_datetime for the entire file, to determine whether that modifier is no longer active. 
--if the MAX update_datetime for the file is greater than the max update_datetime for the entry, then 
-- we set the end_date as a day after that update_datetime. Otherwise, the end_date is NULL
    SELECT
       state_code,
       person_id, 
       start_date,
       CASE
         WHEN update_datetime < max_update_datetime THEN DATE_ADD(update_datetime, INTERVAL 1 DAY)
         ELSE NULL
       END AS end_date,
       Modifier,
    FROM (
      SELECT
        *,
        MAX(DATE(update_datetime)) OVER () AS max_update_datetime
      FROM modifiers_preprocessed
    )
),
{create_sub_sessions_with_attributes('modifier_spans')},
aggregated_modifier_spans AS (
    SELECT 
        state_code,
        person_id, 
        start_date,
        end_date, 
        False AS meets_criteria,
        STRING_AGG(Modifier, ', ') AS active_modifiers
    FROM sub_sessions_with_attributes
    GROUP BY 1,2,3,4,5
)
SELECT 
    state_code,
    person_id, 
    start_date,
    end_date, 
    False AS meets_criteria,
    TO_JSON(STRUCT(active_modifiers AS active_modifiers)) AS reason
FROM aggregated_modifier_spans
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_MI,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        raw_tables_dataset_for_region_dataset=raw_tables_dataset_for_region(
            state_code=StateCode.US_MI,
            instance=DirectIngestInstance.PRIMARY,
        ),
        meets_criteria_default=True,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
