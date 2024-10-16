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

"""Defines a criteria span view that shows spans of time during which a client EITHER
1. Does not have an assigned stat code
2. Has an assigned stat code of 23 (active supervision), 26 (in-patient program), or 31 (technical violation with no detention)
3. Has any other assigned stat code, but has been incarcerated during this stat code session. This is an attempt to capture
instances where stat codes are not updated after someone is re-incarcerated for additional charges/violations and then
released back to supervision. Note that if they are still incarcerated they will be excluded from the candidate population

If true, the client has a status that is eligible for transfer to admin or special circumstances supervision
"""

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_PA_NOT_ASSIGNED_INELIGIBLE_STAT_CODE"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which a client EITHER
1. Does not have an assigned stat code
2. Has an assigned stat code of 23 (active supervision), 26 (in-patient program), or 31 (technical violation with no detention)
3. Has any other assigned stat code, but has been incarcerated during this stat code session. This is an attempt to capture
instances where stat codes are not updated after someone is re-incarcerated for additional charges/violations and then
released back to supervision. Note that if they are still incarcerated they will be excluded from the candidate population

If true, the client has a status that is eligible for transfer to admin or special circumstances supervision
"""

_QUERY_TEMPLATE = f"""
WITH status_deduped AS (
    -- this CTE pulls all status codes and their start dates from raw data 
    -- if there are duplicate start dates, it prioritizes the eligible stat codes to air on the side of including people 
    SELECT 
        pei.state_code,
        pei.person_id, 
        RelStatusCode, 
        DATE(CAST(RelStatusDateYear AS INT64), CAST(RelStatusDateMonth AS INT64), CAST(RelStatusDateDay AS INT64)) AS status_date,
    FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.dbo_RelStatus_latest` stat
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON stat.parolenumber = pei.external_id
        AND id_type = 'US_PA_PBPP'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY ParoleNumber, RelStatusDateYear, RelStatusDateMonth, RelStatusDateDay ORDER BY CASE WHEN RelStatusCode IN ('23', '26', '31') THEN 0 ELSE 1 END) = 1 
),
status_ordered AS (
    -- this CTE assigns row numbers based on the order that stat codes started  
    -- this allows us to create stat code spans in the next CTE 
    SELECT *,
        ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY status_date) AS rn
    FROM status_deduped
),
status_spans AS (
    -- this CTE sessionizes the status table into spans where a stat code is held 
    SELECT 
        s1.state_code,
        s1.person_id,
        s1.RelStatusCode,
        s1.status_date AS start_date, 
        s2.status_date AS end_date, 
    FROM status_ordered s1
    LEFT JOIN status_ordered s2
        ON s1.person_id = s2.person_id
        AND s1.rn + 1 = s2.rn
)
-- this query marks someone as ineligible if they have an ineligible stat code and no overlapping incarceration period
SELECT
    status_spans.state_code,
    status_spans.person_id, 
    status_spans.start_date,
    status_spans.end_date,
    FALSE AS meets_criteria,
    TO_JSON(STRUCT(RelStatusCode AS status_code)) AS reason,
    RelStatusCode AS status_code,
FROM status_spans 
LEFT JOIN `{{project_id}}.{{sessions_dataset}}.compartment_level_1_super_sessions_materialized` incarceration_sessions
    ON incarceration_sessions.person_id = status_spans.person_id
    AND incarceration_sessions.start_date <= {nonnull_end_date_clause('status_spans.end_date')}
    AND status_spans.start_date <= {nonnull_end_date_clause('incarceration_sessions.end_date')}
    AND incarceration_sessions.compartment_level_1 IN ('INCARCERATION', 'PENDING_CUSTODY')
WHERE RelStatusCode NOT IN ('23', '26', '31')
    AND incarceration_sessions.person_id IS NULL 
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    state_code=StateCode.US_PA,
    sessions_dataset=SESSIONS_DATASET,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_PA,
        instance=DirectIngestInstance.PRIMARY,
    ),
    meets_criteria_default=True,
    reasons_fields=[
        ReasonsField(
            name="status_code",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Status code that a client holds during this span",
            # see https://docs.google.com/spreadsheets/d/16sNAeCTqkcneGUk3SP_V0bp69Rmrw4QMAwv0nHLxSWA/edit?gid=0#gid=0
            # for stat code mapping
        )
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
