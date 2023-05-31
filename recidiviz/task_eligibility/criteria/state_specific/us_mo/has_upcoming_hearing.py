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
"""Describes the spans of time during which someone in MO
has an upcoming restrictive housing hearing.
"""
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.task_eligibility.utils.us_mo_query_fragments import hearings_dedup_cte
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_MO_HAS_UPCOMING_HEARING"

_DESCRIPTION = """Describes the spans of time during which someone in MO
has an upcoming disciplinary hearing.
"""

US_MO_HAS_UPCOMING_HEARING_NUM_DAYS = 7

_QUERY_TEMPLATE = f"""
    WITH {hearings_dedup_cte()}
    ,
    critical_date_spans AS (
        SELECT
            state_code,
            person_id,
            hearing_date AS start_datetime,
            CASE WHEN next_review_date IS NOT NULL 
                 THEN LEAST(next_review_date, COALESCE(LEAD(hearing_date) OVER hearing_window, '9999-12-31'))
                 ELSE LEAD(hearing_date) OVER hearing_window
                 END AS end_datetime,
            next_review_date AS critical_date,
        FROM hearings
        WINDOW hearing_window AS (
            PARTITION BY state_code, person_id
            ORDER BY hearing_date ASC
        )
    )
    ,
    {critical_date_has_passed_spans_cte(US_MO_HAS_UPCOMING_HEARING_NUM_DAYS)}
    SELECT 
        state_code,
        person_id,
        start_date,
        -- Set current span's end date to null
        IF(end_date > CURRENT_DATE('US/Pacific'), NULL, end_date) AS end_date,
        critical_date_has_passed AS meets_criteria,
        TO_JSON(STRUCT(
            critical_date as next_review_date
        )) AS reason
    FROM critical_date_has_passed_spans
    -- Exclude spans that start in the future
    WHERE start_date <= CURRENT_DATE('US/Pacific')
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_MO,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        meets_criteria_default=False,
        us_mo_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_MO, instance=DirectIngestInstance.PRIMARY
        ),
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
