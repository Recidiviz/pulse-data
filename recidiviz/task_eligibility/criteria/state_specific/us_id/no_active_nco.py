# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Defines a criteria span view that shows spans of time for
which there are no active no contact orders
"""

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ID_NO_ACTIVE_NCO"

_DESCRIPTION = """Defines a criteria span view that shows spans of time
where there are no active no contact orders"""

_QUERY_TEMPLATE = f"""
WITH active_ncos AS (
/*This CTE identifies active ncos and sets active_nco as TRUE*/
    SELECT
        pei.state_code,
        pei.person_id,
        CAST(CAST(c.strt_dt AS DATETIME) AS DATE) AS start_date,
        {nonnull_end_date_clause('''
        CAST(CAST(c.end_dt AS DATETIME) AS DATE)''')} AS end_date,
        TRUE as active_nco,
    FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.ofndr_caution_latest` c
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
      ON c.ofndr_num = pei.external_id
      AND pei.state_code = 'US_ID'
    --caution codes that indicate a no contact order
    WHERE bci_caution_cd IN ('120','119','118','117','111','106')
    AND CAST(CAST(c.strt_dt AS DATETIME) AS DATE) != {nonnull_end_date_clause('''
        CAST(CAST(c.end_dt AS DATETIME) AS DATE)''')}
#TODO(#15105) remove this section of the query once criteria can default to true
    UNION ALL
/*This CTE identifies supervision sessions for individuals and sets active_nco as FALSE*/
    SELECT
        state_code,
        person_id,
        start_date,
        DATE_ADD(end_date, INTERVAL 1 DAY) AS end_date,
        FALSE as active_nco,
    FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` ses
    WHERE compartment_level_1 = 'SUPERVISION'
),
{create_sub_sessions_with_attributes('active_ncos')}
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    --set any span that is TRUE For active_nco to FALSE for meets_criteria
    NOT LOGICAL_OR(active_nco) AS meets_criteria,
    TO_JSON(STRUCT(LOGICAL_OR(active_nco) AS active_nco)) AS reason,
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4
"""
VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_ID,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        sessions_dataset=SESSIONS_DATASET,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region("us_id"),
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
