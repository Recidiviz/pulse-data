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
"""Defines a criteria span view that shows spans of time during which someone is not
eligible for classification review because they are already on the lowest eligible level for their case
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_MI_NOT_ALREADY_ON_LOWEST_ELIGIBLE_SUPERVISION_LEVEL"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which someone is not
eligible for classification review because they are already on the lowest eligible level for their case
"""
_QUERY_TEMPLATE = f"""
 WITH so_spans AS (
    /* This CTE checks for offenses that require SO registration and sets an open span from the first date_imposed date */
    SELECT
        state_code,
        person_id,
    --find the earliest sex offense sentence date for each person 
        MIN(date_imposed) AS start_date,
        CAST("9999-12-31" AS DATE) AS end_date,
        TRUE AS requires_so_registration,
        NULL AS on_lowest_level,
        NULL AS supervision_level,
    FROM `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sent
    WHERE state_code = "US_MI" 
    AND sent.statute IN (SELECT statute_code FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.RECIDIVIZ_REFERENCE_offense_exclusion_list_latest`
            WHERE CAST(requires_so_registration AS BOOL))   
    GROUP BY 1,2,4
    UNION ALL 
    SELECT 
    /* This CTE creates spans where the supervision level is MEDIUM or lower */
        state_code,
        person_id,
        start_date,
        end_date_exclusive AS end_date,
        NULL AS requires_so_registration,
        TRUE AS on_lowest_level,
        correctional_level AS supervision_level,
    FROM `{{project_id}}.{{sessions_dataset}}.compartment_sub_sessions_materialized` 
    WHERE state_code = "US_MI"
    AND compartment_level_1 = 'SUPERVISION' 
    AND correctional_level IN ('MEDIUM', 'LOW')
    ),
    {create_sub_sessions_with_attributes('so_spans')}
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        --if requires_so_registration AND on_lowest_level, then meets_criteria is FALSE
        NOT (LOGICAL_AND(on_lowest_level) AND LOGICAL_AND(requires_so_registration)) AS meets_criteria,
        TO_JSON(STRUCT(MAX(supervision_level) AS supervision_level, 
                        LOGICAL_AND(requires_so_registration) AS requires_so_registration)) AS reason,
        MAX(supervision_level) AS supervision_level,
        LOGICAL_AND(requires_so_registration) AS requires_so_registration,
    FROM sub_sessions_with_attributes
    GROUP BY 1,2,3,4
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    state_code=StateCode.US_MI,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_MI,
        instance=DirectIngestInstance.PRIMARY,
    ),
    meets_criteria_default=True,
    sessions_dataset=SESSIONS_DATASET,
    reasons_fields=[
        ReasonsField(
            name="supervision_level",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Current supervision level",
        ),
        ReasonsField(
            name="requires_so_registration",
            type=bigquery.enums.StandardSqlTypeNames.BOOL,
            description="Whether a client is required to register as a sex offender based on past charges",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
