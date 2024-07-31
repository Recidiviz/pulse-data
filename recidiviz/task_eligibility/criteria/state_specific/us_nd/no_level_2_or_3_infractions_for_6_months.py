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

"""Defines a criteria view that shows spans of time for
which residents are within 6 months or more of having received a level 2 or 3 
infraction.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ND_NO_LEVEL_2_OR_3_INFRACTIONS_FOR_6_MONTHS"

_DESCRIPTION = """Defines a criteria view that shows spans of time for
which residents are within 6 months or more of having received a level 2 or 3 
infraction.
"""

_QUERY_TEMPLATE = f"""
WITH infractions AS (
  SELECT 
    pei.state_code,
    pei.person_id,
    SAFE_CAST(LEFT(e.incident_date, 10) AS DATE) AS start_date,
    DATE_ADD(SAFE_CAST(LEFT(e.incident_date, 10) AS DATE), INTERVAL 6 MONTH) AS end_date,
    FALSE AS meets_criteria,
    RESULT_OIC_OFFENCE_CATEGORY AS infraction_category,
    SAFE_CAST(LEFT(e.incident_date, 10) AS DATE) AS start_date_infraction
  FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.elite_offense_in_custody_and_pos_report_data_latest` e
  INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
    ON pei.external_id = e.ROOT_OFFENDER_ID 
    AND pei.id_type = 'US_ND_ELITE'
  WHERE FINDING_DESCRIPTION = 'GUILTY'
    AND RESULT_OIC_OFFENCE_CATEGORY IN ('LVL2', 'LVL3', 'LVL2E', 'LVL3R')
),
{create_sub_sessions_with_attributes(table_name='infractions')}
SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    meets_criteria,
    TO_JSON(STRUCT(
        STRING_AGG(DISTINCT infraction_category, ', ' ORDER BY infraction_category) AS infraction_categories,
        MAX(start_date_infraction) AS most_recent_infraction_date
    )) AS reason,
    STRING_AGG(DISTINCT infraction_category, ', ' ORDER BY infraction_category) AS infraction_categories,
    MAX(start_date_infraction) AS most_recent_infraction_date,
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4,5
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    state_code=StateCode.US_ND,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ND, instance=DirectIngestInstance.PRIMARY
    ),
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    meets_criteria_default=True,
    reasons_fields=[
        ReasonsField(
            name="infraction_categories",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Categories of the infractions that led to the level 2 or 3 infraction.",
        ),
        ReasonsField(
            name="most_recent_infraction_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date of the most recent level 2 or 3 infraction.",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
