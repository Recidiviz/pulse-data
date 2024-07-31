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
Shows the spans of time during which someone in ND has no detainers or warrants.
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
from recidiviz.task_eligibility.utils.us_nd_query_fragments import reformat_ids
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ND_NO_DETAINERS_OR_WARRANTS"

_DESCRIPTION = """
Shows the spans of time during which someone in ND has no detainers or warrants.
"""
ORDER_TYPES = [
    "WARM",  # Warrant - Misdemeanor
    "WARF",  # Warrant - Felony
    "DET",  # Detainer
]
_QUERY_TEMPLATE = f"""
WITH warrants_and_detainers AS (
    SELECT 
        peid.state_code,
        peid.person_id,
        SAFE_CAST(LEFT(e.OFFENSE_DATE, 10) AS DATE) AS start_date,
        SAFE_CAST(LEFT(e.OFFENSE_DATE, 10) AS DATE) AS start_date_warrant_or_detainer,
        SAFE_CAST(NULL AS DATE) AS end_date,
        FALSE AS meets_criteria,
        e.ORDER_TYPE,
        e.OFFENSE_STATUS,
        e.OFFENSE_DESC,
    FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.elite_offender_offences_latest` e
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid
    ON peid.external_id = {reformat_ids('e.OFFENDER_BOOK_ID')}
        AND peid.state_code = 'US_ND'
        AND peid.id_type = 'US_ND_ELITE_BOOKING'
    WHERE ORDER_TYPE IN {tuple(ORDER_TYPES)}
),
{create_sub_sessions_with_attributes(table_name='warrants_and_detainers')}
SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    meets_criteria,
    TO_JSON(STRUCT(
        STRING_AGG(DISTINCT OFFENSE_DESC, ', ' ORDER BY OFFENSE_DESC) AS offenses_descriptions,
        STRING_AGG(DISTINCT ORDER_TYPE, ', ' ORDER BY ORDER_TYPE) AS order_types,
        MAX(start_date_warrant_or_detainer) AS most_recent_warrant_or_detainer_date
    )) AS reason,
    STRING_AGG(DISTINCT OFFENSE_DESC, ', ' ORDER BY OFFENSE_DESC) AS offenses_descriptions,
    STRING_AGG(DISTINCT ORDER_TYPE, ', ' ORDER BY ORDER_TYPE) AS order_types,
    MAX(start_date_warrant_or_detainer) AS most_recent_warrant_or_detainer_date,
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
            name="offenses_descriptions",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Descriptions of the offenses that led to the warrants or detainers.",
        ),
        ReasonsField(
            name="order_types",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Types of the warrants or detainers.",
        ),
        ReasonsField(
            name="most_recent_warrant_or_detainer_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date of the most recent warrant or detainer.",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
