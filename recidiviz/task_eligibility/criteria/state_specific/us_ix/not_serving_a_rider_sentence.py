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
This criteria shows the spans of time during which someone in ID is not serving a rider
sentence.

Retained jurisdiction, often called a rider, is a sentencing option available to judges 
in Idaho. Clients sentenced to a rider are incarcerated in an IDOC facility but are under 
the judge's jurisdiction as they receive treatment and programming.

Sentencing judges can place the resident on probation upon successful completion of 
the rider, or they can relinquish jurisdiction and sentence them to prison based on 
their behavior and progress during the retained jurisdiction period.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
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

_CRITERIA_NAME = "US_IX_NOT_SERVING_A_RIDER_SENTENCE"

_DESCRIPTION = """
This criteria shows the spans of time during which someone in ID is not serving a rider
sentence.

Retained jurisdiction, often called a rider, is a sentencing option available to judges 
in Idaho. Clients sentenced to a rider are incarcerated in an IDOC facility but are under 
the judge's jurisdiction as they receive treatment and programming.

Sentencing judges can place the resident on probation upon successful completion of 
the rider, or they can relinquish jurisdiction and sentence them to prison based on 
their behavior and progress during the retained jurisdiction period.
"""

_QUERY_TEMPLATE = f"""
WITH retained_jurisdiction_sentences AS (
  SELECT 
    SentenceOrderId,
    SAFE_CAST(LEFT(RetentionStartDate, 10) AS DATE) AS start_date,
    SAFE_CAST(LEFT(RetentionEndDate, 10) AS DATE) AS end_date
  FROM `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.scl_RetainedJurisdiction_latest`
  LEFT JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.scl_RetainedJurisdictionType_latest`
    USING(RetainedJurisdictionTypeId)
  WHERE RetainedJurisdictionTypeName = 'Sentence'
),

add_sentence_order AS (
  SELECT 
    peid.state_code,
    peid.person_id,
    so.SentenceOrderId,
    rjs.start_date,
    rjs.end_date,
  FROM `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.scl_SentenceOrder_latest` so
  INNER JOIN retained_jurisdiction_sentences rjs
    USING(SentenceOrderId)
  INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid
    ON so.OffenderId = peid.external_id
      AND peid.state_code = 'US_IX'
      AND peid.id_type = 'US_IX_DOC'
  WHERE SentenceOrderTypeId = "2"
),
{create_sub_sessions_with_attributes(
    table_name="add_sentence_order",
)}

SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    False AS meets_criteria,
    TO_JSON(STRUCT(MAX(end_date) AS latest_rider_sentence_end_date)) AS reason,
    MAX(end_date) AS latest_rider_sentence_end_date,
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        us_ix_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_IX, instance=DirectIngestInstance.PRIMARY
        ),
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        meets_criteria_default=True,
        state_code=StateCode.US_IX,
        reasons_fields=[
            ReasonsField(
                name="latest_rider_sentence_end_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="The end date of the most recent rider sentence",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
