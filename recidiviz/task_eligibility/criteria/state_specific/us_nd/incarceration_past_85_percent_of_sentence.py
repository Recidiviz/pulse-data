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
# =============================================================================

"""Defines a criteria view that shows spans of time for which residents 
with a required 85% sentence have completed at least 85% of their sentence.
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
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ND_INCARCERATION_PAST_85_PERCENT_OF_SENTENCE"

_DESCRIPTION = """Defines a criteria view that shows spans of time for which residents
with a required 85% sentence have completed at least 85% of their sentence.
"""

_QUERY_TEMPLATE = f"""
WITH sentences_with_85_rule AS (
  SELECT *
  FROM (
    SELECT 
      pei.state_code,
      pei.person_id,
      SAFE_CAST(LEFT(eo.START_DATE, 10) AS DATE) AS start_date,
      SAFE_CAST(LEFT(eo.SENTENCE_EXPIRY_DATE, 10) AS DATE) AS end_date,
      SAFE_CAST(LEFT(eo.EIGHTYFIVE_PERCENT_DATE, 10) AS DATE) AS critical_date,
      pei.external_id,
    FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.elite_offendersentences_latest` eo
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
      ON state_code = 'US_ND'
        AND id_type = 'US_ND_ELITE_BOOKING'
        AND pei.external_id = REPLACE(REPLACE(eo.OFFENDER_BOOK_ID, '.00', ''), ',', '')
    -- We only keep folks with an actual 85% rule, the rest are deemed eligible by default.
    WHERE EIGHTYFIVE_PERCENT_DATE IS NOT NULL
  )
  -- Start and critical dates should come before end dates. If not, they are wrong.
  WHERE start_date < end_date 
    AND critical_date < end_date
), 

critical_date_spans AS (
  -- Dedupe consecutive sentences by grouping on external_id (OFFENDER_BOOK_ID)
  SELECT 
    state_code,
    person_id,
    external_id,
    MAX(start_date) AS start_datetime,
    MAX(end_date) AS end_datetime,
    MAX(critical_date) AS critical_date,
  FROM sentences_with_85_rule
  GROUP BY 1,2,3
),

{critical_date_has_passed_spans_cte()},
{create_sub_sessions_with_attributes('critical_date_has_passed_spans')}

SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    LOGICAL_AND(critical_date_has_passed) AS meets_criteria,
    TO_JSON(STRUCT(MAX(critical_date) AS eighty_five_percent_date)) AS reason,
    MAX(critical_date) AS eighty_five_percent_date
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4"""

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
            name="eighty_five_percent_date",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="The date at which the resident has completed at least 85% of their sentence.",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
