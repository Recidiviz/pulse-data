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

"""Defines a criteria view that shows if residents have to serve 85% of their sentence.
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

_CRITERIA_NAME = "US_ND_HAS_TO_SERVE_85_PERCENT_OF_SENTENCE"

_DESCRIPTION = """Defines a criteria view that shows if residents have to
serve 85% of their sentence.
"""

_QUERY_TEMPLATE = f"""
WITH sentences_with_85_rule AS (
  SELECT 
    pei.state_code,
    pei.person_id,
    SAFE_CAST(LEFT(eo.START_DATE, 10) AS DATE) AS start_date,
    SAFE_CAST(LEFT(eo.SENTENCE_EXPIRY_DATE, 10) AS DATE) AS end_date,
    SAFE_CAST(LEFT(eo.START_DATE, 10) AS DATE) AS sentence_start_date,
  FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.elite_offendersentences_latest` eo
  INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
    ON state_code = 'US_ND'
      AND id_type = 'US_ND_ELITE_BOOKING'
      AND pei.external_id = {reformat_ids('eo.OFFENDER_BOOK_ID')}
  -- We only keep folks with an actual 85% rule
  WHERE EIGHTYFIVE_PERCENT_DATE IS NOT NULL
    AND eo.START_DATE < eo.SENTENCE_EXPIRY_DATE
), 
{create_sub_sessions_with_attributes('sentences_with_85_rule')}
SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    TRUE AS meets_criteria,
    TO_JSON(STRUCT(MAX(sentence_start_date) AS last_sentence_start_date)) AS reason,
    MAX(sentence_start_date) AS last_sentence_start_date,
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_ND,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_ND, instance=DirectIngestInstance.PRIMARY
        ),
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        meets_criteria_default=False,
        reasons_fields=[
            ReasonsField(
                name="last_sentence_start_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date of the last sentence start date.",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
