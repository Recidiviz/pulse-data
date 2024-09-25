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
# ============================================================================
"""Describes spans of time during which a candidate meets functional literacy"""
from google.cloud import bigquery

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

_CRITERIA_NAME = "US_AZ_MEETS_FUNCTIONAL_LITERACY"

_DESCRIPTION = (
    """Describes spans of time during which a candidate meets functional literacy"""
)

_QUERY_TEMPLATE = """
    WITH literacy_status AS ( 
        SELECT
          pei.state_code,
          pei.person_id, 
          MIN(PARSE_DATE('%m/%d/%Y', SPLIT(eval.CREATE_DTM, ' ')[OFFSET(0)] )) AS start_date,
          CAST(NULL AS DATE) as end_date,
          TRUE AS meets_criteria,
        FROM
          `{project_id}.{raw_data_up_to_date_views_dataset}.AZ_DOC_TRANSITION_PRG_EVAL_latest` eval
        INNER JOIN 
        `{project_id}.{raw_data_up_to_date_views_dataset}.AZ_DOC_TRANSITION_PRG_ELIG_latest` map_to_docid
        USING (TRANSITION_PRG_ELIGIBILITY_ID)
        LEFT JOIN `{project_id}.{raw_data_up_to_date_views_dataset}.DOC_EPISODE_latest` doc_ep
        USING(DOC_ID)
        LEFT JOIN `{project_id}.{raw_data_up_to_date_views_dataset}.PERSON_latest` person
        USING(PERSON_ID)
        INNER JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
            ON ADC_NUMBER = external_id 
            AND pei.state_code = 'US_AZ'
            AND pei.id_type = 'US_AZ_ADC_NUMBER'
        WHERE MEETS_MANDITORY_LITERACY = 'Y'
        GROUP BY 1,2
    )
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        meets_criteria AS meets_criteria,
        TO_JSON(STRUCT(
            start_date AS meets_functional_literacy
        )) AS reason,
        start_date AS meets_functional_literacy,
    FROM literacy_status
"""

_REASONS_FIELDS = [
    ReasonsField(
        name="meets_functional_literacy",
        type=bigquery.enums.StandardSqlTypeNames.DATE,
        description="Date of meeting functional literacy.",
    ),
]

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        reasons_fields=_REASONS_FIELDS,
        state_code=StateCode.US_AZ,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_AZ,
            instance=DirectIngestInstance.PRIMARY,
        ),
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        meets_criteria_default=False,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
