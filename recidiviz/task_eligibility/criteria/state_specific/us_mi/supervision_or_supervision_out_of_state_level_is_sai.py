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
"""This criteria view builder defines spans of time that clients are on supervision after participating in
 SAI (Special Alternative Incarceration) based on supervision level raw text values that contain SAI.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
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

MAGIC_END_DATE = "9999-12-31"

_CRITERIA_NAME = "US_MI_SUPERVISION_OR_SUPERVISION_OUT_OF_STATE_LEVEL_IS_SAI"

_DESCRIPTION = """This criteria view builder defines spans of time that clients are on supervision after participating
in SAI (Special Alternative Incarceration).
"""

_QUERY_TEMPLATE = f"""
#TODO(#22511) refactor to build off of a general criteria view builder
WITH sai_spans AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive AS end_date,
        TRUE as is_sai,
    #TODO(#20035) replace with supervision level raw text sessions once views agree
    FROM `{{project_id}}.{{sessions_dataset}}.compartment_sub_sessions_materialized` sls
    /* Using regex to match digits in sls.correctional_level_raw_text
   so imputed values with the form d*##IMPUTED are correctly joined */
    LEFT JOIN `{{project_id}}.{{analyst_data_dataset}}.us_mi_supervision_level_raw_text_mappings` omni_map
        ON REGEXP_EXTRACT(sls.correctional_level_raw_text, r'(\\d+)') \
         = omni_map.supervision_level_raw_text \
        AND omni_map.source = "OMNI"
    LEFT JOIN `{{project_id}}.{{analyst_data_dataset}}.us_mi_supervision_level_raw_text_mappings` coms_map
        ON REPLACE(sls.correctional_level_raw_text, "##IMPUTED", "") \
        = coms_map.supervision_level_raw_text \
        AND coms_map.source = 'COMS'
    WHERE state_code = "US_MI"
    AND compartment_level_1 IN ('SUPERVISION', 'SUPERVISION_OUT_OF_STATE')
    AND COALESCE(omni_map.is_sai, coms_map.is_sai)
    
    UNION ALL 
    /* COMS data */
    SELECT
        'US_MI' AS state_code, 
        pei.person_id, 
        SAFE_CAST(SAFE_CAST(start_date AS DATETIME) AS DATE) AS start_date, 
        SAFE_CAST(SAFE_CAST(end_date AS DATETIME) AS DATE) AS end_date, 
        TRUE as is_sai,
    FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.COMS_Specialties_latest`
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON LTRIM(Offender_Number, '0')= pei.external_id
        AND pei.state_code = 'US_MI'
        AND pei.id_type = "US_MI_DOC"
    WHERE Specialty = 'Special Alternative Incarceration'
    AND SAFE_CAST(SAFE_CAST(start_date AS DATETIME) AS DATE) != {nonnull_end_date_clause('SAFE_CAST(SAFE_CAST(end_date AS DATETIME) AS DATE)')}
),
{create_sub_sessions_with_attributes('sai_spans')},
deduped_sub_sessions AS (
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        is_sai,
    FROM sub_sessions_with_attributes
    GROUP BY 1,2,3,4,5)
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        is_sai AS meets_criteria,
    TO_JSON(STRUCT(is_sai AS supervision_level_is_sai)) AS reason,
    is_sai AS supervision_level_is_sai,
    FROM ({aggregate_adjacent_spans(table_name='deduped_sub_sessions',
                              attribute=['is_sai'])})
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    state_code=StateCode.US_MI,
    sessions_dataset=SESSIONS_DATASET,
    analyst_data_dataset=ANALYST_VIEWS_DATASET,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_MI,
        instance=DirectIngestInstance.PRIMARY,
    ),
    reasons_fields=[
        ReasonsField(
            name="supervision_level_is_sai",
            type=bigquery.enums.StandardSqlTypeNames.BOOL,
            description="Whether a client is on supervision after participating in SAI",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
