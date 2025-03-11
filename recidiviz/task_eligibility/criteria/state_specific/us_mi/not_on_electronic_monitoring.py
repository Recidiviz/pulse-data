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
"""This criteria view builder defines spans of time that clients are not on a supervision level that indicates
electronic monitoring
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

_CRITERIA_NAME = "US_MI_NOT_ON_ELECTRONIC_MONITORING"

_DESCRIPTION = """This criteria view builder defines spans of time that clients are not on a supervision level that
 indicates electronic monitoring 
"""

_QUERY_TEMPLATE = f"""
WITH em_spans AS (
/* This CTE creates an is_electronic_monitoring variable that sets a boolean for whether the supervision level
includes electronic monitoring */
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive AS end_date,
        TRUE AS is_electronic_monitoring
    #TODO(#20035) replace with supervision level raw text sessions once views agree
    FROM `{{project_id}}.{{sessions_dataset}}.compartment_sub_sessions_materialized` sls
    /* Using regex to match digits in sls.correctional_level_raw_text so imputed values with the form 
    d*##IMPUTED are correctly joined */
    LEFT JOIN `{{project_id}}.{{analyst_data_dataset}}.us_mi_supervision_level_raw_text_mappings` omni_map
        ON REGEXP_EXTRACT(sls.correctional_level_raw_text, r'(\\d+)') = omni_map.supervision_level_raw_text and omni_map.source = 'OMNI'
    LEFT JOIN `{{project_id}}.{{analyst_data_dataset}}.us_mi_supervision_level_raw_text_mappings` coms_map
        ON REPLACE(sls.correctional_level_raw_text, "##IMPUTED", "") = coms_map.supervision_level_raw_text and coms_map.source = 'COMS'
    WHERE state_code = "US_MI"
    AND compartment_level_1 = 'SUPERVISION'
    AND (omni_map.is_em OR coms_map.is_em)
    
    /* additional electronic monitoring can now be found in a separate COMS table as well */
    UNION ALL 
    SELECT
        'US_MI' AS state_code, 
        pei.person_id, 
        SAFE_CAST(SAFE_CAST(start_date AS DATETIME) AS DATE) AS start_date,
        SAFE_CAST(SAFE_CAST(end_date AS DATETIME) AS DATE) AS end_date,  
        TRUE AS is_electronic_monitoring
    FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.COMS_Specialties_latest`
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON Offender_Number = pei.external_id
        AND pei.state_code = 'US_MI'
        AND pei.id_type = "US_MI_DOC"
    WHERE Specialty = 'GPS Monitoring'
    AND SAFE_CAST(SAFE_CAST(start_date AS DATETIME) AS DATE) != {nonnull_end_date_clause('SAFE_CAST(SAFE_CAST(end_date AS DATETIME) AS DATE)')}
),
{create_sub_sessions_with_attributes('em_spans')},
deduped_sub_sessions AS (
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        is_electronic_monitoring,
    FROM sub_sessions_with_attributes
    GROUP BY 1,2,3,4,5),
/* This boolean is then used to aggregate electronic monitoring supervision levels and non electronic monitoring
supervision levels */
sessionized_cte AS (
    {aggregate_adjacent_spans(table_name='deduped_sub_sessions',
                              attribute=['is_electronic_monitoring'])}
)
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        NOT is_electronic_monitoring AS meets_criteria,
        TO_JSON(STRUCT(start_date AS eligible_date)) AS reason,
        start_date AS em_start_date,
    FROM sessionized_cte
    WINDOW w AS (PARTITION BY person_id, state_code ORDER BY start_date)
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
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
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="em_start_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="The date that a client started electronic monitoring",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
