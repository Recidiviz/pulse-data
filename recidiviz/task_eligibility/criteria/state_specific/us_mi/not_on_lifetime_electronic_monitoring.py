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
"""Defines a criteria span view that shows spans of time during which someone is on lifetime electronic monitoring
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_MI_NOT_ON_LIFETIME_ELECTRONIC_MONITORING"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which someone is on lifetime electronic monitoring
"""

_QUERY_TEMPLATE = f"""

WITH lifetime_em_sentences AS (
    /* This CTE checks for sentences where there is a lifetime_gps_flag and sets the end date to far in the future */
    SELECT
        sp.state_code,
        sp.person_id,
    --find the earliest sentence date w/ lifetime gps flag for each person 
        MIN(date_imposed) AS start_date,
        CAST(NULL AS DATE) AS end_date,
        TRUE as is_electronic_monitoring
    FROM `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sp
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON pei.state_code = 'US_MI'
        AND pei.state_code = sp.state_code
        AND pei.person_id = sp.person_id 
        AND pei.id_type = "US_MI_DOC_BOOK"
    INNER JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.ADH_OFFENDER_SENTENCE_latest` s
        ON sp.external_id = s.offender_sentence_id
        AND pei.external_id = s.offender_booking_id
    WHERE lifetime_gps_flag = '1'
    GROUP BY sp.state_code, sp.person_id
    ),
    /* additional lifetime electronic monitoring can now be found in a separate COMS table as well */
    COMS_lifetime_em_sentences AS (
    SELECT
        'US_MI' AS state_code, 
        pei.person_id, 
        SAFE_CAST(SAFE_CAST(start_date AS DATETIME) AS DATE) AS start_date,
        SAFE_CAST(SAFE_CAST(end_date AS DATETIME) AS DATE) AS end_date, 
        TRUE as is_electronic_monitoring
    FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.COMS_Specialties_latest`
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON LTRIM(Offender_Number, '0')= pei.external_id
        AND pei.state_code = 'US_MI'
        AND pei.id_type = "US_MI_DOC"
    WHERE Specialty = 'Lifetime GPS Monitoring'
    AND SAFE_CAST(SAFE_CAST(start_date AS DATETIME) AS DATE) != {nonnull_end_date_clause('SAFE_CAST(SAFE_CAST(end_date AS DATETIME) AS DATE)')}
    ),
    lifetime_em_spans AS (
    SELECT *
    FROM lifetime_em_sentences
    
    UNION ALL 
    
    SELECT *
    FROM COMS_lifetime_em_sentences
    ),
    {create_sub_sessions_with_attributes('lifetime_em_spans')},
    deduped_sub_sessions AS (
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        is_electronic_monitoring,
    FROM sub_sessions_with_attributes
    GROUP BY 1,2,3,4,5)
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        NOT is_electronic_monitoring AS meets_criteria,
        TO_JSON(STRUCT(
            start_date AS lifetime_em_date
        )) AS reason,
        start_date AS lifetime_em_date,
    FROM ({aggregate_adjacent_spans(table_name='deduped_sub_sessions',
                              attribute=['is_electronic_monitoring'])})
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
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    reasons_fields=[
        ReasonsField(
            name="lifetime_em_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="The date that a client started lifetime electronic monitoring",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
