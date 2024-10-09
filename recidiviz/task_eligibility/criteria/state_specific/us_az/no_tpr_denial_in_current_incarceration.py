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
"""Describes spans of time when someone is not denied TPR in their current incarceration"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
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

_CRITERIA_NAME = "US_AZ_NO_TPR_DENIAL_IN_CURRENT_INCARCERATION"

_DESCRIPTION = """Describes spans of time when someone is not denied TPR in their current incarceration"""

_QUERY_TEMPLATE = f"""
WITH denials AS (
    SELECT 
        peid.state_code,
        peid.person_id,
        IFNULL(
            SAFE_CAST(SAFE.PARSE_DATETIME('%m/%d/%Y %I:%M:%S %p', tpr.CREATE_DTM) AS DATE),
            SAFE_CAST(LEFT(tpr.CREATE_DTM, 10) AS DATE))
        AS start_date,
        denial.description AS denied_reason,
        tpr.DENIED_OTHER_COMMENT AS denied_comment,
    FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.AZ_DOC_TRANSITION_PRG_REVIEW_latest` tpr
    LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.AZ_DOC_TRANSITION_PRG_ELIG_latest` tpe
        USING(TRANSITION_PRG_ELIGIBILITY_ID)
    LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.DOC_EPISODE_latest` doc_ep
        USING(DOC_ID)
    LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.LOOKUPS_latest` denial
        ON(DENIED_REASON_ID = denial.LOOKUP_ID)
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid
        ON doc_ep.PERSON_ID = peid.external_id
        AND peid.state_code = 'US_AZ'
        AND id_type = 'US_AZ_PERSON_ID'
    WHERE tpr.DENIED_REASON_ID IS NOT NULL
),

incarceration_with_denials_spans AS (
    SELECT 
        den.state_code,
        den.person_id,
        den.start_date,
        iss.end_date_exclusive AS end_date,
        den.denied_reason,
        den.denied_comment
    FROM denials den
    LEFT JOIN `{{project_id}}.{{sessions_dataset}}.incarceration_super_sessions_materialized` iss
    ON iss.person_id = den.person_id
        AND iss.state_code = den.state_code
        AND den.start_date BETWEEN iss.start_date AND {nonnull_end_date_exclusive_clause('iss.end_date_exclusive')}
        AND iss.state_code = 'US_AZ'
),

{create_sub_sessions_with_attributes('incarceration_with_denials_spans')}

SELECT 
    state_code,
    person_id,
    start_date, 
    end_date,
    FALSE AS meets_criteria,
    TO_JSON(STRUCT(
        STRING_AGG(denied_reason, ' - ') AS denied_reason, 
        STRING_AGG(denied_comment, ' - ') AS denied_comment)) 
    AS reason,
    STRING_AGG(denied_reason, ' - ') AS denied_reason,
    STRING_AGG(denied_comment, ' - ') AS denied_comment
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4
"""

_REASONS_FIELDS = [
    ReasonsField(
        name="denied_reason",
        type=bigquery.enums.StandardSqlTypeNames.STRING,
        description="The reason for the denial",
    ),
    ReasonsField(
        name="denied_comment",
        type=bigquery.enums.StandardSqlTypeNames.STRING,
        description="Free-text comment about the denial",
    ),
]

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        reasons_fields=_REASONS_FIELDS,
        state_code=StateCode.US_AZ,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        sessions_dataset=SESSIONS_DATASET,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_AZ, instance=DirectIngestInstance.PRIMARY
        ),
        meets_criteria_default=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
