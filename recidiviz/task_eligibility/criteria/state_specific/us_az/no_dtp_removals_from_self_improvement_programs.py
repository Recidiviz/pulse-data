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
"""Describes spans of time when someone has not been removed from self-improvement programs in their current incarceration - DTP"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
    revert_nonnull_end_date_clause,
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

_CRITERIA_NAME = "US_AZ_NO_DTP_REMOVALS_FROM_SELF_IMPROVEMENT_PROGRAMS"

_DESCRIPTION = """Describes spans of time when someone has not been removed from self-improvement programs in their current incarceration - DTP"""

_QUERY_TEMPLATE = f"""
WITH removals AS (
    SELECT 
        peid.state_code,
        peid.person_id,
        IFNULL(
            SAFE_CAST(SAFE.PARSE_DATETIME('%m/%d/%Y %I:%M:%S %p', eva.CREATE_DTM) AS DATE),
            SAFE_CAST(LEFT(eva.CREATE_DTM, 10) AS DATE))
        AS start_date,
        SAFE.PARSE_DATETIME('%m/%d/%Y %I:%M:%S %p', eva.CREATE_DTM) AS start_datetime,
        -- False if the person was removed from a self-improvement program and has not previously completed it
        NOT (REMOVED_FROM_SELF_IMPROVE_PRG = 'Y' AND PREVIOUSLY_COMPLETED_PRG = 'N') AS meets_criteria,
        (REMOVED_FROM_SELF_IMPROVE_PRG = 'Y') AS removed_from_program,
        (PREVIOUSLY_COMPLETED_PRG = 'Y') AS previously_completed_removed_program,
    FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.AZ_DOC_DRUG_TRANSITION_PRG_EVAL_latest` eva
    LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.AZ_DOC_DRUG_TRAN_PRG_ELIG_latest` tpe
        USING(DRUG_TRAN_PRG_ELIGIBILITY_ID)
    LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.DOC_EPISODE_latest` doc_ep
        USING(DOC_ID)
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid
        ON doc_ep.PERSON_ID = peid.external_id
        AND peid.state_code = 'US_AZ'
        AND id_type = 'US_AZ_PERSON_ID'
    -- When there are multiple removals on the same day, we want to keep the most recent one
    QUALIFY ROW_NUMBER() OVER(PARTITION BY peid.state_code, peid.person_id, start_date ORDER BY start_datetime DESC) = 1
),

removals_spans AS (
    SELECT 
        *,
        LEAD(start_date) OVER (PARTITION BY state_code, person_id ORDER BY start_date) AS end_date_exclusive
    FROM removals
),

incarceration_with_removals_spans AS (
    SELECT 
        rem.state_code,
        rem.person_id,
        rem.start_date,
        LEAST(
            {nonnull_end_date_clause('iss.end_date_exclusive')},
            {nonnull_end_date_clause('rem.end_date_exclusive')} 
            ) AS end_date,
        meets_criteria,
        removed_from_program,
        previously_completed_removed_program,
    FROM removals_spans rem
    LEFT JOIN `{{project_id}}.{{sessions_dataset}}.incarceration_super_sessions_materialized` iss
    ON iss.person_id = rem.person_id
        AND iss.state_code = rem.state_code
        AND rem.start_date BETWEEN iss.start_date AND {nonnull_end_date_exclusive_clause('iss.end_date_exclusive')}
        AND iss.state_code = 'US_AZ'
)

SELECT 
    state_code,
    person_id,
    start_date, 
    {revert_nonnull_end_date_clause('end_date')} AS end_date,
    meets_criteria,
    TO_JSON(STRUCT(
        removed_from_program AS removed_from_program, 
        previously_completed_removed_program AS previously_completed_removed_program)) 
    AS reason,
    removed_from_program,
    previously_completed_removed_program,
FROM incarceration_with_removals_spans
"""

_REASONS_FIELDS = [
    ReasonsField(
        name="removed_from_program",
        type=bigquery.enums.StandardSqlTypeNames.BOOL,
        description="Was this person removed from a self-improvement program?",
    ),
    ReasonsField(
        name="previously_completed_removed_program",
        type=bigquery.enums.StandardSqlTypeNames.BOOL,
        description="Has inmate previously completed program with refuse/remove tag?",
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
