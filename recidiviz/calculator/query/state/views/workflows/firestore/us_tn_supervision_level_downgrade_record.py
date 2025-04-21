# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Query for relevant metadata needed to support supervision level downgrade opportunity in Tennessee
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    today_between_start_date_and_nullable_end_date_exclusive_clause,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.workflows.us_tn.shared_ctes import (
    keep_contact_codes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_SUPERVISION_LEVEL_DOWNGRADE_RECORD_VIEW_NAME = (
    "us_tn_supervision_level_downgrade_record"
)

US_TN_SUPERVISION_LEVEL_DOWNGRADE_RECORD_DESCRIPTION = """
    Query for relevant metadata needed to support supervision level downgrade opportunity in Tennessee 
    """
US_TN_SUPERVISION_LEVEL_DOWNGRADE_RECORD_QUERY_TEMPLATE = f"""
WITH relevant_codes AS (
    -- Brings in contact codes from the person's current time in system
    SELECT *
    FROM `{{project_id}}.analyst_data.us_tn_relevant_contact_codes_materialized`
),
comments_clean AS (
    SELECT *
    FROM `{{project_id}}.analyst_data.us_tn_contact_comments_preprocessed_materialized`
),
base_query AS (
    SELECT 
       tes.person_id,
       pei.external_id, 
       tes.state_code,
       tes.reasons AS reasons,
       tes.is_eligible,
       tes.is_almost_eligible,
       viol_contact.contact_type AS note_title,
       viol_contact.contact_date AS event_date,
       CAST(NULL AS STRING) AS note_body,
       "VIOLATIONS SINCE LATEST STRONG R" AS criteria,
    FROM `{{project_id}}.{{task_eligibility_dataset}}.supervision_level_downgrade_materialized`  tes
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        USING(person_id)
    /*
    Since violations that may make someone's supervision level higher than assessment level are more relevant if they've
    happened since the last assessment, bring these in separately than the above infrastructure for pulling contact comments
    */
    LEFT JOIN relevant_codes viol_contact
        ON tes.person_id = viol_contact.person_id
        AND viol_contact.contact_date >= CAST(JSON_VALUE(reasons[0].reason.latest_assessment_date) AS DATE)
        AND viol_contact.contact_type IN ('PWAR','VWAR','VRPT','ARRP')
    WHERE {today_between_start_date_and_nullable_end_date_exclusive_clause(
                start_date_column="tes.start_date",
                end_date_column="tes.end_date"
            )}
        AND tes.is_eligible
        AND tes.state_code = 'US_TN'
),
case_notes_array AS (
    SELECT person_id,
            TO_JSON(
                ARRAY_AGG(
                    IF(note_title IS NOT NULL,
                        STRUCT(note_title, note_body, event_date, criteria),
                        NULL
                    )
                IGNORE NULLS
                ORDER BY criteria, event_date
                )
            ) AS case_notes    
    FROM (
        SELECT person_id,
              contact_type AS note_title,
              contact_date AS event_date,
              contact_comment AS note_body,
              "COURT HEARINGS" AS criteria,
        FROM ({keep_contact_codes(
            codes_cte="relevant_codes",
            comments_cte="comments_clean",
            where_clause_codes_cte="WHERE contact_type IN ('COHC')",
            keep_last=False
            )})
        
        UNION ALL
    
        SELECT person_id,
              contact_type AS note_title,
              contact_date AS event_date,
              contact_comment AS note_body,
              "TN ROC" AS criteria,
        FROM ({keep_contact_codes(
            codes_cte="relevant_codes",
            comments_cte="comments_clean",
            where_clause_codes_cte="WHERE contact_type IN ('TRO1','TRO2','TRO3')",
            keep_last=False
            )})
        
        UNION ALL 
        
        SELECT person_id,
              note_title,
              event_date,
              note_body,
              criteria
        FROM base_query
    )
    GROUP BY 1
)
SELECT 
    person_id,
    external_id,
    state_code,
    reasons,
    is_eligible,
    is_almost_eligible,
    case_notes,
    --TODO(#27391): Deprecate this field
    IF(metadata_violations[offset(0)].event_date IS NULL, [], metadata_violations) AS metadata_violations,
FROM (
    SELECT
        person_id,
        external_id,
        state_code,
        ANY_VALUE(reasons) AS reasons,
        --TODO(#27391): Remove this `LOGICAL_AND` when the violations group by is removed
        LOGICAL_AND(is_eligible) AS is_eligible,
        LOGICAL_AND(is_almost_eligible) AS is_almost_eligible,
        --TODO(#27391): Deprecate this field
        ARRAY_AGG(
            IF(
                b.event_date IS NOT NULL,
                STRUCT(
                    b.event_date,
                    b.note_title
                    ),
                NULL
            )
            ORDER BY b.event_date, b.note_title
        ) AS metadata_violations,
        FROM
            base_query b
        GROUP BY 1,2,3
)
LEFT JOIN 
    case_notes_array
USING(person_id)
"""

US_TN_SUPERVISION_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_TN_SUPERVISION_LEVEL_DOWNGRADE_RECORD_VIEW_NAME,
    view_query_template=US_TN_SUPERVISION_LEVEL_DOWNGRADE_RECORD_QUERY_TEMPLATE,
    description=US_TN_SUPERVISION_LEVEL_DOWNGRADE_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_TN
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_SUPERVISION_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER.build_and_print()
