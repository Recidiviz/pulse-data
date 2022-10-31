# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Query for relevant case notes needed to determine eligibility
for early discharge from supervision in Idaho
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.pipeline.supplemental.dataset_config import (
    SUPPLEMENTAL_DATA_DATASET,
)
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ID_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_NAME = (
    "us_id_complete_discharge_early_from_supervision_request_record"
)

US_ID_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_DESCRIPTION = """
    View of relevant case notes for determining eligibility 
    for early discharge from supervision in Idaho 
    """
US_ID_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_QUERY_TEMPLATE = f"""
   WITH notes AS(
    --violations
        SELECT 
            v.external_id,
            v.state_code,
            v.person_id,
            COALESCE(vr.response_date,v.violation_date) AS event_date,
            COALESCE(vt.violation_type, cn.agnt_note_title) AS note_title,
            IF(cn.agnt_note_txt IS NULL, "--",cn.agnt_note_txt) AS note_body,
            "Violations" AS criteria,
        FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation` v
        LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation_type_entry` vt
            USING (supervision_violation_id, person_id, state_code)
        LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation_response` vr
             USING (supervision_violation_id, person_id, state_code)
        LEFT JOIN `{{project_id}}.{{us_id_raw_data_up_to_date_dataset}}.agnt_case_updt_latest` cn
                ON cn.agnt_case_updt_id = SPLIT((v.external_id), '-')[SAFE_OFFSET(1)]
        WHERE (violation_type NOT IN ('TECHNICAL') OR violation_type IS NULL)
        AND DATE_ADD(COALESCE(vr.response_date,v.violation_date), INTERVAL 90 DAY) >= CURRENT_DATE('US/Pacific')
    --ncic/ilets and new crime violations
        UNION ALL
        SELECT 
            a.agnt_case_updt_id AS external_id,
            a.state_code,
            a.person_id,
            a.create_dt AS event_date,
            cn.agnt_note_title AS note_title, 
            cn.agnt_note_txt AS note_body, 
            CASE 
                WHEN ncic_ilets_nco_check THEN "No new criminal activity check"
                WHEN new_crime THEN "Violations"
                ELSE NULL
            END AS criteria
        FROM `{{project_id}}.{{supplemental_dataset}}.us_id_case_note_matched_entities` a
        LEFT JOIN `{{project_id}}.{{us_id_raw_data_up_to_date_dataset}}.agnt_case_updt_latest` cn
            USING(agnt_case_updt_id)
        WHERE (ncic_ilets_nco_check OR new_crime)
        --only select ncic checks and new crime within the past 90 days
        AND DATE_ADD(a.create_dt, INTERVAL 90 DAY) >= CURRENT_DATE('US/Pacific')
    --Community service,Special conditions, Interlock, Treatment, Specialty court, Transfer chrono
        UNION ALL
        SELECT 
            a.agnt_case_updt_id AS external_id,
            a.state_code,
            a.person_id,
            a.create_dt AS event_date,
            cn.agnt_note_title AS note_title, 
            cn.agnt_note_txt AS note_body,
            CASE 
                WHEN (community_service AND NOT not_cs AND NOT agents_warning) THEN "Community service"
                WHEN case_plan THEN "Special Conditions"
                WHEN interlock THEN "Interlock"
                WHEN any_treatment THEN "Treatment"
                WHEN (specialty_court AND court AND NOT psi) THEN "Specialty court"
                WHEN transfer_chrono THEN "Transfer chrono"
                ELSE NULL
            END AS criteria
          FROM `{{project_id}}.{{supplemental_dataset}}.us_id_case_note_matched_entities` a
          LEFT JOIN `{{project_id}}.{{us_id_raw_data_up_to_date_dataset}}.agnt_case_updt_latest` cn
            USING(agnt_case_updt_id)
          WHERE (community_service AND NOT not_cs AND NOT agents_warning)
                OR case_plan
                OR interlock
                OR any_treatment
                OR (specialty_court AND court AND NOT psi)
                OR (transfer_chrono AND DATE_ADD(a.create_dt, INTERVAL 1 YEAR) >= CURRENT_DATE('US/Pacific'))
    --DUI notes
        UNION ALL
        SELECT 
            a.agnt_case_updt_id AS external_id,
            a.state_code,
            a.person_id,
            a.create_dt AS event_date,
            cn.agnt_note_title AS note_title, 
            cn.agnt_note_txt AS note_body,
            "DUI" AS criteria
        FROM `{{project_id}}.{{supplemental_dataset}}.us_id_case_note_matched_entities` a
        LEFT JOIN `{{project_id}}.{{us_id_raw_data_up_to_date_dataset}}.agnt_case_updt_latest` cn
            USING(agnt_case_updt_id)
        WHERE dui 
            AND NOT not_m_dui
            --only include DUI notes within the past 12 months 
            AND DATE_ADD(a.create_dt, INTERVAL 12 MONTH) >= CURRENT_DATE('US/Pacific')
    ),
    latest_notes AS(
    SELECT
        n.state_code,
        n.person_id,
        TO_JSON(ARRAY_AGG(IF(n.note_title IS NOT NULL, STRUCT(n.note_title, n.note_body, n.event_date, n.criteria),NULL) IGNORE NULLS)) AS case_notes,
    FROM notes n
    --only select notes during the current supervision session
    INNER JOIN `{{project_id}}.{{sessions_dataset}}.supervision_super_sessions_materialized` ses
        ON n.person_id = ses.person_id
        AND CURRENT_DATE('US/Pacific') BETWEEN ses.start_date AND {nonnull_end_date_clause('ses.end_date')}
        AND n.event_date BETWEEN ses.start_date AND {nonnull_end_date_clause('ses.end_date')}
    GROUP BY 1,2
    ),
    client_notes AS (
        SELECT 
            pei.external_id,
            tes.state_code,
            tes.start_date AS eligible_start_date,
            ses.start_date AS supervision_start_date,
            DATE_DIFF(proj.projected_completion_date_max, CURRENT_DATE('US/Pacific'), DAY) AS days_remaining_on_supervision,
            ARRAY_AGG(tes.reasons)[ORDINAL(1)] AS reasons,
            ARRAY_AGG(n.case_notes IGNORE NULLS ORDER BY ses.start_date)[ORDINAL(1)] AS case_notes,
        FROM `{{project_id}}.{{task_eligibility_dataset}}.all_tasks_materialized` tes 
        INNER JOIN `{{project_id}}.{{sessions_dataset}}.supervision_super_sessions_materialized` ses
            ON tes.state_code = ses.state_code
            AND tes.person_id = ses.person_id
            AND tes.start_date BETWEEN ses.start_date AND {nonnull_end_date_clause('ses.end_date')}
            AND task_name IN ( 
                "COMPLETE_DISCHARGE_EARLY_FROM_PROBATION_SUPERVISION_REQUEST",
                "COMPLETE_DISCHARGE_EARLY_FROM_PAROLE_DUAL_SUPERVISION_REQUEST"
                )
        LEFT JOIN `{{project_id}}.{{sessions_dataset}}.supervision_projected_completion_date_spans_materialized` proj
            ON proj.state_code = ses.state_code
            AND proj.person_id = ses.person_id
            --use the projected completion date from the current span
            AND CURRENT_DATE('US/Pacific') BETWEEN proj.start_date AND {nonnull_end_date_exclusive_clause('proj.end_date')}
        LEFT JOIN latest_notes n
            ON ses.state_code = n.state_code
            AND ses.person_id = n.person_id
        INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
            ON ses.state_code = pei.state_code 
            AND ses.person_id = pei.person_id
            AND pei.id_type = "US_ID_DOC"
            --only individuals that are currently eligible for early discharge
        WHERE CURRENT_DATE('US/Pacific') BETWEEN tes.start_date AND {nonnull_end_date_exclusive_clause('tes.end_date')}
            AND tes.is_eligible
            AND tes.state_code = 'US_ID'
        GROUP BY 1,2,3,4,5
    )
   SELECT * FROM client_notes
"""

US_ID_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_ID_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_NAME,
    view_query_template=US_ID_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_QUERY_TEMPLATE,
    description=US_ID_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    supplemental_dataset=SUPPLEMENTAL_DATA_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_ID
    ),
    should_materialize=True,
    us_id_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region("us_id"),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ID_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.build_and_print()
