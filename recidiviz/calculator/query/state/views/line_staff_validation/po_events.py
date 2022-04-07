# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Events from the PO report and synthesized from other data

To generate the BQ view, run:
    python -m recidiviz.calculator.query.state.views.line_staff_validation.po_events
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.case_triage.views import dataset_config as case_triage_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PO_EVENTS_VIEW_NAME = "po_events"

PO_EVENTS_DESCRIPTION = """"Events from the PO report and synthesized from other data"""

PO_EVENTS_QUERY_TEMPLATE = """
WITH synthetic_events AS (
    SELECT 
        supervision_officer_sessions_materialized.state_code,
        supervision_officer_sessions_materialized.supervising_officer_external_id AS officer_external_id,
        'New To Caseload' AS event,
        state_person_external_id.external_id AS person_external_id,
        state_person.full_name,
        supervision_officer_sessions_materialized.start_date AS event_date
    FROM `{project_id}.{sessions_dataset}.supervision_officer_sessions_materialized` supervision_officer_sessions_materialized
    JOIN `{project_id}.{state_dataset}.state_person` state_person USING (state_code, person_id)
    JOIN `{project_id}.{state_dataset}.state_person_external_id` state_person_external_id USING (state_code, person_id)
        
), 
source_table AS (
    SELECT *
    FROM `{project_id}.{po_report_dataset}.report_data_by_officer_by_month_materialized` po_monthly_report_data
    WHERE DATE(year, month, 1) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE('US/Eastern'), MONTH), INTERVAL 3 MONTH)
),
unordered_po_events AS (
    SELECT
        po_monthly_report_data.state_code,
        po_monthly_report_data.officer_external_id,
        'Successful Completion' AS event,
        pos_discharges_clients.person_external_id,
        pos_discharges_clients.full_name,
        pos_discharges_clients.successful_completion_date AS event_date
     FROM source_table po_monthly_report_data
     JOIN UNNEST(pos_discharges_clients) AS pos_discharges_clients ON TRUE
     
     UNION ALL 
     
    SELECT
        po_monthly_report_data.state_code,
        po_monthly_report_data.officer_external_id,
        'Absconsion' AS event,
        absconsions_clients.person_external_id,
        absconsions_clients.full_name,
        absconsions_clients.absconsion_report_date AS event_date
     FROM source_table po_monthly_report_data
     JOIN UNNEST(absconsions_clients) AS absconsions_clients ON TRUE
     
  
    UNION ALL 
     
    SELECT
        po_monthly_report_data.state_code,
        po_monthly_report_data.officer_external_id,
        CASE 
            WHEN revocations_clients.revocation_violation_type = 'TECHNICAL' THEN 'Technical Revocation'
            WHEN revocations_clients.revocation_violation_type = 'NEW_CRIME' THEN 'New Crime Revocation'
            ELSE 'REVOCATION'
        END AS event,
        revocations_clients.person_external_id,
        revocations_clients.full_name,
        revocations_clients.revocation_report_date AS event_date
     FROM source_table po_monthly_report_data
     JOIN UNNEST(revocations_clients) AS revocations_clients ON TRUE
     
     
    UNION ALL 
     
    SELECT
        po_monthly_report_data.state_code,
        po_monthly_report_data.officer_external_id,
        'Earned Discharge' AS event,
        earned_discharges_clients.person_external_id,
        earned_discharges_clients.full_name,
        earned_discharges_clients.earned_discharge_date AS event_date
     FROM source_table po_monthly_report_data
     JOIN UNNEST(earned_discharges_clients) AS earned_discharges_clients ON TRUE
 ),
 all_events AS (
     SELECT 
        unordered_po_events.state_code,
        unordered_po_events.officer_external_id,
        unordered_po_events.event,
        unordered_po_events.event_date,
        unordered_po_events.person_external_id,
        unordered_po_events.full_name AS client_full_name,
     FROM unordered_po_events
     
     UNION ALL 
     
     SELECT 
        synthetic_events.state_code,
        synthetic_events.officer_external_id,
        synthetic_events.event,
        synthetic_events.event_date,
        synthetic_events.person_external_id,
        synthetic_events.full_name AS client_full_name
    FROM synthetic_events
)
SELECT
        all_events.state_code,
        all_events.officer_external_id,
        JSON_EXTRACT_SCALAR(etl_officers.full_name, '$.full_name') AS officer_full_name,
        all_events.event,
        all_events.event_date,
        all_events.person_external_id AS client_external_id,
        all_events.client_full_name
FROM all_events
JOIN `{project_id}.{case_triage_dataset}.etl_officers_materialized` etl_officers ON etl_officers.external_id = all_events.officer_external_id
WHERE event_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 90 DAY) 
ORDER BY all_events.event_date
"""

PO_EVENTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.LINESTAFF_DATA_VALIDATION,
    view_id=PO_EVENTS_VIEW_NAME,
    should_materialize=True,
    view_query_template=PO_EVENTS_QUERY_TEMPLATE,
    description=PO_EVENTS_DESCRIPTION,
    po_report_dataset=dataset_config.PO_REPORT_DATASET,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    case_triage_dataset=case_triage_dataset_config.CASE_TRIAGE_DATASET,
    state_dataset=dataset_config.STATE_BASE_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PO_EVENTS_VIEW_BUILDER.build_and_print()
