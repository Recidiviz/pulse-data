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
# =============================================================================

"""Defines a criteria view that shows spans of time for which supervision clients
are compliant with type agnostic contacts
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
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

_CRITERIA_NAME = "US_TX_MEETS_TYPE_AGNOSTIC_CONTACT_STANDARDS"

_DESCRIPTION = """Defines a criteria view that shows spans of time for which supervision clients
meet standards for type agnostic contacts.
"""
_QUERY_TEMPLATE = f"""
WITH
-- Create periods with case type and supervision level information
person_info AS (
   SELECT 
      person_id,
      start_date,
      end_date,
      correctional_level_start AS supervision_level,
      case_type_start AS case_type,
      "US_TX" as state_code,
    FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` 
    WHERE state_code = "US_TX" AND
    -- Make sure that we have enough info to create span
    correctional_level_start IS NOT NULL AND case_type_start IS NOT NULL
),
-- Aggregate above periods by supervision_level and case_type
person_info_agg AS (
    {aggregate_adjacent_spans(
        table_name='person_info',
        attribute=['supervision_level','case_type'],
        session_id_output_name='person_info_agg',
        end_date_field_name='end_date'
    )}
),
-- Create list of types and amounts required
contact_required AS (
  SELECT
        supervision_level,
        case_type,
        CONCAT(
            CASE 
                WHEN CAST(SCHEDULED_HOME_REQ AS INT64) != 0 
                    THEN CONCAT("Scheduled home: ", SCHEDULED_HOME_REQ, " ")
                ELSE ""
            END,
            CASE
                WHEN CAST(SCHEDULED_FIELD_REQ AS INT64) != 0 
                    THEN CONCAT("Scheduled field: ", SCHEDULED_FIELD_REQ, " ")
                ELSE ""
            END,
            CASE
                WHEN CAST(UNSCHEDULED_FIELD_REQ AS INT64) != 0 
                    THEN CONCAT("Unscheduled field: ", UNSCHEDULED_FIELD_REQ, " ")
                ELSE ""
            END,
            CASE
                WHEN CAST(UNSCHEDULED_HOME_REQ AS INT64) != 0 
                    THEN CONCAT("Unscheduled home: ", UNSCHEDULED_HOME_REQ, " ")
                ELSE ""
            END,
            CASE
                WHEN CAST(SCHEDULED_ELECTRONIC_REQ AS INT64) != 0 
                    THEN CONCAT("Scheduled electronic: ", SCHEDULED_ELECTRONIC_REQ, " ")
                ELSE ""
            END,
            CASE
                WHEN CAST(SCHEDULED_OFFICE_REQ AS INT64) != 0 
                    THEN CONCAT("Scheduled office: ", SCHEDULED_OFFICE_REQ, " ")
                ELSE ""
            END
        ) AS types_and_amounts_due,
    FROM `{{project_id}}.{{raw_data_up_to_date_dataset}}.RECIDIVIZ_REFERENCE_ContactCadenceAgnostic_latest`
),
-- Create contacts table by adding scheduled prefix
contact_info AS (
    SELECT 
        person_id,
        contact_date,
        CONCAT(
            CASE 
                WHEN contact_reason_raw_text = "REGULAR VISIT" 
                    THEN "SCHEDULED " 
                ELSE "UNSCHEDULED " 
            END 
        || contact_method_raw_text ) AS contact_type,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_contact` 
),
-- Creates a record of contact types that can be accepted for a given supervision period
person_info_with_contact_types_accepted AS (
    SELECT 
        pia.supervision_level,
        pia.case_type,
        pia.person_id,
        contact_types_accepted,
        pia.start_date,
        pia.end_date,
        frequency_in_months,
    FROM person_info_agg pia
    LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_dataset}}.RECIDIVIZ_REFERENCE_ContactCadenceAgnostic_latest` cca
        ON cca.supervision_level = pia.supervision_level AND  cca.case_type = pia.case_type
),
-- Creates a table that counts contacts by distinct types in the last x months
contacts_compliance AS (
    SELECT
        p.person_id,
        p.supervision_level,
        p.case_type,
        start_date,
        end_date,
        c.contact_date as eval_date,
        c.contact_type,
        p.contact_types_accepted,
        COUNT(cc.contact_date) AS contact_count,
    FROM person_info_with_contact_types_accepted p
    JOIN contact_info c
        ON c.person_id = p.person_id 
        AND c.contact_date BETWEEN start_date AND COALESCE(end_date, DATE("9999-12-31"))
    JOIN contact_info cc ON
        c.person_id = cc.person_id 
    AND p.contact_types_accepted LIKE concat("%",cc.contact_type,"%")
    AND cc.contact_type IS NOT NULL AND cc.contact_date BETWEEN DATE_SUB(c.contact_date, INTERVAL CAST(frequency_in_months AS INT64) MONTH) 
        AND c.contact_date AND c.contact_type = cc.contact_type
    GROUP BY person_id,supervision_level,case_type,start_date,end_date, contact_types_accepted,c.contact_date,c.contact_type
),
-- Check for compliance based on contact standards for that given supervision level and case type
compliance_check AS (
    SELECT 
        person_id,
        cc.supervision_level,
        cc.case_type,
        cc.contact_types_accepted,
        start_date,
        end_date,
        eval_date,
        DATE_ADD(date(eval_date), INTERVAL CAST(caa.frequency_in_months AS INT64) MONTH) as eval_end_date,
        CASE
            WHEN CAST(SCHEDULED_HOME_REQ AS INT64) <= contact_count AND CAST(SCHEDULED_HOME_REQ AS INT64) != 0 AND contact_type = "SCHEDULED HOME"
                THEN "COMPLIANT"
            WHEN CAST(SCHEDULED_FIELD_REQ AS INT64) <= contact_count AND CAST(SCHEDULED_FIELD_REQ AS INT64) != 0 AND contact_type = "SCHEDULED FIELD"
                THEN "COMPLIANT"
            WHEN CAST(UNSCHEDULED_FIELD_REQ AS INT64) <= contact_count AND CAST(UNSCHEDULED_FIELD_REQ AS INT64) != 0 AND contact_type = "UNSCHEDULED FIELD"
                THEN "COMPLIANT"
            WHEN CAST(UNSCHEDULED_HOME_REQ AS INT64) <= contact_count AND CAST(UNSCHEDULED_HOME_REQ AS INT64) != 0 AND contact_type = "UNSCHEDULED HOME"
                THEN "COMPLIANT"
            WHEN CAST(SCHEDULED_ELECTRONIC_REQ AS INT64) <= contact_count AND CAST(SCHEDULED_ELECTRONIC_REQ AS INT64) != 0 AND contact_type = "SCHEDULED ELECTRONIC"
                THEN "COMPLIANT"
            WHEN CAST(SCHEDULED_OFFICE_REQ AS INT64) <= contact_count AND CAST(SCHEDULED_OFFICE_REQ AS INT64) != 0 AND contact_type = "SCHEDULED OFFICE"
                THEN "COMPLIANT"
            ELSE "NON-COMPLIANT"
        END AS compliance,
        LEAD(eval_date) OVER (PARTITION BY person_id, start_date, cc.case_type, cc.contact_types_accepted, cc.supervision_level ORDER BY eval_date) AS next_eval_date,
    FROM contacts_compliance cc
    LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_dataset}}.RECIDIVIZ_REFERENCE_ContactCadenceAgnostic_latest` caa
         USING(supervision_level, case_type, contact_types_accepted)
),
-- Create a list of critical dates
event_boundaries AS (
    SELECT
        person_id,
        start_date AS eval_date,
        start_date AS start_date,
        "NON-COMPLIANT" AS compliance,   
        contact_types_accepted,
    FROM person_info_with_contact_types_accepted

    UNION ALL

    SELECT
        person_id,
        end_date AS eval_date,
        start_date AS start_date,
        "NON-COMPLIANT" AS compliance,   
        contact_types_accepted,
    FROM person_info_with_contact_types_accepted
    
    UNION ALL

    SELECT
        person_id,
        eval_date,
        start_date,
        compliance,
        contact_types_accepted,
    FROM compliance_check
    WHERE compliance = "COMPLIANT"

    UNION ALL

    SELECT
        person_id,
        eval_end_date as eval_date,
        start_date,
        CASE 
            WHEN eval_end_date < next_eval_date 
                THEN "NON-COMPLIANT" 
            ELSE 
                COALESCE(LEAD(compliance) OVER(PARTITION BY person_id, start_date, contact_types_accepted order by   eval_date), "NON-COMPLIANT") end as compliance,
        contact_types_accepted,
    FROM compliance_check
    WHERE compliance = "COMPLIANT"
),
-- Orders critical dates
 ordered_dates AS (
    SELECT 
        eb.person_id,
        eval_date AS start_date,
        compliance,
        eb.contact_types_accepted,
        pi.supervision_level,
        pi.case_type,
        "US_TX" AS state_code,
        LEAD(eval_date) OVER (PARTITION BY eb.person_id,eb.start_date,eb.contact_types_accepted ORDER BY eval_date) AS end_date,
    FROM event_boundaries eb 
    LEFT JOIN person_info_with_contact_types_accepted pi
        USING (person_id,contact_types_accepted, start_date) 
    WHERE eval_date IS NOT NULL

),
-- Aggregate above periods by supervision_level and case_type
comp_agg AS (
    {aggregate_adjacent_spans(
        table_name='ordered_dates',
        attribute=['supervision_level','case_type','contact_types_accepted','compliance'],
        session_id_output_name='comp_agg',
        end_date_field_name='end_date'
    )}
),
-- Creates final periods of compliance
periods AS (
  SELECT 
    "US_TX" as state_code,
    person_id,
    start_date,
    end_date,
    compliance,
    contact_types_accepted,
    CASE WHEN 
        compliance = "COMPLIANT"
            THEN TRUE 
        ELSE FALSE
    END AS meets_criteria,
    TO_JSON(STRUCT(compliance AS compliance)) AS reason,
    CASE WHEN 
        compliance = "COMPLIANT"
            THEN start_date
        ELSE LAG(start_date) OVER (PARTITION BY person_id, contact_types_accepted ORDER BY start_date)
    END AS last_contact_date,
    CASE WHEN 
        compliance = "COMPLIANT"
            THEN end_date
        ELSE start_date
    END AS contact_due_date,
    types_and_amounts_due,
  FROM comp_agg
  LEFT JOIN contact_required USING (supervision_level, case_type)
)
SELECT 
  *
FROM periods
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        state_code=StateCode.US_TX,
        sessions_dataset=SESSIONS_DATASET,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_TX, instance=DirectIngestInstance.PRIMARY
        ),
        reasons_fields=[
            ReasonsField(
                name="last_contact_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date of the last contact.",
            ),
            ReasonsField(
                name="contact_due_date",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Due date of the contact.",
            ),
            ReasonsField(
                name="types_and_amounts_due",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="The type and amount due.",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
