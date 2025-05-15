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
are compliant with edge case home contacts. The following edge case are as follow:

- "A home contact and residence validation are required within 30 calendar days of the initial
office contact for a newly released or transferred client, unless otherwise specified in
Section VII. For a client on normal reporting status, home contacts are required and are
based on their assigned TRAS supervision level and caseload type. Two separate OIMS
contact entries are not required if the home contact and residence validation occur on the
same day and the client was present."

- "A home contact is required within 30 calendar days of a client`s change of address."

- "The PO shall conduct a home contact within five business days after a client is returned
to supervision from any correctional facility, such as a TDCJ-Correctional Institutions
Division (CID) unit, TDCJ state jail, Intermediate Sanction Facility (ISF), Substance
Abuse Felony Punishment Facility (SAFPF), or county jail."
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TX_MEETS_EDGE_CASE_HOME_CONTACT_STANDARDS"

_DESCRIPTION = """Defines a criteria view that shows spans of time for which supervision clients
meet standards for edge case home contacts.
"""
_QUERY_TEMPLATE = f"""
WITH
-- Create periods with case type and supervision level information
person_info AS (
   SELECT 
      sp.person_id,
      start_date,
      termination_date AS end_date,
      supervision_level,
      case_type,
      case_type_raw_text,
      sp.state_code,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period` sp
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_supervision_case_type_entry` ct
      USING(supervision_period_id)
    WHERE sp.state_code = "US_TX"
),
-- Aggregate above periods by supervision_level and case_type
person_info_agg AS (
    {aggregate_adjacent_spans(
        table_name='person_info',
        attribute=['supervision_level','case_type','case_type_raw_text'],
        session_id_output_name='person_info_agg',
        end_date_field_name='end_date'
    )}
),
-- Creates table of all contacts and adds scheduled/unscheduled prefix
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
        external_id,
        Contact_reason,
        contact_method_raw_text
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_contact` 
),
-- Create periods of time for the home contact visit 30 days after the intitial
-- office visit. #TODO(#39631) Refactor this CTE to use address periods
office_visit AS
(
    SELECT
        person_id,
        contact_date AS start_date,
        LAST_DAY(DATE_ADD(DATE(contact_date), INTERVAL 1 MONTH)) AS end_date,
        "Initial Home Contact" as reason_for_contact,
    FROM contact_info
    WHERE Contact_reason = "INITIAL_CONTACT" AND contact_method_raw_text != "HOME"
),
address_cte AS (
    SELECT DISTINCT
        SID_Number, 
        Address as AddressLoc, 
        Creation_Date, 
        LAG(Address) OVER (PARTITION BY SID_Number ORDER BY Creation_Date) AS prev_address,
        LAG(Creation_Date) OVER (PARTITION BY SID_Number ORDER BY Creation_Date) AS prev_creation_date 
    FROM `{{project_id}}.{{raw_data_up_to_date_dataset}}.ClientData_latest` 
    WHERE Address IS NOT NULL
),
address_change_periods AS (
    SELECT 
        SID_Number,
        person_id,
        DATE(Creation_Date) as start_date,
        LAST_DAY(DATE_ADD(DATE(Creation_Date), INTERVAL 1 MONTH)) AS end_date,
        "Home Contact due to Address Change" AS reason_for_contact,
    FROM address_cte a
    LEFT JOIN  `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` e
        ON  a.SID_Number = e.external_id
    WHERE prev_address IS DISTINCT FROM AddressLoc 
        AND prev_creation_date IS DISTINCT FROM Creation_date 
        AND prev_creation_date IS NOT NULL
),
status_cte AS (
    SELECT DISTINCT 
        JSON_EXTRACT(supervision_period_metadata, '$.status') AS status,
        person_id,
        start_date, 
        termination_date   
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period` 
),
status_change_cte AS (
        SELECT 
        person_id,
        status,
        start_date,
        termination_date,
        LAG(status) OVER (PARTITION BY person_id ORDER BY start_date) as prev_status,
        LAG(termination_date) OVER (Partition by person_id ORDER BY start_date) as prev_end_date,
    from status_cte
),
status_change_periods AS (
    SELECT 
        person_id,
        start_date,
        DATE_ADD(DATE(start_date), INTERVAL 5 DAY) AS end_date,
        "Home Contact due to Return to Supervision" AS reason_for_contact
    FROM status_change_cte 
    -- Filter to only status changes
    WHERE status IS DISTINCT FROM prev_status 
        --ignore supervision starts        
        AND prev_status IS NOT NULL
        --ensure that changes are within the same supervision stint         
        AND prev_end_date = start_date 
        AND prev_status IN 
            ("\\"IN CUSTODY - COUNTY JAIL\\"",
            "\\"IN CUSTODY FCI - NO PRW\\"",
            "\\"PRE-REVOCATION - IN CUSTODY\\"",
            "\\"PRE-REVOCATION - SAFPF\\"",
             "\\"PRE-REVOCATION - ISF\\"") 
        AND status = "\\"NORMAL REPORT\\""
),
union_periods_cte AS (
    SELECT
        person_id,
        start_date,
        end_date,
        reason_for_contact,
    FROM status_change_periods

    UNION ALL

    SELECT
        person_id,
        start_date,
        end_date,
        reason_for_contact,
    FROM address_change_periods

    UNION ALL

    SELECT
        person_id,
        start_date,
        end_date,
        reason_for_contact,
    FROM office_visit
),
contact_count_cte AS (
    SELECT DISTINCT
        p.person_id,
        reason_for_contact,
        start_date,
        end_date,
        MAX(CASE 
            WHEN contact_method_raw_text = "HOME" 
                THEN 1 
            ELSE 0 
        END) AS contact_made_flag,
    FROM union_periods_cte p
    LEFT JOIN contact_info c 
        ON c.person_id = p.person_id
        AND DATE(c.contact_date) BETWEEN DATE(start_date) AND DATE(end_date)
    GROUP BY p.person_id, reason_for_contact, start_date, end_date
)
    SELECT
        person_id,
        start_date,
        end_date,
        end_date AS contact_due_date,
        reason_for_contact,
        (contact_made_flag = 1) AS meets_criteria,
        (contact_made_flag = 0 AND end_date < CURRENT_DATE("US/Eastern")) AS overdue_flag,
        TO_JSON(STRUCT((contact_made_flag = 1) AS compliance)) AS reason,
        "US_TX" AS state_code,
    FROM contact_count_cte
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        state_code=StateCode.US_TX,
        raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_TX, instance=DirectIngestInstance.PRIMARY
        ),
        normalized_state_dataset="us_tx_normalized_state",
        reasons_fields=[
            ReasonsField(
                name="contact_due_date",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Due date of the contact.",
            ),
            ReasonsField(
                name="reason_for_contact",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="The type of period.",
            ),
            ReasonsField(
                name="overdue_flag",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Flag that indicates whether contact was missed.",
            ),
            ReasonsField(
                name="meets_criteria",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Flag that indicates whether the the criteria was met.",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
