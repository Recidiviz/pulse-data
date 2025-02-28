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
    WHERE state_code = "US_TX"
        -- Make sure that we have enough info to create span
        AND correctional_level_start IS NOT NULL 
        AND case_type_start IS NOT NULL
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
-- Create list of contact types and amounts required, this is only used to add to the 
-- reasons JSON column at the end so we are able to surface how many contacts are 
-- required at the start of each contact period.
contact_required AS (
  SELECT
        supervision_level,
        case_type,
        CONCAT(
            CASE 
                WHEN CAST(SCHEDULED_HOME_REQ AS INT64) != 0 
                    THEN CONCAT("SCHEDULED HOME: ", SCHEDULED_HOME_REQ, " ")
                ELSE ""
            END,
            CASE
                WHEN CAST(SCHEDULED_FIELD_REQ AS INT64) != 0 
                    THEN CONCAT("SCHEDULED FIELD: ", SCHEDULED_FIELD_REQ, " ")
                ELSE ""
            END,
            CASE
                WHEN CAST(UNSCHEDULED_FIELD_REQ AS INT64) != 0 
                    THEN CONCAT("UNSCHEDULED FIELD: ", UNSCHEDULED_FIELD_REQ, " ")
                ELSE ""
            END,
            CASE
                WHEN CAST(UNSCHEDULED_HOME_REQ AS INT64) != 0 
                    THEN CONCAT("UNSCHEDULED HOME: ", UNSCHEDULED_HOME_REQ, " ")
                ELSE ""
            END,
            CASE
                WHEN CAST(SCHEDULED_ELECTRONIC_REQ AS INT64) != 0 
                    THEN CONCAT("SCHEDULED ELECTRONIC: ", SCHEDULED_ELECTRONIC_REQ, " ")
                ELSE ""
            END,
            CASE
                WHEN CAST(SCHEDULED_OFFICE_REQ AS INT64) != 0 
                    THEN CONCAT("SCHEDULED OFFICE: ", SCHEDULED_OFFICE_REQ, " ")
                ELSE ""
            END
        ) AS types_and_amounts_due,
        frequency_in_months,
    FROM `{{project_id}}.{{raw_data_up_to_date_dataset}}.RECIDIVIZ_REFERENCE_ContactCadenceAgnostic_latest`
),
-- Creates table of all contacts and adds scheduled/unscheculed prefix
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
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_contact` 
),
-- Creates a record of what contact types can be accepted for a given supervision period
-- and calculates how long the person has been in supervision, and thus how many contact
-- periods should have occured. 
person_info_with_contact_types_accepted AS (
    SELECT 
        pia.supervision_level,
        pia.case_type,
        pia.person_id,
        contact_types_accepted,
        pia.start_date,
        pia.end_date,
        DATE_TRUNC(start_date, MONTH) AS month_start,
        CAST(frequency_in_months AS INT64) AS frequency_in_months,
        DATE_DIFF(COALESCE(end_date, CURRENT_DATE), start_date, MONTH) + 1 AS total_months,
        FLOOR ((DATE_DIFF(COALESCE(end_date, CURRENT_DATE), start_date, MONTH) + 1)/CAST(cca.frequency_in_months AS INT64)) as num_periods 
    FROM person_info_agg pia
    LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_dataset}}.RECIDIVIZ_REFERENCE_ContactCadenceAgnostic_latest` cca
        ON cca.supervision_level = pia.supervision_level 
        AND  cca.case_type = pia.case_type
    -- Check to see if this supervision level and case type has any agnostic contacts
    WHERE cca.frequency_in_months IS NOT NULL
),
-- Creates sets of empty periods that are the duration of the contact frequency
empty_periods AS (
    Select 
        person_id,
        contact_types_accepted,
        supervision_level,
        case_type,
        -- For each span, calculate the starting month and ending month
        month_start + INTERVAL period_index * frequency_in_months MONTH AS month_start,
        -- Calculate the end of the span (last day of the month)
        LAST_DAY(month_start + INTERVAL (period_index + 1) * frequency_in_months - 1 MONTH) AS month_end
    from person_info_with_contact_types_accepted,
    UNNEST(GENERATE_ARRAY(0, CAST(num_periods AS INT64))) AS period_index
),
-- Looks back to connect contacts to a given contact period they were completed in
lookback_cte AS
(
 SELECT
        p.person_id,
        p.case_type,
        p.contact_types_accepted,
        p.supervision_level,
        ci.contact_date,
        month_start,
        month_end,
        ci.contact_type,
    FROM empty_periods p
    LEFT JOIN contact_info ci
        ON p.person_id = ci.person_id
        AND p.contact_types_accepted LIKE CONCAT("%",ci.contact_type,"%") 
        AND ci.contact_date BETWEEN p.month_start and p.month_end 
),
-- Union all critical dates (start date, end date, contact dates)
critical_dates as (
   SELECT 
        person_id,
        contact_types_accepted,
        month_start,
        month_end,
        null as contact_type,
        month_start as critical_date,
        case_type,
        supervision_level,
        "START" as period_type,
    FROM lookback_cte
    UNION ALL 
    SELECT 
        person_id,
        contact_types_accepted,
        month_start,
        month_end,
        null as contact_type,
        month_end as critical_date,
        case_type,
        supervision_level,
        "END" as period_type,
    FROM lookback_cte
    UNION ALL
    SELECT 
        person_id,
        contact_types_accepted,
        month_start,
        month_end,
        contact_type,
        contact_date as critical_date,
        case_type,
        supervision_level,
        "CONTACT" as period_type,
    FROM lookback_cte
    WHERE contact_date IS NOT NULL
), 
-- Creates smaller periods divided by contact dates.
divided_periods AS (
    SELECT
        month_start,
        month_end,
        person_id,
        contact_type,
        contact_types_accepted,
        critical_date as period_start,
        LEAD (critical_date) OVER(PARTITION BY month_start,person_id ORDER BY critical_date)AS period_end,
        case_type,
        supervision_level,
        period_type,
    FROM (SELECT DISTINCT * FROM critical_dates)
),
-- Divided periods with the associated contacts connected
divided_periods_with_contacts as (
    SELECT
        p.person_id,
        period_start,
        period_end,
        month_start,
        p.contact_type,
        month_end,
        case_type,
        supervision_level,
        contact_types_accepted,
        period_type,
    FROM divided_periods p
    LEFT JOIN contact_info ci
        ON p.person_id = ci.person_id
        AND ci.contact_date BETWEEN p.month_start and p.period_end 
        AND p.contact_types_accepted LIKE CONCAT("%",ci.contact_type,"%") 
    WHERE period_end IS NOT NULL
),
-- CTE that counts the contacts by type up to a certain date
contact_count AS (
    SELECT
        *,
        SUM (case when contact_type = "SCHEDULED HOME" THEN 1 ELSE 0 END) OVER (PARTITION BY person_id, month_start ORDER BY period_start asc) as scheduled_home_count,
        SUM (case when contact_type = "SCHEDULED OFFICE" THEN 1 ELSE 0 END) OVER (PARTITION BY person_id, month_start ORDER BY period_start asc) as scheduled_office_count,
        SUM (case when contact_type = "SCHEDULED FIELD" THEN 1 ELSE 0 END) OVER (PARTITION BY person_id, month_start ORDER BY period_start asc) as scheduled_field_count,
        SUM (case when contact_type = "UNSCHEDULED HOME" THEN 1 ELSE 0 END) OVER (PARTITION BY person_id, month_start ORDER BY period_start asc) as unscheduled_home_count,
        SUM (case when contact_type = "SCHEDULED ELECTRONIC" THEN 1 ELSE 0 END) OVER (PARTITION BY person_id, month_start ORDER BY period_start asc) as scheduled_electronic_count,
        SUM (case when contact_type = "UNSCHEDULED FIELD" THEN 1 ELSE 0 END) OVER (PARTITION BY person_id, month_start ORDER BY period_start asc) as unscheduled_field_count,
    FROM divided_periods_with_contacts
),
-- Check for compliance based on contact standards for that given supervision level and case type
compliance_check AS (
    SELECT 
        "US_TX" AS state_code,
        person_id,
        period_start as start_date,
        period_end as end_date,
        month_start,
        month_end as contact_due_date,
        CASE
            WHEN CAST(SCHEDULED_HOME_REQ AS INT64) <= scheduled_home_count AND CAST(SCHEDULED_HOME_REQ AS INT64) != 0
                THEN TRUE
            WHEN CAST(SCHEDULED_FIELD_REQ AS INT64) <= scheduled_field_count AND CAST(SCHEDULED_FIELD_REQ AS INT64) != 0
                THEN TRUE
            WHEN CAST(UNSCHEDULED_FIELD_REQ AS INT64) <= unscheduled_field_count AND CAST(UNSCHEDULED_FIELD_REQ AS INT64) != 0 
                THEN TRUE
            WHEN CAST(UNSCHEDULED_HOME_REQ AS INT64) <= unscheduled_home_count AND CAST(UNSCHEDULED_HOME_REQ AS INT64) != 0
                THEN TRUE
            WHEN CAST(SCHEDULED_ELECTRONIC_REQ AS INT64) <= scheduled_electronic_count AND CAST(SCHEDULED_ELECTRONIC_REQ AS INT64) != 0 
                THEN TRUE
            WHEN CAST(SCHEDULED_OFFICE_REQ AS INT64) <= scheduled_office_count AND CAST(SCHEDULED_OFFICE_REQ AS INT64) != 0  
                THEN TRUE
            ELSE FALSE
        END AS meets_criteria,
        TO_JSON(STRUCT(
            CONCAT("SCHEDULED HOME: ", scheduled_home_count, " ") AS scheduled_home_done,
            CONCAT("SCHEDULED FIELD: ", scheduled_field_count, " ") AS scheduled_field_done,
            CONCAT("UNSCHEDULED FIELD: ", unscheduled_field_count, " ") AS unscheduled_field_done,
            CONCAT("UNSCHEDULED HOME: ", unscheduled_home_count, " ") AS unscheduled_home_done,
            CONCAT("SCHEDULED ELECTRONIC: ", scheduled_electronic_count, " ") AS scheduled_electronic_done,
            CONCAT("SCHEDULED OFFICE: ", scheduled_office_count, " ") AS scheduled_office_done
        )) AS types_and_amounts_done,
        types_and_amounts_due,
        period_type,
    FROM contact_count cc
        LEFT JOIN contact_required
        USING (supervision_level, case_type)
    LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_dataset}}.RECIDIVIZ_REFERENCE_ContactCadenceAgnostic_latest` caa
         USING(supervision_level, case_type)
),
-- Finalize periods
finalized_periods AS (
    SELECT
        cc.person_id,
        state_code,
        start_date,
        end_date,
        meets_criteria,
        TO_JSON(STRUCT(meets_criteria AS compliance)) AS reason,
        contact_due_date,
        types_and_amounts_done,
        types_and_amounts_due,
        period_type,
        MAX(contact_date) OVER (PARTITION BY cc.person_id,types_and_amounts_due) as last_contact_date,
        CASE WHEN
            meets_criteria IS FALSE AND CURRENT_DATE > end_date
            THEN TRUE
            ELSE FALSE
        END AS overdue_flag,
    FROM compliance_check cc
    LEFT JOIN contact_info ci
      ON cc.person_id = ci.person_id
        AND cc.types_and_amounts_due LIKE CONCAT("%",ci.contact_type,"%") 
        AND ci.contact_date < end_date
)
SELECT 
  *
FROM finalized_periods
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
            ReasonsField(
                name="types_and_amounts_done",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="The type and amount due.",
            ),
            ReasonsField(
                name="period_type",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="The type of period.",
            ),
            ReasonsField(
                name="overdue_flag",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Flag that indicates whether contact was missed.",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
