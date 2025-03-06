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
"""
Helper SQL queries for Texas
"""

from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)


def contact_compliance_builder(
    criteria_name: str, description: str, contact_type: str
) -> StateSpecificTaskCriteriaBigQueryViewBuilder:
    """Returns a state-specific criteria view builder indicating the spans of time when
    a person has on supervision has met the contact compliance cadence for a given
    contact type.

    Args:
        criteria_name (str): The name of the criteria
        description (str): The description of the criteria
        contact_type (int): The type of contact
    """

    criteria_query = f"""
WITH
-- Create periods of case type and supervision level information
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
-- Create contacts table by adding scheduled/unscheduled prefix
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
    WHERE state_code = "US_TX"
),
-- Create a table where each contact is associated with the appropriate cadence and we 
-- begin to count the months the client has been on supervision and the number of 
-- periods they'll have given the contact frequency
contacts_compliance AS (
        SELECT DISTINCT
        p.person_id,
        p.start_date,
        ca.contact_type,
        p.supervision_level,
        DATE_TRUNC(start_date, MONTH) AS month_start,
        p.case_type, 
        CAST(ca.frequency_in_months AS INT64) AS frequency_in_months,
        CAST(ca.quantity AS INT64) AS quantity,
        DATE_DIFF(COALESCE(end_date, CURRENT_DATE), start_date, MONTH) + 1 AS total_months,
        FLOOR ((DATE_DIFF(COALESCE(end_date, CURRENT_DATE), start_date, MONTH) + 1)/CAST(ca.frequency_in_months AS INT64)) as num_periods 
    FROM person_info_agg p
    LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_dataset}}.RECIDIVIZ_REFERENCE_ContactCadence_latest` ca
        ON "{contact_type}" = ca.contact_type
        AND p.case_type = ca.case_type
        AND p.supervision_level = ca.supervision_level
    -- Remove rows with no contact requirements
    WHERE ca.contact_type IS NOT NULL
),
-- Creates sets of empty periods that are the duration of the contact frequency
empty_periods as (
    SELECT 
        person_id,
        contact_type,
        supervision_level,
        case_type,
        quantity,
        frequency_in_months,
        -- For each span, calculate the starting month and ending month
        month_start + INTERVAL x * frequency_in_months MONTH AS month_start,
        -- Calculate the end of the span (last day of the month)
        LAST_DAY(month_start + INTERVAL (x + 1) * frequency_in_months - 1 MONTH) AS month_end
    FROM contacts_compliance,
    UNNEST(GENERATE_ARRAY(0, CAST(num_periods AS INT64))) AS x
),
-- Looks back to connect contacts to a given contact period they were completed in
lookback_cte AS
(
 SELECT
        p.person_id,
        p.case_type,
        p.contact_type,
        p.supervision_level,
        p.frequency_in_months,
        ci.contact_date,
        month_start,
        month_end,
        quantity,
    FROM empty_periods p
    LEFT JOIN contact_info ci
        ON p.person_id = ci.person_id
        AND ci.contact_type = p.contact_type
        AND ci.contact_date BETWEEN p.month_start and p.month_end 
),
-- Union all critical dates (start date, end date, contact dates)
critical_dates as (
    SELECT 
        person_id,
        month_start,
        month_end,
        contact_type,
        quantity,
        frequency_in_months,
        month_start as critical_date,
    FROM lookback_cte
    UNION ALL 
    SELECT 
        person_id,
        month_start,
        month_end,
        contact_type,
        quantity,
        frequency_in_months,
        month_end as critical_date,
    FROM lookback_cte
    UNION ALL
    SELECT 
        person_id,
        month_start,
        month_end,
        contact_type,
        quantity,
        frequency_in_months,
        contact_date as critical_date,
    FROM lookback_cte
    WHERE contact_date IS NOT NULL
), 
divided_periods as (
    SELECT
        month_start,
        month_end,
        person_id,
        contact_type,
        quantity,
        frequency_in_months,
        critical_date as period_start,
        LEAD (critical_date) OVER(PARTITION BY month_start,person_id ORDER BY critical_date)AS period_end,
    FROM (SELECT DISTINCT * FROM critical_dates)
),
divided_periods_with_contacts as (
    SELECT
        p.person_id,
        COUNT(ci.external_id) AS contact_count,
        period_start,
        period_end,
        month_start,
        p.contact_type,
        month_end,
        quantity,
        frequency_in_months,
    FROM divided_periods p
    LEFT JOIN contact_info ci
        ON p.person_id = ci.person_id
        AND ci.contact_date BETWEEN p.month_start and DATE_SUB(p.period_end, INTERVAL 1 DAY)
        AND ci.contact_type = "{contact_type}"
    WHERE period_end is not null
    GROUP BY p.person_id, month_start, month_end, period_start, period_end, contact_type, quantity, frequency_in_months
),
-- Creates periods of time in which a person is compliance given a contact and it's cadence
compliance_periods as (
    SELECT 
        person_id,
        contact_type,
        contact_count,
        period_start,
        period_end,
        month_start,
        month_end,
        quantity,
        frequency_in_months,
    FROM divided_periods_with_contacts
),
-- Creates final periods of compliance
periods AS (
    SELECT 
        lc.person_id,
        "US_TX" as state_code,
        lc.contact_type as type_of_contact,
        contact_count >= quantity AS meets_criteria,
        contact_count < quantity AND CURRENT_DATE > month_end AS overdue_flag,
        CASE
            WHEN period_start = month_start
                THEN "start"
            WHEN period_end = month_end
                THEN "end"
            ELSE "contact"
        END AS period_type,
        month_end AS contact_due_date,
        contact_count,
        period_start as start_date,
        period_end as end_date,
        max(contact_date) as last_contact_date,
        TO_JSON(STRUCT(contact_count >= quantity AS compliance)) AS reason,
        CASE 
            WHEN frequency_in_months = 1 
                THEN "1 MONTH"
            ELSE
                CONCAT(frequency_in_months, " MONTHS") 
        END AS frequency,
    FROM compliance_periods lc
    LEFT JOIN contact_info ci
        ON lc.person_id = ci.person_id
        AND ci.contact_type = lc.contact_type
        AND ci.contact_date < period_end
    GROUP BY person_id,month_start,month_end,lc.contact_type,contact_count,quantity, period_start, period_end, frequency_in_months
)
SELECT 
  *
FROM periods
"""

    return StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        description=description,
        state_code=StateCode.US_TX,
        criteria_spans_query_template=criteria_query,
        raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_TX, instance=DirectIngestInstance.PRIMARY
        ),
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
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
                name="type_of_contact",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Type of contact due.",
            ),
            ReasonsField(
                name="overdue_flag",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Flag that indicates whether contact was missed.",
            ),
            ReasonsField(
                name="contact_count",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Number of contacts done within the overall period.",
            ),
            ReasonsField(
                name="period_type",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Type of period.",
            ),
            ReasonsField(
                name="frequency",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Contact cadence.",
            ),
        ],
    )
