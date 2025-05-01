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

from typing import Optional

from google.cloud import bigquery

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)


def contact_compliance_builder(
    criteria_name: str,
    description: str,
    contact_type: str,
    custom_contact_cadence_spans: Optional[str] = None,
) -> StateSpecificTaskCriteriaBigQueryViewBuilder:
    """Returns a state-specific criteria view builder indicating the spans of time when
    a person has on supervision has not met the contact compliance cadence for a given
    contact type.

    Args:
        criteria_name (str): The name of the criteria
        description (str): The description of the criteria
        contact_type (int): The type of contact
        custom_contact_cadence_spans (str): The custom contact cadence spans query.
            Must contain date fields for `month_start` and `month_end`, indicating
            the start and end of the period of time over which a contact standard
            is applicable to the person. If not provided, the default query will
            pull from the `contact_cadence_spans_materialized` table.
    """
    if not custom_contact_cadence_spans:
        custom_contact_cadence_spans = "SELECT * FROM `{project_id}.analyst_data.us_tx_contact_cadence_spans_materialized`"

    criteria_query = f"""
WITH
-- Create contacts table by adding scheduled/unscheduled prefix
contact_info AS (
    SELECT 
        person_id,
        contact_date,
        CASE
            -- When we are looking at collateral contacts, look at contact type in 
            -- state supervision contact as opposed to contact method
            WHEN "{contact_type}" = "SCHEDULED COLLATERAL" 
            AND contact_type IN ("COLLATERAL", "BOTH_COLLATERAL_AND_DIRECT")
                THEN "SCHEDULED COLLATERAL"
            -- Consider all contacts with contact_method "Telephone" and "Virtual" as
            -- Electronic Contacts.
            WHEN contact_method in ("TELEPHONE", "VIRTUAL")
            AND contact_type IN ("DIRECT", "BOTH_COLLATERAL_AND_DIRECT")
                THEN ("SCHEDULED ELECTRONIC")
            -- When the contact reason has unscheduled in raw text, label as unscheduled
            WHEN contact_reason_raw_text LIKE "%UNSCHEDULED%"
                THEN CONCAT("UNSCHEDULED " || contact_method_raw_text)
            ELSE
                CONCAT("SCHEDULED " || contact_method_raw_text)
        END AS contact_type,
        external_id,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_contact` 
    WHERE state_code = "US_TX" AND status = "COMPLETED"
),
contact_cadence_spans AS (
    {custom_contact_cadence_spans}
)
,
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
    FROM contact_cadence_spans p
    LEFT JOIN contact_info ci
        ON p.person_id = ci.person_id
        AND ci.contact_type = p.contact_type
        AND ci.contact_date BETWEEN p.month_start and p.month_end 
    WHERE p.contact_type = "{contact_type}"
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
    UNION DISTINCT 
    SELECT 
        person_id,
        month_start,
        month_end,
        contact_type,
        quantity,
        frequency_in_months,
        month_end as critical_date,
    FROM lookback_cte
    UNION DISTINCT
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
        LEAD (critical_date) OVER (PARTITION BY month_start, person_id ORDER BY critical_date) AS period_end,
    FROM critical_dates
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
        contact_count < quantity AS meets_criteria,
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
    GROUP BY person_id, month_start, month_end, lc.contact_type, contact_count, quantity, period_start, period_end, frequency_in_months
)
SELECT 
  *,
  TO_JSON(STRUCT(
    last_contact_date,
    contact_due_date,
    type_of_contact,
    overdue_flag,
    contact_count,
    period_type,
    frequency
  )) AS reason,
FROM periods
"""

    return StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        description=description,
        state_code=StateCode.US_TX,
        criteria_spans_query_template=criteria_query,
        normalized_state_dataset="us_tx_normalized_state",
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
