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

from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    create_intersection_spans,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.general_criteria_builders import (
    get_reason_json_fields_query_template_for_criteria,
)
from recidiviz.utils.string_formatting import fix_indent


def contact_compliance_builder_critical_understaffing_monthly_virtual_override(
    description: str,
    base_criteria: StateSpecificTaskCriteriaBigQueryViewBuilder,
    contact_type: str,
) -> StateSpecificTaskCriteriaBigQueryViewBuilder:
    """
    Returns a state-specific criteria view builder indicating the spans of time when
    a person has on supervision has not met the contact compliance cadence for a given
    contact type, applying an override for alternating monthly virtual contacts
    based on critical understaffing policy, which schedules virtual contacts
    based on the month and the last name of the client.

    Args:
        description (str): The description of the criteria
        base_criteria (StateSpecificTaskCriteriaBigQueryViewBuilder): The criteria that represents the standard policy on which to apply overrides
        contact_type (int): The type of contact
    """

    standard_policy_criteria_name = base_criteria.view_id

    criteria_query = f"""
WITH has_monthly_home_contact_requirement AS (
    SELECT * FROM `{{project_id}}.tasks_views.us_tx_contact_cadence_spans_materialized`
    WHERE frequency = 1
        AND frequency_date_part = 'MONTH'
        AND contact_type = "{contact_type}"
)
,
-- Generate a monthly date array spanning all of the time over which a person may
-- have had monthly contact requirements.
date_range AS (
    SELECT
        person_id,
        state_code,
        DATE_TRUNC(MIN(start_date), MONTH) AS min_date,
        {revert_nonnull_end_date_clause(f"MAX({nonnull_end_date_clause('end_date')})")} AS max_date,
    FROM
        has_monthly_home_contact_requirement
    GROUP BY 1, 2
)
,
-- Create monthly spans for each person_id, and assign an override contact type based on
-- the first letter of the last name and the month of the contact.
special_monthly_contact_cadence AS (
    SELECT
        date_range.person_id,
        date_range.state_code,
        contact_month_start_date,
        DATE_ADD(contact_month_start_date, INTERVAL 1 MONTH) AS contact_month_end_date,
        CASE
            WHEN
            -- If the first letter of the last name is A-M and it's an "even" month, 
            -- or the first letter of the last name is N-Z and it's an "odd" month,
            -- then the contact should be a virtual contact. Otherwise, the contact
            -- is a standard home contact and there is no override.
                (
                    LEFT(JSON_EXTRACT_SCALAR(person.full_name, "$.surname"), 1) < "N"
                    AND MOD(EXTRACT(MONTH FROM contact_month_start_date), 2) = 0
                )
                OR (
                    LEFT(JSON_EXTRACT_SCALAR(person.full_name, "$.surname"), 1) >= "N"
                    AND MOD(EXTRACT(MONTH FROM contact_month_start_date), 2) = 1
                )
            THEN "{contact_type} (VIRTUAL)"
            ELSE NULL
        END AS override_contact_type,
    FROM
        date_range,
        UNNEST(GENERATE_DATE_ARRAY(
            min_date,
            IFNULL(max_date, CURRENT_DATE('US/Eastern')),
            INTERVAL 1 MONTH
        )) AS contact_month_start_date
    INNER JOIN
        `{{project_id}}.normalized_state.state_person` person
    USING (person_id)
)
,
needs_contact AS (
    SELECT
        *
    FROM
        `{{project_id}}.task_eligibility_criteria_us_tx.{standard_policy_criteria_name}_materialized`
    WHERE
        JSON_EXTRACT_SCALAR(reason_v2, "$.contact_cadence") = "1 EVERY MONTH"
)
,
-- Intersect the compliant month ranges with the original monthly contact
-- cadence spans to get the final spans of time where a client is due for
-- a monthly contact under the critical understaffing policy,
-- along with the value of the override contact type.
intersection_spans AS (
    {create_intersection_spans(
        table_1_name="needs_contact",
        table_2_name="special_monthly_contact_cadence",
        index_columns=["state_code", "person_id"],
        table_1_columns=["meets_criteria", "reason_v2"],
        table_2_columns=["override_contact_type"],
        table_1_start_date_field_name="start_date",
        table_1_end_date_field_name="end_date",
        table_2_start_date_field_name="contact_month_start_date",
        table_2_end_date_field_name="contact_month_end_date"
    )}
)
,
-- Further intersect these spans with critical understaffing spans, so that we only apply
-- the override contact type to spans that are in the critical understaffing location.
critical_understaffing_spans AS (
    SELECT
        person_id,
        state_code,
        start_date,
        end_date,
        meets_criteria AS officer_in_critically_understaffed_location,
    FROM
        `{{project_id}}.task_eligibility_criteria_us_tx.supervision_officer_in_critically_understaffed_location_materialized`
    WHERE
        state_code = "US_TX"
)
,
-- If a client has monthly contact requirement but not in critically understaffed policy, 
-- they'd be covered by a different criteria, so we can ignore them here by just taking
-- spans of time when someone is both critically understaffed and requiring monthly
-- home contact.
intersection_spans_with_critical_understaffing AS (
    {create_intersection_spans(
        table_1_name="intersection_spans",
        table_2_name="critical_understaffing_spans",
        index_columns=["state_code", "person_id"],
        table_1_columns=["meets_criteria", "reason_v2", "override_contact_type"],
        table_2_columns=["officer_in_critically_understaffed_location"],
        table_1_start_date_field_name="start_date",
        table_1_end_date_field_name="end_date_exclusive",
        table_2_start_date_field_name="start_date",
        table_2_end_date_field_name="end_date"
    )}
)
SELECT
    person_id,
    state_code,
    start_date,
    end_date_exclusive AS end_date,
    -- meets_criteria still reflects the standard home contacts policy.
    meets_criteria,
    TO_JSON(STRUCT(
{fix_indent(
            get_reason_json_fields_query_template_for_criteria(base_criteria),
            indent_level = 8
        )},
        override_contact_type,
        officer_in_critically_understaffed_location
    )) AS reason,
{fix_indent(
        get_reason_json_fields_query_template_for_criteria(base_criteria),
        indent_level = 4
    )},
    override_contact_type,
    officer_in_critically_understaffed_location,
FROM intersection_spans_with_critical_understaffing
"""
    criteria_name = f"US_TX_NEEDS_{contact_type.replace(' ', '_')}_CONTACT_MONTHLY_CRITICAL_UNDERSTAFFING"
    return StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        description=description,
        state_code=StateCode.US_TX,
        criteria_spans_query_template=criteria_query,
        reasons_fields=[
            ReasonsField(
                name="last_contact_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date of the last contact.",
            ),
            ReasonsField(
                name="contact_due_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
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
                name="contact_cadence",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Contact cadence.",
            ),
            ReasonsField(
                name="override_contact_type",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Override contact type based on special policy.",
            ),
            ReasonsField(
                name="officer_in_critically_understaffed_location",
                type=bigquery.enums.StandardSqlTypeNames.BOOL,
                description="Boolean indicating whether client is assigned to an officer in a critically understaffed location",
            ),
        ],
    )


def contact_compliance_builder_type_agnostic(
    criteria_name: str, description: str, where_clause: str
) -> StateSpecificTaskCriteriaBigQueryViewBuilder:
    """Returns a state-specific criteria view builder indicating the spans of time when
    a person has on supervision has not met the contact compliance cadence for a given
    contact type.

    Args:
        criteria_name (str): The name of the criteria
        description (str): The description of the criteria
        where_clause (str): What type-agnostic contacts to filter down to
    """

    _QUERY_TEMPLATE = f"""
    WITH
    -- Create list of contact types and amounts required, this is only used to add to the 
    -- reasons JSON column at the end so we are able to surface how many contacts are 
    -- required at the start of each contact period.

    -- Creates table of all contacts and adds scheduled/unscheduled prefix
    contact_info AS (
        SELECT
            person_id,
            contact_external_id,
            contact_date,
            contact_type,
        FROM
            `{{project_id}}.tasks_views.us_tx_contact_events_preprocessed_materialized` 
        WHERE
            status = "COMPLETED"
    ),
    -- Looks back to connect contacts to a given contact period they were completed in
    lookback_cte AS
    (
     SELECT
            p.person_id,
            p.case_type,
            p.frequency,
            p.frequency_date_part,
            p.contact_types_accepted,
            p.supervision_level,
            ci.contact_date,
            CAST(contact_period_start AS DATE) AS contact_period_start,
            CAST(p.contact_period_end AS DATE) AS contact_period_end,
            DATE_ADD(CAST(p.contact_period_end AS DATE), INTERVAL 1 DAY) as contact_period_end_exclusive,
            ci.contact_type,
        FROM `{{project_id}}.tasks_views.us_tx_contact_cadence_spans_type_agnostic_materialized` p
        LEFT JOIN contact_info ci
            ON p.person_id = ci.person_id
            AND ci.contact_type IN UNNEST(SPLIT(p.contact_types_accepted, ','))
            AND ci.contact_date >= p.contact_period_start
            AND ci.contact_date < DATE_ADD(CAST(p.contact_period_end AS DATE), INTERVAL 1 DAY)
    {where_clause}
    ),
    -- Union all critical dates (start date, end date, contact dates)
    critical_dates as (
       SELECT 
            person_id,
            contact_types_accepted,
            contact_period_start,
            contact_period_end_exclusive,
            contact_period_start as critical_date,
            case_type,
            supervision_level,
            frequency,
            frequency_date_part,
        FROM lookback_cte
        UNION DISTINCT 
        SELECT 
            person_id,
            contact_types_accepted,
            contact_period_start,
            contact_period_end_exclusive,
            contact_period_end_exclusive as critical_date,
            case_type,
            supervision_level,
            frequency,
            frequency_date_part,
        FROM lookback_cte
        UNION DISTINCT
        SELECT 
            person_id,
            contact_types_accepted,
            contact_period_start,
            contact_period_end_exclusive,
            contact_date as critical_date,
            case_type,
            supervision_level,
            frequency,
            frequency_date_part,
        FROM lookback_cte
        WHERE contact_date IS NOT NULL
    ), 
    -- Creates smaller periods divided by contact dates.
    divided_periods AS (
        SELECT
            contact_period_start,
            contact_period_end_exclusive,
            person_id,
            contact_types_accepted,
            critical_date as period_start,
            LEAD (critical_date) OVER(PARTITION BY contact_period_start,person_id ORDER BY critical_date) AS period_end,
            case_type,
            supervision_level,
            frequency,
            frequency_date_part,
        FROM critical_dates
    ),
    -- Divided periods with the associated contacts connected
    divided_periods_with_contacts as (
        SELECT DISTINCT
            p.person_id,
            period_start,
            period_end,
            contact_period_start,
            ci.contact_type,
            contact_period_end_exclusive,
            case_type,
            supervision_level,
            contact_types_accepted,
            frequency,
            frequency_date_part,
        FROM divided_periods p
        LEFT JOIN contact_info ci
            ON p.person_id = ci.person_id
            AND ci.contact_date >= p.contact_period_start
            AND ci.contact_date < p.period_end
            AND ci.contact_type IN UNNEST(SPLIT(p.contact_types_accepted, ','))
        WHERE period_end IS NOT NULL
    ),
    -- CTE that counts the contacts by type up to a certain date
    contact_count AS (
        SELECT
            *,
            SUM (case when contact_type = "SCHEDULED HOME" THEN 1 ELSE 0 END) OVER (PARTITION BY person_id, contact_period_start ORDER BY period_start asc) as scheduled_home_count,
            SUM (case when contact_type = "SCHEDULED OFFICE" THEN 1 ELSE 0 END) OVER (PARTITION BY person_id, contact_period_start ORDER BY period_start asc) as scheduled_office_count,
            SUM (case when contact_type = "SCHEDULED FIELD" THEN 1 ELSE 0 END) OVER (PARTITION BY person_id, contact_period_start ORDER BY period_start asc) as scheduled_field_count,
            SUM (case when contact_type = "UNSCHEDULED HOME" THEN 1 ELSE 0 END) OVER (PARTITION BY person_id, contact_period_start ORDER BY period_start asc) as unscheduled_home_count,
            SUM (case when contact_type = "SCHEDULED VIRTUAL OFFICE" THEN 1 ELSE 0 END) OVER (PARTITION BY person_id, contact_period_start ORDER BY period_start asc) as scheduled_virtual_office_count,
            SUM (case when contact_type = "UNSCHEDULED FIELD" THEN 1 ELSE 0 END) OVER (PARTITION BY person_id, contact_period_start ORDER BY period_start asc) as unscheduled_field_count,
        FROM divided_periods_with_contacts
    ),
    types_and_amounts_due_cte AS (
      SELECT
            * EXCEPT(frequency, frequency_date_part),
            TO_JSON(STRUCT(
              IF(CAST(SCHEDULED_HOME_REQ AS INT64) != 0, SCHEDULED_HOME_REQ, NULL) AS scheduled_home_due,
              IF(CAST(SCHEDULED_FIELD_REQ AS INT64) != 0, SCHEDULED_FIELD_REQ, NULL) AS scheduled_field_due,
              IF(CAST(UNSCHEDULED_FIELD_REQ AS INT64) != 0, UNSCHEDULED_FIELD_REQ, NULL) AS unscheduled_field_due,
              IF(CAST(UNSCHEDULED_HOME_REQ AS INT64) != 0, UNSCHEDULED_HOME_REQ, NULL) AS unscheduled_home_due,
              IF(CAST(SCHEDULED_VIRTUAL_OFFICE_REQ AS INT64) != 0, SCHEDULED_VIRTUAL_OFFICE_REQ, NULL) AS scheduled_virtual_office_due,
              IF(CAST(SCHEDULED_OFFICE_REQ AS INT64) != 0, SCHEDULED_OFFICE_REQ, NULL) AS scheduled_office_due
            )) AS types_and_amounts_due
        FROM `{{project_id}}.static_reference_data_views.us_tx_contact_standards_type_agnostic_materialized`
    ),
    -- Check for compliance based on contact standards for that given supervision level and case type
    compliance_check AS (
        SELECT 
            "US_TX" AS state_code,
            person_id,
            period_start as start_date,
            period_end as end_date,
            contact_period_start,
            DATE_SUB(contact_period_end_exclusive, INTERVAL 1 DAY) as contact_due_date,
            supervision_level,
            case_type,
            CASE
                WHEN CAST(SCHEDULED_HOME_REQ AS INT64) <= scheduled_home_count AND CAST(SCHEDULED_HOME_REQ AS INT64) != 0
                    THEN FALSE
                WHEN CAST(SCHEDULED_FIELD_REQ AS INT64) <= scheduled_field_count AND CAST(SCHEDULED_FIELD_REQ AS INT64) != 0
                    THEN FALSE
                WHEN CAST(UNSCHEDULED_FIELD_REQ AS INT64) <= unscheduled_field_count AND CAST(UNSCHEDULED_FIELD_REQ AS INT64) != 0 
                    THEN FALSE
                WHEN CAST(UNSCHEDULED_HOME_REQ AS INT64) <= unscheduled_home_count AND CAST(UNSCHEDULED_HOME_REQ AS INT64) != 0
                    THEN FALSE
                WHEN CAST(SCHEDULED_VIRTUAL_OFFICE_REQ AS INT64) <= scheduled_virtual_office_count AND CAST(SCHEDULED_VIRTUAL_OFFICE_REQ AS INT64) != 0 
                    THEN FALSE
                WHEN CAST(SCHEDULED_OFFICE_REQ AS INT64) <= scheduled_office_count AND CAST(SCHEDULED_OFFICE_REQ AS INT64) != 0  
                    THEN FALSE
                ELSE TRUE
            END AS meets_criteria,
            TO_JSON(STRUCT(
                scheduled_home_count AS scheduled_home_done,
                scheduled_field_count AS scheduled_field_done,
                unscheduled_field_count AS unscheduled_field_done,
                unscheduled_home_count AS unscheduled_home_done,
                scheduled_virtual_office_count AS scheduled_virtual_office_done,
                scheduled_office_count AS scheduled_office_done
            )) AS types_and_amounts_done,
            types_and_amounts_due,
            contact_types_accepted,
            CASE 
                WHEN period_start = contact_period_start
                    THEN "START"
                WHEN period_end =  contact_period_end_exclusive
                    THEN "END"
                ELSE
                 "CONTACT"
            END AS period_type,
            CASE 
                WHEN frequency = 1 AND frequency_date_part = "MONTH"
                    THEN "1 EVERY MONTH"
                WHEN frequency = 1 AND frequency_date_part = "WEEK"
                    THEN "1 EVERY WEEK"
                WHEN frequency = 1 AND frequency_date_part = "DAY"
                    THEN "1 EVERY DAY"
                ELSE
                    CONCAT("1 EVERY ", frequency, " ", frequency_date_part, "S") 
            END AS contact_cadence,
            frequency,
            frequency_date_part
        FROM contact_count cc
        LEFT JOIN types_and_amounts_due_cte
            USING (supervision_level, case_type, contact_types_accepted)

    ),
    -- Finalize periods
    finalized_periods AS (
        SELECT
            cc.person_id,
            state_code,
            start_date,
            end_date,
            meets_criteria,
            contact_due_date,
            types_and_amounts_done,
            types_and_amounts_due,
            RTRIM(contact_types_accepted,",") as contact_types_accepted,
            period_type,
            ci.contact_date AS last_contact_date,
            CASE WHEN
                meets_criteria IS TRUE AND CURRENT_DATE > end_date
                THEN TRUE
                ELSE FALSE
            END AS overdue_flag,
            frequency,
            frequency_date_part,
            contact_cadence,
            supervision_level,
            case_type
        FROM compliance_check cc
        LEFT JOIN contact_info ci
          ON cc.person_id = ci.person_id
            AND ci.contact_type IN UNNEST(SPLIT(contact_types_accepted, ','))
            AND ci.contact_date < start_date
        QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id, contact_types_accepted, start_date ORDER BY contact_date DESC) = 1
    )
    SELECT 
      *,
      TO_JSON(STRUCT(
        last_contact_date,
        contact_due_date,
        types_and_amounts_due,
        types_and_amounts_done,
        period_type,
        overdue_flag,
        frequency,
        contact_cadence,
        contact_types_accepted,
        supervision_level,
        case_type
      )) AS reason,
    FROM finalized_periods
    """

    return StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        description=description,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        state_code=StateCode.US_TX,
        reasons_fields=[
            ReasonsField(
                name="last_contact_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date of the last contact.",
            ),
            ReasonsField(
                name="contact_due_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
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
            ReasonsField(
                name="frequency",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Frequency of contact requirement, paired with frequency_date_part. Backwards compatible for TX.",
            ),
            ReasonsField(
                name="frequency_date_part",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Date part for frequency contact requirement. Backwards compatible for TX.",
            ),
            ReasonsField(
                name="contact_types_accepted",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Types of contacts that satisfy this criteria",
            ),
            ReasonsField(
                name="supervision_level",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="The supervision level that determines these contact standards",
            ),
            ReasonsField(
                name="case_type",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="The case type that determines these contact standards",
            ),
            ReasonsField(
                name="contact_cadence",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Contact cadence requirement.",
            ),
        ],
    )
