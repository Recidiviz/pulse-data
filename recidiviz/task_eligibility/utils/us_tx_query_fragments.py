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

from typing import Sequence

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import (
    list_to_query_string,
    nonnull_end_date_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_intersection_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.views.tasks.contact_type import ContactType
from recidiviz.calculator.query.state.views.tasks.tasks_criteria_utils import (
    truncate_to_calendar_period,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
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

# Some US_TX populations are encoded as a supervision_level, not a case_type. Maps those
# levels to the case type TES criteria should treat them as. RRC residents ingest as
# RESIDENTIAL_PROGRAM (since #77278); RESIDENTIAL_REENTRY has no StateSupervisionCaseType
# member, so it stays a string.
RESIDENTIAL_REENTRY_CASE_TYPE = "RESIDENTIAL_REENTRY"
SUPERVISION_LEVEL_TO_CASE_TYPE_OVERRIDE: dict[StateSupervisionLevel, str] = {
    StateSupervisionLevel.RESIDENTIAL_PROGRAM: RESIDENTIAL_REENTRY_CASE_TYPE,
}

# Substance abuse program IDs that count as treatment programs requiring an
# additional collateral contact for RRC clients (per the contact policy
# encoded in US_TX_NEEDS_SCHEDULED_COLLATERAL_CONTACT). DWI, SACP, and
# Therapeutic Community programs are included; AA/NA/peer support are
# intentionally excluded.
US_TX_SUBSTANCE_ABUSE_PROGRAM_IDS_REQUIRING_COLLATERAL: list[str] = [
    "DWI REPEAT OFFENDER PROGRAM",
    "SUBSTANCE ABUSE - SACP - LEVEL 2 - SUPPORTIVE OUTPATIENT",
    "SUBSTANCE ABUSE - SACP - RESIDENTIAL",
    "SUBSTANCE ABUSE - THERAPEUTIC COMMUNITY - SUPPORTIVE OUTPATIENT",
    "SUBSTANCE ABUSE - THERAPEUTIC COMMUNITY - RESIDENTIAL",
    "SUBSTANCE ABUSE - THERAPEUTIC COMMUNITY - RELAPSE OUTPATIENT",
    "SUBSTANCE ABUSE - THERAPEUTIC COMMUNITY - RELAPSE RESIDENTIAL",
]


def active_collateral_contact_treatment_program_filter() -> str:
    """Returns a SQL boolean predicate that filters
    `us_tx_normalized_state.state_program_assignment` rows to active treatment
    program assignments that count toward the additional collateral-contact
    requirement for RRC clients (per US_TX_NEEDS_SCHEDULED_COLLATERAL_CONTACT).

    Inline this expression in a WHERE clause selecting from
    `state_program_assignment`.
    """
    substance_abuse_ids_sql = list_to_query_string(
        US_TX_SUBSTANCE_ABUSE_PROGRAM_IDS_REQUIRING_COLLATERAL, quoted=True
    )
    return f"""participation_status_raw_text = 'ACTIVE'
        AND (
            JSON_EXTRACT_SCALAR(referral_metadata, '$.program_type') IN ('SPECIAL NEEDS', 'SEX OFFENDER')
            OR program_id IN ({substance_abuse_ids_sql})
        )"""


def alternative_contact_cadence_reason(
    is_every_other_month_column: str = "ca.is_every_other_month",
) -> str:
    """Returns a SQL CASE expression that builds an alternative_contact_cadence_reason
    string based on the is_every_other_month column (ODD/EVEN).

    Expects the column named by `is_every_other_month_column`, plus
    `cadence.quantity` and `cadence.frequency_date_part`, to be available in the
    query context.
    """
    return f"""
    CASE
        WHEN {is_every_other_month_column} = 'ODD'
            THEN CONCAT(cadence.quantity, ' EVERY ODD ', cadence.frequency_date_part)
        WHEN {is_every_other_month_column} = 'EVEN'
            THEN CONCAT(cadence.quantity, ' EVERY EVEN ', cadence.frequency_date_part)
        ELSE NULL
    END AS alternative_contact_cadence_reason"""


# TODO(#62741): Deprecate overdue_flag from reasons blobs in favor of
# compliance TES is_overdue column.
def contact_compliance_builder_critical_understaffing_monthly_virtual_override(
    description: str,
    base_criteria: StateSpecificTaskCriteriaBigQueryViewBuilder,
    contact_type: ContactType,
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
        contact_type (ContactType): The type of contact
    """

    standard_policy_criteria_name = base_criteria.view_id

    criteria_query = f"""
WITH has_monthly_home_contact_requirement AS (
    SELECT * FROM `{{project_id}}.tasks_views.us_tx_contact_cadence_spans_materialized`
    WHERE frequency = 1
        AND frequency_date_part = 'MONTH'
        AND contact_type = "{contact_type.value}"
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
            THEN "{contact_type.value} (VIRTUAL)"
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
    # Derive this criteria's name from the base (standard-policy) criteria rather than
    # from the contact_type value, so normalizing contact_type values doesn't rename the
    # criteria (and so two bases sharing a contact_type can't collide on the same name).
    criteria_name = f"US_TX_{standard_policy_criteria_name.upper()}".replace(
        "_STANDARD_POLICY", "_MONTHLY_CRITICAL_UNDERSTAFFING"
    )
    return StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        description=description,
        state_code=StateCode.US_TX,
        criteria_spans_query_template=criteria_query,
        contact_types=[contact_type],
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
                name="scheduled_contact_dates",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="String list dates of scheduled contacts between start date and contact due date",
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


# Values of the `type_agnostic_criteria` routing column in the type-agnostic contact
# standards CSV. Each labels the criterion a standard is evaluated by; concurrent
# obligations for the same (supervision_level, case_type) must use different buckets.
TYPE_AGNOSTIC_CRITERIA_STANDARD_POLICY = "STANDARD_POLICY"
TYPE_AGNOSTIC_CRITERIA_STANDARD_POLICY_SECONDARY = "STANDARD_POLICY_SECONDARY"
TYPE_AGNOSTIC_CRITERIA_SCHEDULED_OFFICE_OR_VIRTUAL_OFFICE = (
    "SCHEDULED_OFFICE_OR_VIRTUAL_OFFICE"
)


def contact_compliance_builder_type_agnostic(
    criteria_name: str,
    description: str,
    where_clause: str,
    contact_types: Sequence[ContactType],
    use_alternative_contact_cadence_reason: bool = False,
) -> StateSpecificTaskCriteriaBigQueryViewBuilder:
    """Returns a state-specific criteria view builder indicating the spans of time when
    a person has on supervision has not met the contact compliance cadence for a given
    contact type.

    Args:
        criteria_name (str): The name of the criteria
        description (str): The description of the criteria
        where_clause (str): What type-agnostic contacts to filter down to
        contact_types (list[ContactType]): The contact types that satisfy / close out
            this criterion (the union of accepted types this criterion can be met by).
        use_alternative_contact_cadence_reason (bool): If True, COALESCE the
            pre-computed `alternative_contact_cadence_reason` from the type-agnostic
            cadence spans view into the `contact_cadence` string (e.g. surfaces
            "1 EVERY EVEN MONTH" for MINIMUM/SMI office contacts).
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
            {"p.alternative_contact_cadence_reason," if use_alternative_contact_cadence_reason else ""}
        FROM `{{project_id}}.tasks_views.us_tx_contact_cadence_spans_type_agnostic_materialized` p
        LEFT JOIN contact_info ci
            ON p.person_id = ci.person_id
            AND ci.contact_type IN UNNEST(SPLIT(p.contact_types_accepted, ','))
            AND ci.contact_date >= {truncate_to_calendar_period("p.contact_period_start", "p.frequency_date_part")}
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
            {"alternative_contact_cadence_reason," if use_alternative_contact_cadence_reason else ""}
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
            {"alternative_contact_cadence_reason," if use_alternative_contact_cadence_reason else ""}
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
            {"alternative_contact_cadence_reason," if use_alternative_contact_cadence_reason else ""}
        FROM lookback_cte
        WHERE contact_date IS NOT NULL
            -- Restrict critical dates to the contact period range so that contacts
            -- from the calendar-month lookback don't create sub-period boundaries
            -- outside the period, which would cause overlapping output spans when
            -- a cadence change splits a month into two contact periods.
            AND contact_date BETWEEN contact_period_start AND contact_period_end_exclusive
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
            {"alternative_contact_cadence_reason," if use_alternative_contact_cadence_reason else ""}
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
            {"p.alternative_contact_cadence_reason," if use_alternative_contact_cadence_reason else ""}
        FROM divided_periods p
        LEFT JOIN contact_info ci
            ON p.person_id = ci.person_id
            AND ci.contact_date >= {truncate_to_calendar_period("p.contact_period_start", "p.frequency_date_part")}
            AND ci.contact_date < p.period_end
            AND ci.contact_type IN UNNEST(SPLIT(p.contact_types_accepted, ','))
        -- Filter out zero-day spans that occur when contact dates coincide with period boundaries
        WHERE period_end IS NOT NULL AND period_start < {nonnull_end_date_clause('period_end')}
    ),
    -- CTE that counts the contacts by type up to a certain date
    contact_count AS (
        SELECT
            *,
            SUM (case when contact_type = "{ContactType.HOME_VISIT.value}" THEN 1 ELSE 0 END) OVER (PARTITION BY person_id, contact_period_start ORDER BY period_start asc) as scheduled_home_count,
            SUM (case when contact_type = "{ContactType.OFFICE_VISIT.value}" THEN 1 ELSE 0 END) OVER (PARTITION BY person_id, contact_period_start ORDER BY period_start asc) as scheduled_office_count,
            SUM (case when contact_type = "{ContactType.SCHEDULED_FIELD_CONTACT.value}" THEN 1 ELSE 0 END) OVER (PARTITION BY person_id, contact_period_start ORDER BY period_start asc) as scheduled_field_count,
            SUM (case when contact_type = "{ContactType.UNSCHEDULED_HOME_VISIT.value}" THEN 1 ELSE 0 END) OVER (PARTITION BY person_id, contact_period_start ORDER BY period_start asc) as unscheduled_home_count,
            SUM (case when contact_type = "{ContactType.SCHEDULED_VIRTUAL_OFFICE_VISIT.value}" THEN 1 ELSE 0 END) OVER (PARTITION BY person_id, contact_period_start ORDER BY period_start asc) as scheduled_virtual_office_count,
            SUM (case when contact_type = "{ContactType.UNSCHEDULED_FIELD_CONTACT.value}" THEN 1 ELSE 0 END) OVER (PARTITION BY person_id, contact_period_start ORDER BY period_start asc) as unscheduled_field_count,
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
            cc.supervision_level,
            cc.case_type,
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
            cc.contact_types_accepted,
            CASE 
                WHEN period_start = contact_period_start
                    THEN "START"
                WHEN period_end =  contact_period_end_exclusive
                    THEN "END"
                ELSE
                 "CONTACT"
            END AS period_type,
            {"COALESCE(alternative_contact_cadence_reason, " if use_alternative_contact_cadence_reason else ""}CASE
                WHEN frequency = 1 AND frequency_date_part = "MONTH"
                    THEN "1 EVERY MONTH"
                WHEN frequency = 1 AND frequency_date_part = "WEEK"
                    THEN "1 EVERY WEEK"
                WHEN frequency = 1 AND frequency_date_part = "DAY"
                    THEN "1 EVERY DAY"
                ELSE
                    CONCAT("1 EVERY ", frequency, " ", frequency_date_part, "S")
            END{")" if use_alternative_contact_cadence_reason else ""} AS contact_cadence
        FROM contact_count cc
        LEFT JOIN types_and_amounts_due_cte td
            ON cc.supervision_level = td.supervision_level
            AND cc.contact_types_accepted = td.contact_types_accepted
            -- The type-agnostic standards table leaves case_type blank for standards
            -- that apply regardless of case type (e.g. RESIDENTIAL_PROGRAM), so treat a
            -- blank standards case_type as a wildcard that matches the client's actual
            -- case_type. This mirrors the join in us_tx_contact_cadence_spans_type_agnostic;
            -- without it the requirement columns come back NULL and the contact never clears.
            AND (cc.case_type = td.case_type OR COALESCE(td.case_type, '') = '')

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
            contact_cadence,
            supervision_level,
            case_type
        FROM compliance_check cc
        LEFT JOIN contact_info ci
          ON cc.person_id = ci.person_id
            AND ci.contact_type IN UNNEST(SPLIT(contact_types_accepted, ','))
            AND ci.contact_date <= start_date
        QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id, contact_types_accepted, start_date ORDER BY contact_date DESC) = 1
    ),
    join_scheduled_contacts AS (
        SELECT 
            periods.person_id,
            periods.start_date, 
            ARRAY_TO_STRING(
                ARRAY_AGG(
                    CAST(scheduled_contacts.scheduled_contact_date AS STRING)
                    IGNORE NULLS 
                    ORDER BY scheduled_contacts.scheduled_contact_date ASC
                ), ', '
            ) as scheduled_contact_dates,
            TO_JSON_STRING(ARRAY_AGG(
            STRUCT(
                scheduled_contacts.scheduled_contact_date,
                scheduled_contacts.contact_type
            )
            IGNORE NULLS 
            ORDER BY scheduled_contacts.scheduled_contact_date ASC
            )) AS scheduled_contacts_info
        FROM finalized_periods as periods
        LEFT JOIN `{{project_id}}.tasks_views.us_tx_scheduled_contacts_preprocessed_materialized` as scheduled_contacts
            ON scheduled_contacts.person_id = periods.person_id
            AND scheduled_contacts.scheduled_contact_date BETWEEN periods.start_date and periods.contact_due_date
            AND scheduled_contacts.contact_type IN UNNEST(SPLIT(periods.contact_types_accepted, ','))
        WHERE scheduled_contacts.status = 'SCHEDULED'
        GROUP BY
            periods.person_id,
            periods.start_date
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
        contact_cadence,
        contact_types_accepted,
        supervision_level,
        case_type,
        scheduled_contact_dates,
        scheduled_contacts_info
      )) AS reason,
    FROM finalized_periods
    LEFT JOIN join_scheduled_contacts
      USING (person_id, start_date)
    """

    return StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        description=description,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        state_code=StateCode.US_TX,
        contact_types=contact_types,
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
            ReasonsField(
                name="scheduled_contact_dates",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="String list dates of scheduled contacts between start date and contact due date",
            ),
            ReasonsField(
                name="scheduled_contacts_info",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="JSON string that shows the contact type is for each scheduled contact date",
            ),
        ],
    )


# TRAS types that count toward the SISP review schedule (CSST excluded).
SISP_REVIEW_TRAS_TYPES = ("TX_CST", "TX_SRT", "TX_RT")

# TRAS levels that get the 6-month SISP review cadence. HIGH gets 12 months.
SISP_REVIEW_NON_HIGH_LEVELS = ("MODERATE", "LOW_MODERATE", "LOW")


def sisp_review_contact_event_query(contact_type: str) -> str:
    """Zero-row placeholder for SISP-review completion events.

    TODO(#78423): replace with the real SISP-review completion source once
    known.
    """
    return f"""
    SELECT *
    FROM (
        SELECT
            CAST(NULL AS STRING) AS state_code,
            CAST(NULL AS INT64) AS person_id,
            CAST(NULL AS STRING) AS contact_external_id,
            CAST(NULL AS DATE) AS contact_date,
            CAST('{contact_type}' AS STRING) AS contact_type,
    )
    WHERE FALSE
    """


def sisp_recurring_review_cadence_spans_query(
    *,
    contact_type: str,
    cadence_months: int,
    assessment_level_filter_sql: str,
) -> str:
    """Builds the cadence-spans query for a SISP recurring-review criterion.

    Finds the time periods when a client is both on SISP and at a given TRAS
    level, then chops each one into `cadence_months`-long review periods.
    The result feeds `fixed_cadence_contact_compliance_builder` as its
    `custom_contact_cadence_spans` input.

    Args:
        contact_type: label emitted on each cadence row.
        cadence_months: months between reviews.
        assessment_level_filter_sql: SQL fragment to keep only the TRAS
            levels this criterion cares about (e.g.
            `assessment_level = 'High'`).
    """
    return f"""
    WITH
    -- Stack SISP supervision spans and TRAS-level spans together so we can
    -- find where they overlap in the next CTE.
    sisp_and_tras_spans AS (
        SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            case_type,
            CAST(NULL AS STRING) AS assessment_level,
        FROM `{{project_id}}.tasks_views.us_tx_case_type_supervision_level_spans_materialized`
        WHERE state_code = 'US_TX' AND case_type = 'INTENSE_SUPERVISION'

        UNION ALL

        SELECT
            state_code,
            person_id,
            assessment_date AS start_date,
            score_end_date_exclusive AS end_date,
            CAST(NULL AS STRING) AS case_type,
            assessment_level,
        FROM `{{project_id}}.sessions.assessment_score_sessions_materialized`
        WHERE state_code = 'US_TX'
            AND assessment_type IN {SISP_REVIEW_TRAS_TYPES}
            AND assessment_level IS NOT NULL
            AND {assessment_level_filter_sql}
    ),
    {create_sub_sessions_with_attributes(
        table_name='sisp_and_tras_spans',
        end_date_field_name='end_date',
    )},
    -- Keep only the spans where the client is on SISP AND has a TRAS level.
    sisp_level_spans_raw AS (
        SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            MAX(case_type) AS case_type,
            -- If multiple TRAS types (CST/SRT/RT) overlap, pick one
            -- deterministically. Rare in practice.
            MAX(assessment_level) AS assessment_level,
        FROM sub_sessions_with_attributes
        GROUP BY 1, 2, 3, 4
        HAVING case_type IS NOT NULL AND assessment_level IS NOT NULL
    ),
    sisp_level_spans AS (
        {aggregate_adjacent_spans(
            table_name='sisp_level_spans_raw',
            attribute=['case_type', 'assessment_level'],
            end_date_field_name='end_date',
        )}
    ),
    cadence_per_span AS (
        SELECT
            state_code,
            person_id,
            start_date AS cadence_start_date,
            -- For open-ended spans, extend one cycle past today so we still
            -- get a future cadence period to surface.
            IFNULL(
                end_date,
                LAST_DAY(DATE_ADD(
                    CURRENT_DATE('US/Eastern'),
                    INTERVAL {cadence_months} MONTH
                ))
            ) AS cadence_end_date_exclusive,
            -- Start of the next SISP-AND-level span for this person, if any.
            -- Used to keep an extended last cadence period from overlapping
            -- the next span's cadence.
            LEAD(start_date) OVER (
                PARTITION BY state_code, person_id ORDER BY start_date
            ) AS next_span_start_date,
        FROM sisp_level_spans
    )

    SELECT
        state_code,
        person_id,
        CAST(NULL AS STRING) AS supervision_level,
        CAST(NULL AS STRING) AS case_type,
        '{contact_type}' AS contact_type,
        1 AS quantity,
        {cadence_months} AS frequency,
        'MONTH' AS frequency_date_part,
        cadence_start_date AS start_date,
        cadence_end_date_exclusive AS end_date,
        period_start AS contact_period_start,
        -- Full cadence period, trimmed back if a later SISP-AND-level span
        -- would overlap.
        DATE_SUB(
            LEAST(
                DATE_ADD(period_start, INTERVAL {cadence_months} MONTH),
                IFNULL(next_span_start_date, DATE '9999-12-31')
            ),
            INTERVAL 1 DAY
        ) AS contact_period_end,
    FROM cadence_per_span,
    UNNEST(GENERATE_DATE_ARRAY(
        cadence_start_date,
        DATE_SUB(cadence_end_date_exclusive, INTERVAL 1 DAY),
        INTERVAL {cadence_months} MONTH
    )) AS period_start
    WHERE cadence_start_date < cadence_end_date_exclusive
    """


def us_tx_contact_type_case_sql(*, metadata_column: str) -> str:
    """Returns the CASE expression that maps US_TX raw contact methods/reasons to the
    tracked `ContactType` values, emitting NULL (so the row is dropped downstream) for
    any method with no tracked type. Shared by the completed-contacts and
    scheduled-contacts preprocessed views, which differ only in the JSON metadata
    column that carries the virtual-office flag.

    Args:
        metadata_column: Name of the JSON metadata column holding `$.VIRTUAL_FLAG`
            (`supervision_contact_metadata` for completed contacts,
            `scheduled_supervision_contact_metadata` for scheduled contacts).
    """
    return f"""-- Map only the contact methods that correspond to a tracked ContactType.
-- Any other method (e.g. TELEPHONIC, ELECTRONIC, SURVEILLANCE, WRITTEN,
-- unscheduled OFFICE/EMPLOYMENT) is not a Tasks-product contact type and
-- maps to NULL; those rows are dropped in the final SELECT.
CASE
    WHEN contact_reason_raw_text LIKE "%UNSCHEDULED%" AND contact_method_raw_text = "HOME"
        THEN "{ContactType.UNSCHEDULED_HOME_VISIT.value}"
    WHEN contact_reason_raw_text LIKE "%UNSCHEDULED%" AND contact_method_raw_text = "FIELD"
        THEN "{ContactType.UNSCHEDULED_FIELD_CONTACT.value}"
    WHEN contact_reason_raw_text LIKE "%UNSCHEDULED%"
        THEN NULL
    WHEN JSON_VALUE({metadata_column}, '$.VIRTUAL_FLAG') = "1"
        AND contact_method_raw_text = 'OFFICE'
        THEN "{ContactType.SCHEDULED_VIRTUAL_OFFICE_VISIT.value}"
    WHEN contact_method_raw_text = "EMPLOYMENT"
        THEN "{ContactType.SCHEDULED_FIELD_CONTACT.value}"
    WHEN contact_method_raw_text = "HOME"
        THEN "{ContactType.HOME_VISIT.value}"
    WHEN contact_method_raw_text = "OFFICE"
        -- The VIRTUAL_FLAG branch above still emits SCHEDULED_VIRTUAL_OFFICE;
        -- only the non-virtual office type maps here.
        THEN "{ContactType.OFFICE_VISIT.value}"
    WHEN contact_method_raw_text = "FIELD"
        THEN "{ContactType.SCHEDULED_FIELD_CONTACT.value}"
    ELSE NULL
END AS contact_type,"""


def _us_tx_fees_no_records_fallback_select(*, criteria: str) -> str:
    """Returns a SQL SELECT producing a 'No records found' fallback row.

    Caller is responsible for appending the FROM/LEFT JOIN/WHERE to complete
    the UNION ALL block — this function only produces the SELECT columns.
    """
    return f"""SELECT
        pilot_clients.person_id,
        "{criteria}"                                                    AS criteria,
        "No records found"                                              AS note_title,
        "If this is inconsistent with OIMS, please let us know via feedback@recidiviz.org or the browser's chat feature."                          AS note_body,
        CAST(NULL AS DATE)                                              AS event_date,"""


# TODO(OBT-33896): Drop this constant when we FSL.
_US_TX_FEES_PILOT_CLIENTS_SUBQUERY = """(
    SELECT DISTINCT sc.person_id
    FROM `{project_id}.analyst_data.us_tx_supervision_staff_reporting_chain_materialized` sc
    INNER JOIN `{project_id}.static_reference_tables.us_tx_ers_ars_fines_fees_pilot_users` pilot
        ON pilot.staff_id = sc.staff_id
)"""


def us_tx_fines_fees_balances_case_notes() -> str:
    """Returns a SQL fragment selecting active fines/fees balances as case notes.

    Produces one row per active fee type with a non-zero assessed amount, plus a
    "No records found" fallback row for pilot clients with no qualifying fee records.
    Columns: person_id, criteria ('Current Fees'), note_title (fee type), note_body (assessed amount and remaining balance), event_date (date of last fee-related interaction).
    """
    pilot_clients = _US_TX_FEES_PILOT_CLIENTS_SUBQUERY
    return f"""
    SELECT
        person_id,
        "Current Fees"                                                  AS criteria,
        -- Ex: 'CRIME VICTIM FUND' --> 'Crime victim fund'
        INITCAP(fee_type)                                               AS note_title,
        -- Ex: 'Amount assessed: $600.00 | Balance remaining: $20.00'
        CONCAT(
            "Amount assessed: ", FORMAT("$%.2f", assessed_amount),
            " | Balance remaining: ", FORMAT("$%.2f", unpaid_balance)
        )                                                               AS note_body,
        -- fees.start_date surfaces the current date for the time being, but when fees sessions are hydrated with real spans, the start_date will represent the start of the current span, basically indicating the last time a fee was assessed, changed, or paid.
        -- TODO(#78182): Hydrate fees sessions data with real spans and remove above comment.
        fees.start_date                                                 AS event_date,
    FROM `{{project_id}}.analyst_data.us_tx_fines_fees_sessions_preprocessed` fees
    INNER JOIN {pilot_clients} pilot_clients
        USING (person_id)
    WHERE state_code = "US_TX"
        AND CURRENT_DATE('US/Eastern') BETWEEN start_date AND {nonnull_end_date_clause('end_date')}
        AND fee_type NOT IN ("ALL", "ALL_NON_RESTITUTION")
        AND assessed_amount != 0

    -- TODO(OBT-33896): Drop this block when we FSL.
    -- If a person in the pilot population does not have fee balances data, then we implement the no-records fallback JSON.
    UNION ALL

    {_us_tx_fees_no_records_fallback_select(criteria="Current Fees")}
    FROM {pilot_clients} pilot_clients
    LEFT JOIN (
        SELECT DISTINCT person_id
        FROM `{{project_id}}.analyst_data.us_tx_fines_fees_sessions_preprocessed`
        WHERE state_code = "US_TX"
            AND CURRENT_DATE('US/Eastern') BETWEEN start_date AND {nonnull_end_date_clause('end_date')}
            AND fee_type NOT IN ("ALL", "ALL_NON_RESTITUTION")
            AND assessed_amount != 0
    ) has_fees USING (person_id)
    WHERE has_fees.person_id IS NULL"""


def us_tx_fee_type_case_sql(*, column: str) -> str:
    """Returns a CASE expression mapping FTRN_TRANS_TYPE single-letter codes to full fee type names, for use in us_tx_fines_fees_recent_payments_case_notes().
    Used specifically for the TARCDZ_TPFEES_TRANS table, which stores all fee transactions for parole clients.
    """
    return f"""CASE {column}
        WHEN 'C' THEN 'Crime Victim Fund'
        WHEN 'S' THEN 'Supervision'
        WHEN 'E' THEN 'Post-Secondary Education Reimbursement'
        WHEN 'P' THEN 'Sexual Assault Program'
        WHEN 'R' THEN 'Restitution'
        ELSE NULL
    END"""


def us_tx_fines_fees_recent_payments_case_notes(
    *,
    n_distinct_transaction_dates: int = 3,
    max_transactions: int = 15,
) -> str:
    """Returns a SQL fragment selecting recent fee transactions as case notes.

    Produces one row per transaction on the most recent N distinct payment dates
    (capped at max_transactions total), plus a "No records found" fallback row for
    pilot clients with no transaction records.
    Columns: person_id, criteria ('Most Recent Payments'), note_title, note_body, event_date.
    """
    pilot_clients = _US_TX_FEES_PILOT_CLIENTS_SUBQUERY

    # Obtains all recent payments that happened on the last three distinct transaction dates, up to a total of 15 transactions.
    recent_payments_subquery = f"""(
        SELECT
            ext_id.person_id,
            "Most Recent Payments"                                          AS criteria,
            -- Maps FTRN_TRANS_TYPE single-letter codes to readable fee type names.
            -- Excludes V (void) and O; rows with unknown types are pre-filtered out in the inner subquery.
            -- TODO(OBT-35355): Update when ITD confirms what FTRN_TRANS_TYPE = 'O' represents.
            {us_tx_fee_type_case_sql(column="trans.FTRN_TRANS_TYPE")}       AS note_title,
            FORMAT("$%.2f", SAFE_CAST(trans.FTRN_TRANS_AMT AS NUMERIC))    AS note_body,
            DATE(trans.FTRN_DATE_POSTED)                                    AS event_date,
        FROM (
            -- TODO(OBT-35387): Create a preprocessed transactions view that accounts for TODO(OBT-35356) and TODO(OBT-35355)
            -- TODO(OBT-35356): Clarify with ITD which columns can deduplicate transaction rows and how to identify voided transactions.
            -- Pre-filter to known fee types so that DENSE_RANK and ROW_NUMBER window functions
            -- in QUALIFY only count valid rows. Without this, large batches of void (V) rows on a
            -- single date can consume all ROW_NUMBER slots and push valid rows past the cap.
            SELECT DISTINCT
                FTRN_DPS_NO,
                FTRN_TERM_ID,
                FTRN_DATE_POSTED,
                FTRN_TIME_POSTED,
                FTRN_SEQ_NO,
                FTRN_TRANS_TYPE,
                FTRN_TRANS_AMT
            FROM `{{project_id}}.us_tx_raw_data_up_to_date_views.TARCDZ_TPFEES_TRANS_latest`
            WHERE {us_tx_fee_type_case_sql(column="FTRN_TRANS_TYPE")} IS NOT NULL
        ) trans
        INNER JOIN (
            SELECT external_id, person_id
            FROM `{{project_id}}.us_tx_normalized_state.state_person_external_id`
            WHERE id_type = 'US_TX_SID'
        ) ext_id
            ON ext_id.external_id = trans.FTRN_DPS_NO
        -- TODO(OBT-33896): Drop this inner join when we FSL or change the pilot population.
        INNER JOIN {pilot_clients} pilot_clients
            USING (person_id)
        QUALIFY
            DENSE_RANK() OVER (
                PARTITION BY ext_id.person_id
                ORDER BY DATE(trans.FTRN_DATE_POSTED) DESC
            ) <= {n_distinct_transaction_dates}
            AND ROW_NUMBER() OVER (
                PARTITION BY ext_id.person_id
                ORDER BY DATE(trans.FTRN_DATE_POSTED) DESC, trans.FTRN_SEQ_NO
            ) <= {max_transactions}
    )"""

    # Grabs recent payments and adds in fallback no-records JSON for clients without any transaction data
    return f"""
    SELECT person_id, criteria, note_title, note_body, event_date
    FROM {recent_payments_subquery}
    -- TODO(OBT-33896): Drop this block when we FSL or change the pilot population.
    UNION ALL
    {_us_tx_fees_no_records_fallback_select(criteria="Most Recent Payments")}
    FROM {pilot_clients} pilot_clients
    LEFT JOIN (SELECT DISTINCT person_id FROM {recent_payments_subquery}) rp
        USING(person_id)
    WHERE rp.person_id IS NULL
    """
