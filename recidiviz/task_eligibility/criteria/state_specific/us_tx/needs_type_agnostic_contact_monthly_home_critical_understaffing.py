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
"""
Defines a criteria span view that shows spans of time during which a client
with a monthly schedule home contacts who is associated with a critically
understaffed location is due for a type-agnostic contact where a home contact is
an accepted type. These monthly contacts can be alternated between virtual and 
in-person visits according to a schedule by month and client last name, as indicated 
by `override_contact_types_accepted`.
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
from recidiviz.task_eligibility.criteria.state_specific.us_tx import (
    needs_type_agnostic_contact_standard_policy,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.general_criteria_builders import (
    get_reason_json_fields_query_template_for_criteria,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.string_formatting import fix_indent

_CRITERIA_NAME = "US_TX_NEEDS_TYPE_AGNOSTIC_CONTACT_MONTHLY_HOME_CRITICAL_UNDERSTAFFING"

_DESCRIPTION = """
Defines a criteria span view that shows spans of time during which a client
with a monthly schedule home contacts who is associated with a critically
understaffed location is due for a type-agnostic contact where a home contact is
an accepted type. These monthly contacts can be alternated between virtual and 
in-person visits according to a schedule by month and client last name, as indicated 
by `override_contact_types_accepted`.
"""

_QUERY_TEMPLATE = f"""
-- Identify all type-agnostic contact requirements where there is a monthly home contact option
WITH has_monthly_home_contact_requirement AS (
    SELECT * 
    FROM
        `{{project_id}}.task_eligibility_criteria_us_tx.needs_type_agnostic_contact_standard_policy_materialized` 
    WHERE
        JSON_EXTRACT_SCALAR(reason_v2, "$.contact_types_accepted") LIKE "%HOME%"
        AND JSON_EXTRACT_SCALAR(reason_v2, "$.frequency") = "1 MONTH"
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
        (
            LEFT(JSON_EXTRACT_SCALAR(person.full_name, "$.surname"), 1) < "N"
            AND MOD(EXTRACT(MONTH FROM contact_month_start_date), 2) = 0
        )
        OR (
            LEFT(JSON_EXTRACT_SCALAR(person.full_name, "$.surname"), 1) >= "N"
            AND MOD(EXTRACT(MONTH FROM contact_month_start_date), 2) = 1
        ) AS has_override,
    FROM
        date_range,
        UNNEST(GENERATE_DATE_ARRAY(
            CAST(min_date AS DATE),
            IFNULL(CAST(max_date AS DATE), CURRENT_DATE('US/Eastern')),
            INTERVAL 1 MONTH
        )) AS contact_month_start_date
    INNER JOIN
        `{{project_id}}.normalized_state.state_person` person
    USING (person_id)
)
,
-- Intersect the compliant month ranges with the original monthly contact
-- cadence spans to get the final spans of time where a client is due for
-- a monthly scheduled home contact under the critical understaffing policy,
-- along with the value of the override contact type.
intersection_spans AS (
    {create_intersection_spans(
        table_1_name="has_monthly_home_contact_requirement",
        table_2_name="special_monthly_contact_cadence",
        index_columns=["state_code", "person_id"],
        table_1_columns=["meets_criteria", "reason_v2"],
        table_2_columns=["has_override"],
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
        table_1_columns=["meets_criteria", "reason_v2", "has_override"],
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
    -- meets_criteria still reflects the standard type-agnostic contacts policy.
    meets_criteria,
    TO_JSON(STRUCT(
{fix_indent(
            get_reason_json_fields_query_template_for_criteria(needs_type_agnostic_contact_standard_policy.VIEW_BUILDER),
            indent_level = 8
        )},
        REPLACE(
            JSON_EXTRACT_SCALAR(reason_v2, "$.contact_types_accepted"),
            "HOME",
            "HOME (VIRTUAL)"
        ) AS override_contact_types_accepted,
        officer_in_critically_understaffed_location
    )) AS reason,
{fix_indent(
        get_reason_json_fields_query_template_for_criteria(needs_type_agnostic_contact_standard_policy.VIEW_BUILDER),
        indent_level = 4
    )},
    REPLACE(
        JSON_EXTRACT_SCALAR(reason_v2, "$.contact_types_accepted"),
        "HOME",
        "HOME (VIRTUAL)"
    ) AS override_contact_types_accepted,
    officer_in_critically_understaffed_location,
FROM intersection_spans_with_critical_understaffing
"""


VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
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
        ReasonsField(
            name="frequency",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Contact cadence.",
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
            name="override_contact_types_accepted",
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

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
