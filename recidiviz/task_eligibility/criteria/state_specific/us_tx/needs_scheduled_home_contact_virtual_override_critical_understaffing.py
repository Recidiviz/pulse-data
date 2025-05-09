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
on regular low/low medium supervision who is associated with a critically
understaffed location is due for a home contact, which can be completed virtually.
"""

from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_intersection_spans,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = (
    "US_TX_NEEDS_SCHEDULED_HOME_CONTACT_VIRTUAL_OVERRIDE_CRITICAL_UNDERSTAFFING"
)

_DESCRIPTION = """
Defines a criteria span view that shows spans of time during which a client
on regular low/low medium supervision who is associated with a critically
understaffed location is due for a home contact, which can be completed virtually.
"""

_QUERY_TEMPLATE = f"""
WITH has_monthly_home_contact_requirement_low_medium AS (
    SELECT
        *,
        "SCHEDULED HOME (VIRTUAL)" AS override_contact_type,
    FROM
        `{{project_id}}.analyst_data.us_tx_contact_cadence_spans_materialized`
    WHERE
        contact_type = "SCHEDULED HOME"
        AND case_type = "GENERAL"
        AND supervision_level IN ("MINIMUM", "MEDIUM")
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
intersection_spans_critical_understaffing AS (
    {create_intersection_spans(
        table_1_name="has_monthly_home_contact_requirement_low_medium",
        table_2_name="critical_understaffing_spans",
        index_columns=["state_code", "person_id"],
        table_1_columns=["override_contact_type"],
        table_2_columns=["officer_in_critically_understaffed_location"],
        table_1_start_date_field_name="start_date",
        table_1_end_date_field_name="end_date",
        table_2_start_date_field_name="start_date",
        table_2_end_date_field_name="end_date"
    )}
)
,
-- Further intersect with the home contacts policy to get the meets_criteria values
-- of the original policy
home_contacts_policy AS (
    SELECT
        *
    FROM
        `{{project_id}}.task_eligibility_criteria_us_tx.needs_scheduled_home_contact_standard_policy_materialized`
)
,
intersection_spans_critical_understaffing_home_contact_required AS (
    {create_intersection_spans(
        table_1_name="intersection_spans_critical_understaffing",
        table_2_name="home_contacts_policy",
        index_columns=["state_code", "person_id"],
        table_1_columns=["override_contact_type", "officer_in_critically_understaffed_location"],
        table_2_columns=["meets_criteria"],
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
        override_contact_type,
        officer_in_critically_understaffed_location
    )) AS reason,
    override_contact_type,
    officer_in_critically_understaffed_location,
FROM intersection_spans_critical_understaffing_home_contact_required
"""


VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        state_code=StateCode.US_TX,
        reasons_fields=[
            ReasonsField(
                name="override_contact_type",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Override contact type based on special policy.",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
