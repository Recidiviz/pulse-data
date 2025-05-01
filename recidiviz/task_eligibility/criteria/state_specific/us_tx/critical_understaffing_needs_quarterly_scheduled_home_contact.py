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
Defines a criteria span view that shows the schedule of months during which a client
would be expected to have a contact if they were under the critical understaffing
quarterly home contact policy. These contacts are required to occur in January, 
April, July, and October. Note that this criteria query doesn't actually identify
clients in critically understaffed locations, but rather sets up the schedule of 
one month periods where a contact would be required.
"""


from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    create_intersection_spans,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_tx_query_fragments import (
    contact_compliance_builder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TX_CRITICAL_UNDERSTAFFING_NEEDS_QUARTERLY_SCHEDULED_HOME_CONTACT"

_DESCRIPTION = """
Defines a criteria span view that shows the schedule of months during which a client
would be expected to have a contact if they were under the critical understaffing
quarterly home contact policy. These contacts are required to occur in January, 
April, July, and October. Note that this criteria query doesn't actually identify
clients in critically understaffed locations, but rather sets up the schedule of 
one month periods where a contact would be required.
"""


def _get_special_understaffing_quarterly_contact_cadence() -> str:
    """
    Returns a query template with the special quarterly contact cadence for the critical understaffing
    policy, which is Jan, Apr, Jul, and Oct. Does so by taking all spans of time where
    a client is required to receive quarterly scheduled home contacts, then further
    narrows down these spans to only include the months of January, April, July, and October.
    """
    return f"""
WITH has_quarterly_home_contact_requirement AS (
    SELECT * FROM `{{project_id}}.analyst_data.us_tx_contact_cadence_spans_materialized`
    WHERE frequency_in_months = 3
    AND contact_type = "SCHEDULED HOME"
)
,
-- Further subsect this spans to only include jan/apr/jul/oct
date_range AS (
    SELECT
        person_id,
        state_code,
        DATE_TRUNC(MIN(start_date), YEAR) AS min_date,
        {revert_nonnull_end_date_clause(f"MAX({nonnull_end_date_clause('end_date')})")} AS max_date,
    FROM
        has_quarterly_home_contact_requirement
    GROUP BY 1, 2
)
,
-- Create a date range for each person_id
special_quarterly_contact_cadence AS (
    SELECT
        person_id,
        state_code,
        contact_month_start_date,
        DATE_ADD(contact_month_start_date, INTERVAL 1 MONTH) AS contact_month_end_date,
    FROM
        date_range,
        UNNEST(GENERATE_DATE_ARRAY(
            min_date,
            IFNULL(max_date, CURRENT_DATE('US/Eastern')),
            INTERVAL 3 MONTH
        )) AS contact_month_start_date
)
,
-- Intersect the compliant month ranges with the original quarterly contact
-- cadence spans to get the final spans of time where a client is due for
-- a quarterly scheduled home contact under the critical understaffing policy.
intersection_spans AS (
    {create_intersection_spans(
        table_1_name="has_quarterly_home_contact_requirement",
        table_2_name="special_quarterly_contact_cadence",
        index_columns=["state_code", "person_id"],
        table_1_columns=["contact_type", "supervision_level", "case_type", "quantity", "frequency_in_months"],
        table_2_columns=[],
        table_1_start_date_field_name="month_start",
        table_1_end_date_field_name="month_end",
        table_2_start_date_field_name="contact_month_start_date",
        table_2_end_date_field_name="contact_month_end_date"
    )}
)
SELECT
    * EXCEPT (start_date, end_date_exclusive),
    start_date AS month_start,
    end_date_exclusive AS month_end
FROM intersection_spans
"""


VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = contact_compliance_builder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    contact_type="SCHEDULED HOME",
    custom_contact_cadence_spans=_get_special_understaffing_quarterly_contact_cadence(),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
