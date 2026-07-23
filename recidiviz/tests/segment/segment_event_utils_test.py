# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for segment_event_utils.py"""
import unittest

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.segment.product_type import ProductType
from recidiviz.segment.segment_event_utils import (
    FIRST_IX_EXPORT_DATE,
    _get_url_filter_for_all_products,
    build_segment_event_view_query_template,
    segment_event_schema,
)


class TestSegmentEventSchema(unittest.TestCase):
    """Tests for segment_event_schema()."""

    def test_schema_fixed_columns(self) -> None:
        address = BigQueryAddress(
            dataset_id="pulse_dashboard_segment_metrics",
            table_id="frontend_caseload_search",
        )
        schema = segment_event_schema(
            segment_events_source_table_address=address,
            additional_attribute_cols=[],
        )
        col_info = [(c.name, c.field_type, c.mode) for c in schema]
        self.assertEqual(
            col_info,
            [
                ("state_code", bigquery.SqlTypeNames.STRING, "NULLABLE"),
                ("user_id", bigquery.SqlTypeNames.STRING, "NULLABLE"),
                ("email_address", bigquery.SqlTypeNames.STRING, "NULLABLE"),
                ("event_ts", bigquery.SqlTypeNames.DATETIME, "NULLABLE"),
                ("person_id", bigquery.SqlTypeNames.INTEGER, "NULLABLE"),
                ("session_id", bigquery.SqlTypeNames.STRING, "NULLABLE"),
                ("context_page_path", bigquery.SqlTypeNames.STRING, "NULLABLE"),
                ("context_page_url", bigquery.SqlTypeNames.STRING, "NULLABLE"),
                ("product_type", bigquery.SqlTypeNames.STRING, "NULLABLE"),
            ],
        )

    def test_schema_includes_additional_attribute_cols(self) -> None:
        address = BigQueryAddress(
            dataset_id="pulse_dashboard_segment_metrics",
            table_id="frontend_caseload_search",
        )
        schema = segment_event_schema(
            segment_events_source_table_address=address,
            additional_attribute_cols=["search_type", "search_count"],
        )
        col_info = [(c.name, c.field_type, c.mode) for c in schema]
        self.assertEqual(len(col_info), 11)
        self.assertEqual(
            col_info[-2:],
            [
                ("search_type", bigquery.SqlTypeNames.STRING, "NULLABLE"),
                ("search_count", bigquery.SqlTypeNames.INTEGER, "NULLABLE"),
            ],
        )

    def test_schema_attribute_col_not_in_yaml_raises(self) -> None:
        address = BigQueryAddress(
            dataset_id="pulse_dashboard_segment_metrics",
            table_id="frontend_caseload_search",
        )
        with self.assertRaises(ValueError):
            segment_event_schema(
                segment_events_source_table_address=address,
                additional_attribute_cols=["nonexistent_column"],
            )


class TestBuildSegmentEventViewQueryTemplate(unittest.TestCase):
    """Tests for build_segment_event_view_query_template()."""

    def test_staff_query_reidentifies_via_dashboard_users(self) -> None:
        # Staff events re-identify the user via the dashboard users table (surfacing
        # their email address) and never touch the resident record salt.
        query = build_segment_event_view_query_template(
            segment_table_sql_source=BigQueryAddress(
                dataset_id="pulse_dashboard_segment_metrics",
                table_id="frontend_opportunity_previewed",
            ),
            segment_table_jii_pseudonymized_id_columns=["client_id"],
            additional_attribute_cols=[],
            relevant_product_types=[ProductType.WORKFLOWS],
        )
        url_filter = _get_url_filter_for_all_products()
        expected_query = f"""
SELECT
    COALESCE(person.state_code, rdu.state_code) AS state_code,
    events.user_id,
    LOWER(rdu.email) AS email_address,
    DATETIME(events.timestamp, "US/Eastern") AS event_ts,
    person_id,
    session_id,
    context_page_path,
    context_page_url,
    CASE WHEN ((context_page_url LIKE 'https://dashboard.recidiviz.org/%')) AND (REGEXP_CONTAINS(context_page_url, r'/workflows') AND NOT REGEXP_CONTAINS(context_page_url, r'/workflows/(clients|residents|milestones|tasks)'))
  THEN 'WORKFLOWS'
ELSE "UNKNOWN_PRODUCT_TYPE" END AS product_type,
    
FROM (
    SELECT
        -- default columns for all views
        COALESCE(client_id) AS pseudonymized_id,
        timestamp,
    session_id,
        user_id,
        context_page_path,
        context_page_url,
        "frontend_opportunity_previewed" AS event,
        
    FROM
        `{{project_id}}.pulse_dashboard_segment_metrics.frontend_opportunity_previewed`
    -- events from prod deployment only
    WHERE
        {url_filter}
    -- dedupes events loaded more than once
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY loaded_at DESC) = 1
) events
-- join to filter out recidiviz users and others unidentified (if any)
INNER JOIN
    `{{project_id}}.workflows_views.reidentified_dashboard_users_materialized` rdu
ON
    -- We get the state_code above from `reidentified_dashboard_users`, which could have have an
    -- entry for a user for both US_ID and US_IX. We can't use the pseudonymized id to distinguish
    -- because they may match between both states. Instead, use the timestamp of the event to
    -- determine whether it is a US_ID event or a US_IX event.
    events.user_id = rdu.user_id
    AND (
        rdu.state_code != "US_ID"
        OR events.timestamp < "{FIRST_IX_EXPORT_DATE}"
    )
INNER JOIN
    `{{project_id}}.workflows_views.pseudonymized_id_to_person_id_materialized` person
ON
    events.pseudonymized_id = person.pseudonymized_id
    -- This condition ensures that we only keep events where the state_code of the person_id
    -- matches the state_code of the user_id.
    -- TODO(#60453): Investigate cases where rdu state_code doesn't match person state_code
    AND rdu.state_code = person.state_code

"""
        self.assertEqual(expected_query, query)

    def test_jii_query_skips_staff_join_and_uses_resident_salt(self) -> None:
        # JII events carry no staff identity: skip the dashboard users join, null out
        # the email, resolve state_code/person_id via the resident record salt where the
        # event's user_id is the resident's pseudonymized ID, and always inner join to
        # the person table to keep only residents with a resolvable person_id.
        query = build_segment_event_view_query_template(
            segment_table_sql_source=BigQueryAddress(
                dataset_id="jii_frontend_prod_segment_metrics",
                table_id="frontend_cpa_intake_chat_client_address_submitted",
            ),
            segment_table_jii_pseudonymized_id_columns=[],
            additional_attribute_cols=[],
            relevant_product_types=[ProductType.JII_OPPORTUNITIES_APP],
            user_is_jii=True,
        )
        url_filter = _get_url_filter_for_all_products()
        expected_query = f"""
SELECT
    person.state_code AS state_code,
    events.user_id,
    CAST(NULL AS STRING) AS email_address,
    DATETIME(events.timestamp, "US/Eastern") AS event_ts,
    person_id,
    session_id,
    context_page_path,
    context_page_url,
    CASE WHEN (context_page_url LIKE 'https://opportunities.app/%' OR context_page_url LIKE 'https://opportunities.edovo.com/%') AND NOT REGEXP_CONTAINS(context_page_url, r'/reentry-assessment')
  THEN 'JII_OPPORTUNITIES_APP'
ELSE "UNKNOWN_PRODUCT_TYPE" END AS product_type,
    
FROM (
    SELECT
        -- default columns for all views
        user_id AS pseudonymized_id,
        timestamp,
    session_id,
        user_id,
        context_page_path,
        context_page_url,
        "frontend_cpa_intake_chat_client_address_submitted" AS event,
        
    FROM
        `{{project_id}}.jii_frontend_prod_segment_metrics.frontend_cpa_intake_chat_client_address_submitted`
    -- events from prod deployment only
    WHERE
        {url_filter}
    -- dedupes events loaded more than once
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY loaded_at DESC) = 1
) events

INNER JOIN
    `{{project_id}}.workflows_views.pseudonymized_id_to_person_id_materialized` person
ON
    events.pseudonymized_id = person.pseudonymized_id
    AND person.pseudonymized_id_type = "PERSON_EXTERNAL_ID_WITH_RESIDENT_RECORD_SALT"

"""
        self.assertEqual(expected_query, query)
