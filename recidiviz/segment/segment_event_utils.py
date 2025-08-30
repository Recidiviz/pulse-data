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
"""Util functions for processing segment events."""

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.segment.product_type import ProductType
from recidiviz.segment.segment_event_config import get_segment_event_types_by_product

# The first US_IX export for workflows was on 1/11 in staging and 1/17 in prod.
# For simplicity, use the prod date.
FIRST_IX_EXPORT_DATE = "2023-01-17"


def _get_product_type_case_when_statement() -> str:
    product_type_conditionals = [
        f"""WHEN {product_type.context_page_filter_query_fragment(context_page_url_col_name='context_page_url')}
    AND event in ({list_to_query_string(events, quoted=True)})
  THEN '{product_type.value}'"""
        for product_type, events in get_segment_event_types_by_product().items()
    ]
    product_type_conditionals.append('ELSE "UNKNOWN_PRODUCT_TYPE"')
    product_type_query_fragment = (
        "CASE " + "\n".join(product_type_conditionals) + " END AS product_type,"
    )
    return product_type_query_fragment


# TODO(#46239): Move this helper into recidiviz/segment/segment_event_big_query_view_builder.py
#  once the product x event views no longer use it.
def build_segment_event_view_query_template(
    *,
    segment_table_sql_source: BigQueryAddress,
    segment_table_jii_pseudonymized_id_columns: list[str],
    additional_attribute_cols: list[str],
    # TODO(#46239): Remove this argument once the product x event views read directly
    #  from event_specific views and don't need to use the
    #  build_segment_event_view_query_template() helper.
    product_type_filter: ProductType | None,
) -> str:
    """Builds the SQL query template for a Segment event view by transforming
    hashed user and client id's into internal id's and pulling any additonal
    attribute columns from the sql source table. If product_type is provided,
    it will be used to filter the context_page_url to only include events from
    the specified product type."""

    if not additional_attribute_cols:
        additional_attribute_cols = []

    person_id_join_type = "LEFT"
    if segment_table_jii_pseudonymized_id_columns:
        # If JII ID columns are specified, only include events with non-null ID's
        person_id_join_type = "INNER"

    product_type_clause = _get_product_type_case_when_statement()

    template = f"""
SELECT
    state_code,
    user_id,
    LOWER(rdu.email) AS email,
    DATETIME(timestamp, "US/Eastern") AS event_ts,
    person_id,
    session_id,
    context_page_path,
    context_page_url,
    {product_type_clause if product_type_clause else ""}
    {list_to_query_string(additional_attribute_cols, table_prefix="events")}
FROM (
    SELECT
        -- default columns for all views
        -- this field was renamed, fall back to previous name for older records
        {f"COALESCE({list_to_query_string(segment_table_jii_pseudonymized_id_columns)})" 
            if segment_table_jii_pseudonymized_id_columns
            else "CAST(NULL AS STRING)"
        } AS pseudonymized_id,
        timestamp,
        session_id,
        user_id,
        context_page_path,
        context_page_url,
        "{segment_table_sql_source.table_id}" AS event,
        {list_to_query_string(additional_attribute_cols)}
    FROM
        `{{project_id}}.{segment_table_sql_source.to_str()}`
    -- events from prod deployment only
    WHERE
        --TODO(#43316): Adjust logic to support JII tablet events, and move this filtering
        -- logic to segment_event_big_query_view_builder based on ProductType url logic.
        context_page_url LIKE '%://dashboard.recidiviz.org/%'
    -- dedupes events loaded more than once
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY loaded_at DESC) = 1
) events
-- inner join to filter out recidiviz users and others unidentified (if any)
INNER JOIN
    `{{project_id}}.workflows_views.reidentified_dashboard_users_materialized` rdu
USING(user_id)
{person_id_join_type} JOIN
    `{{project_id}}.workflows_views.pseudonymized_id_to_person_id_materialized`
USING
    (state_code, pseudonymized_id)
-- We get the state_code above from `reidentified_dashboard_users`, which could have have an
-- entry for a user for both US_ID and US_IX. We can't use the pseudonymized id to distinguish
-- because they may match between both states. Instead, use the timestamp of the event to
-- determine whether it is a US_ID event or a US_IX event.
WHERE
    state_code != "US_ID" 
    OR timestamp < "{FIRST_IX_EXPORT_DATE}"
"""
    if product_type_filter:
        template = f"""
WITH all_events AS ({template})
SELECT * FROM all_events WHERE product_type = "{product_type_filter.value}"
"""
    return template
