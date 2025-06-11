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
"""View builder that can be used to encode events tracked in Segment
"""

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.segment.product_type import ProductType

# The first US_IX export for workflows was on 1/11 in staging and 1/17 in prod.
# For simplicity, use the prod date.
FIRST_IX_EXPORT_DATE = "2023-01-17"


class SegmentEventBigQueryViewBuilder(SimpleBigQueryViewBuilder):
    """View builder that can be used to encode events tracked in Segment"""

    def __init__(
        self,
        # Description of the segment view
        description: str,
        # The product type associated with the segment events
        product_type: ProductType,
        # The source table of the segment events
        segment_table_sql_source: BigQueryAddress,
        # The id column(s) if they exist in the sql source indicating the anonymized id of the justice involved person
        # Multiple id columns may be present in event tables where the name of column changed over time,
        # and some events may have no JII-identifying id column.
        segment_table_jii_pseudonymized_id_columns: list[str] | None = None,
        # Any additional attribute columns that should be included in the view
        additional_attribute_cols: list[str] | None = None,
    ) -> None:
        self.product_type = product_type
        self.product_name = product_type.pretty_name
        self.segment_table_sql_source = segment_table_sql_source
        self.segment_table_jii_pseudonymized_id_columns = (
            segment_table_jii_pseudonymized_id_columns
        )
        self.additional_attribute_cols = additional_attribute_cols
        self.segment_event_name = segment_table_sql_source.table_id

        address = self.view_address(product_type, segment_table_sql_source)
        super().__init__(
            dataset_id=address.dataset_id,
            view_id=address.table_id,
            description=description,
            view_query_template=self._build_query_template(
                product_type=product_type,
                segment_table_sql_source=segment_table_sql_source,
                segment_table_jii_pseudonymized_id_columns=segment_table_jii_pseudonymized_id_columns,
                additional_attribute_cols=additional_attribute_cols,
            ),
            should_materialize=True,
            clustering_fields=["state_code", "person_id"],
        )

    @classmethod
    def view_address(
        cls, product_type: ProductType, segment_table_sql_source: BigQueryAddress
    ) -> BigQueryAddress:
        """Returns the BigQueryAddress for the view based on the product type and
        segment table SQL source.
        """
        return BigQueryAddress(
            dataset_id=product_type.segment_dataset_name,
            table_id=segment_table_sql_source.table_id,
        )

    @classmethod
    def _build_query_template(
        cls,
        product_type: ProductType,
        segment_table_sql_source: BigQueryAddress,
        segment_table_jii_pseudonymized_id_columns: list[str] | None = None,
        additional_attribute_cols: list[str] | None = None,
    ) -> str:
        """Builds the SQL query template for the Segment event view by transforming
        hashed user and client id's into internal id's and pulling any additonal
        attribute columns from the sql source table."""

        if not additional_attribute_cols:
            additional_attribute_cols = []
        template = f"""
SELECT
    state_code,
    user_external_id AS user_id,
    LOWER(rdu.email) AS email,
    DATETIME(timestamp, "US/Eastern") AS event_ts,
    person_id,
    session_id,
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
        {list_to_query_string(additional_attribute_cols)}
    FROM
        `{{project_id}}.{segment_table_sql_source.to_str()}`
    -- events from prod deployment only
    WHERE
        context_page_path LIKE '%/{product_type.context_page_keyword}%' 
        --TODO(#43316): Adjust logic to support JII tablet events
        AND context_page_url LIKE '%://dashboard.recidiviz.org/%'
    -- dedupes events loaded more than once
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY loaded_at DESC) = 1
) events
-- inner join to filter out recidiviz users and others unidentified (if any)
INNER JOIN
    `{{project_id}}.workflows_views.reidentified_dashboard_users_materialized` rdu
ON
    events.user_id = rdu.user_id
    -- TODO(#43067): Remove this logic once historical views have been migrated to the current user_id format
    OR (STARTS_WITH(rdu.user_id, "_")
        AND STARTS_WITH(events.user_id, "/")
        AND SUBSTR(rdu.user_id, 2) = SUBSTR(events.user_id, 2))
LEFT JOIN
    `{{project_id}}.workflows_views.person_id_to_pseudonymized_id_materialized`
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
        return template
