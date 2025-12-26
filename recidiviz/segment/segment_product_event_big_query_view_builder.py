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
"""View builder that can be used to encode events tracked in Segment for a specific product type.
"""

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.segment.product_type import ProductType
from recidiviz.segment.segment_event_utils import (
    build_segment_event_view_query_template,
)


class SegmentProductEventBigQueryViewBuilder(SimpleBigQueryViewBuilder):
    """View builder that can be used to encode events tracked in Segment for a specific product type."""

    def __init__(
        self,
        # Description of the segment view
        description: str,
        # The product type associated with the segment events
        product_type: ProductType,
        # The source table of the segment events
        segment_table_sql_source: BigQueryAddress,
        # The ID column(s) if they exist in the sql source indicating the anonymized ID of the justice involved person
        # Multiple ID columns may be present in event tables where the name of column changed over time,
        # and some events may have no JII-identifying ID column.
        # If JII ID columns are specified, only events with non-null ID's will be included.
        segment_table_jii_pseudonymized_id_columns: list[str] | None = None,
        # Any additional attribute columns that should be included in the view
        additional_attribute_cols: list[str] | None = None,
        # Whether the source table has a session_id column
        has_session_id: bool = True,
        # Whether the source table has a user_id column (for staff users)
        has_user_id: bool = True,
    ) -> None:
        self.product_type = product_type
        self.product_name = product_type.pretty_name
        self.segment_table_sql_source = segment_table_sql_source
        self.segment_table_jii_pseudonymized_id_columns = (
            segment_table_jii_pseudonymized_id_columns
        )
        self.additional_attribute_cols = additional_attribute_cols
        self.segment_event_name = segment_table_sql_source.table_id
        self.has_session_id = has_session_id
        self.has_user_id = has_user_id

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
                has_session_id=has_session_id,
                has_user_id=has_user_id,
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
        has_session_id: bool = True,
        has_user_id: bool = True,
    ) -> str:
        """Builds the SQL query template for the Segment event view for a single product type
        by transforming hashed user and client id's into internal id's and pulling any additonal
        attribute columns from the sql source table."""

        return build_segment_event_view_query_template(
            segment_table_sql_source=segment_table_sql_source,
            product_type_filter=product_type,
            segment_table_jii_pseudonymized_id_columns=(
                segment_table_jii_pseudonymized_id_columns or []
            ),
            has_session_id=has_session_id,
            has_user_id=has_user_id,
            additional_attribute_cols=(additional_attribute_cols or []),
        )
