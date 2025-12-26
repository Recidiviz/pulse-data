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
from recidiviz.segment.segment_event_utils import (
    build_segment_event_view_query_template,
)


class SegmentEventBigQueryViewBuilder(SimpleBigQueryViewBuilder):
    """View builder that can be used to encode events tracked in Segment"""

    def __init__(
        self,
        # Description of the segment view
        description: str,
        # The address of the source table of the segment events
        segment_events_source_table_address: BigQueryAddress,
        # The ID column(s) if they exist in the sql source indicating the
        # anonymized (external_id, id_type) of the justice involved person. Multiple ID
        # columns may be present in event tables where the name of column changed over
        # time, and some events may have no JII-identifying ID column. If JII ID columns
        # are specified, only events with non-null ID's will be included.
        segment_table_jii_pseudonymized_id_columns: list[str],
        # Any additional attribute columns that should be included in the view
        additional_attribute_cols: list[str],
        # Whether the source table has a session_id column
        has_session_id: bool = True,
        # Whether the source table has a user_id column (for staff users)
        has_user_id: bool = True,
    ) -> None:
        self.segment_table_sql_source = segment_events_source_table_address
        self.segment_table_jii_pseudonymized_id_columns = (
            segment_table_jii_pseudonymized_id_columns
        )
        self.additional_attribute_cols = additional_attribute_cols
        self.segment_event_name = segment_events_source_table_address.table_id
        self.has_session_id = has_session_id
        self.has_user_id = has_user_id

        address = self.view_address(segment_events_source_table_address)
        super().__init__(
            dataset_id=address.dataset_id,
            view_id=address.table_id,
            description=description,
            view_query_template=self._build_query_template(
                segment_table_sql_source=segment_events_source_table_address,
                segment_table_jii_pseudonymized_id_columns=segment_table_jii_pseudonymized_id_columns,
                additional_attribute_cols=additional_attribute_cols,
                has_session_id=has_session_id,
                has_user_id=has_user_id,
            ),
            should_materialize=True,
            clustering_fields=["state_code", "user_id"],
        )

    @classmethod
    def view_address(cls, segment_table_sql_source: BigQueryAddress) -> BigQueryAddress:
        """Returns the BigQueryAddress for the view based on the product type and
        segment table SQL source.
        """
        return BigQueryAddress(
            dataset_id="segment_events",
            table_id=segment_table_sql_source.table_id,
        )

    @classmethod
    def _build_query_template(
        cls,
        segment_table_sql_source: BigQueryAddress,
        segment_table_jii_pseudonymized_id_columns: list[str],
        additional_attribute_cols: list[str],
        has_session_id: bool = True,
        has_user_id: bool = True,
    ) -> str:
        """Builds the SQL query template for the Segment event view for a single product type
        by transforming hashed user and client id's into internal id's and pulling any additonal
        attribute columns from the sql source table."""

        return build_segment_event_view_query_template(
            segment_table_sql_source=segment_table_sql_source,
            segment_table_jii_pseudonymized_id_columns=segment_table_jii_pseudonymized_id_columns,
            additional_attribute_cols=additional_attribute_cols,
            product_type_filter=None,
            has_session_id=has_session_id,
            has_user_id=has_user_id,
        )
