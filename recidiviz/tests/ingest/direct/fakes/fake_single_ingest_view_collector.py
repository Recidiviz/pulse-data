# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Fake ingest view ViewCollector for tests."""

from typing import List, Optional

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestView,
)
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.utils.regions import Region


class _FakeDirectIngestViewBuilder(
    BigQueryViewBuilder[DirectIngestPreProcessedIngestView]
):
    """Fake BQ View Builder for tests."""

    def __init__(
        self,
        ingest_view_name: str,
        is_detect_row_deletion_view: bool = False,
        materialize_raw_data_table_views: bool = False,
    ):
        self.ingest_view_name = ingest_view_name
        self.is_detect_row_deletion_view = is_detect_row_deletion_view
        self.materialize_raw_data_table_views = materialize_raw_data_table_views

    # pylint: disable=unused-argument
    def _build(
        self, *, address_overrides: Optional[BigQueryAddressOverrides] = None
    ) -> DirectIngestPreProcessedIngestView:
        region_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx",
            region_module=fake_regions,
        )

        query = "select * from {file_tag_first} JOIN {tagFullHistoricalExport} USING (COL_1)"
        primary_key_tables_for_entity_deletion = (
            [] if not self.is_detect_row_deletion_view else ["tagFullHistoricalExport"]
        )
        return DirectIngestPreProcessedIngestView(
            ingest_view_name=self.ingest_view_name,
            view_query_template=query,
            region_raw_table_config=region_config,
            order_by_cols="colA, colC",
            is_detect_row_deletion_view=self.is_detect_row_deletion_view,
            primary_key_tables_for_entity_deletion=primary_key_tables_for_entity_deletion,
            materialize_raw_data_table_views=self.materialize_raw_data_table_views,
        )

    def build_and_print(self) -> None:
        self.build()


class FakeSingleIngestViewCollector(
    BigQueryViewCollector[_FakeDirectIngestViewBuilder]
):
    """Fake ingest view ViewCollector for tests that returns a single ingest view."""

    def __init__(
        self,
        region: Region,
        ingest_view_name: str,
        is_detect_row_deletion_view: bool,
        materialize_raw_data_table_views: bool,
    ):
        self.region = region
        self.ingest_view_name = ingest_view_name
        self.is_detect_row_deletion_view = is_detect_row_deletion_view
        self.materialize_raw_data_table_views = materialize_raw_data_table_views

    def collect_view_builders(self) -> List[_FakeDirectIngestViewBuilder]:
        builders = [
            _FakeDirectIngestViewBuilder(
                ingest_view_name=self.ingest_view_name,
                is_detect_row_deletion_view=self.is_detect_row_deletion_view,
                materialize_raw_data_table_views=self.materialize_raw_data_table_views,
            )
        ]

        return builders
