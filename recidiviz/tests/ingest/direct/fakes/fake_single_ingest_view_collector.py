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

from typing import List

from recidiviz.ingest.direct.direct_ingest_regions import DirectIngestRegion
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.tests.ingest.direct import fake_regions


class FakeSingleIngestViewCollector:
    """Fake ingest view ViewCollector for tests that returns a single ingest view."""

    def __init__(
        self,
        region: DirectIngestRegion,
        ingest_view_name: str,
        materialize_raw_data_table_views: bool,
    ):
        self.region = region
        self.ingest_view_name = ingest_view_name
        self.materialize_raw_data_table_views = materialize_raw_data_table_views

    def collect_query_builders(self) -> List[DirectIngestViewQueryBuilder]:
        query = "select * from {file_tag_first} JOIN {tagFullHistoricalExport} USING (COL_1)"

        builders = [
            DirectIngestViewQueryBuilder(
                ingest_view_name=self.ingest_view_name,
                view_query_template=query,
                region="us_xx",
                order_by_cols="colA, colC",
                region_module=fake_regions,
            )
        ]

        return builders
