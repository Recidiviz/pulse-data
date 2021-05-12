# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Defines subclasses of BigQueryView used in the direct ingest flow.

TODO(#6197): This file should be cleaned up after the new normalized views land.
"""

# pylint: disable=unused-import
from recidiviz.ingest.direct.views.unnormalized_direct_ingest_big_query_view_types import (
    DestinationTableType,
    RawTableViewType,
)


def should_use_normalized_direct_ingest_raw_table_query() -> bool:
    return True


if should_use_normalized_direct_ingest_raw_table_query():
    from recidiviz.ingest.direct.views.normalized_direct_ingest_big_query_view_types import (
        NormalizedDirectIngestRawDataTableBigQueryView as DirectIngestRawDataTableBigQueryView,
        NormalizedDirectIngestRawDataTableLatestView as DirectIngestRawDataTableLatestView,
        NormalizedDirectIngestRawDataTableUpToDateView as DirectIngestRawDataTableUpToDateView,
        NormalizedDirectIngestPreProcessedIngestView as DirectIngestPreProcessedIngestView,
        NormalizedDirectIngestPreProcessedIngestViewBuilder as DirectIngestPreProcessedIngestViewBuilder,
    )
else:
    from recidiviz.ingest.direct.views.unnormalized_direct_ingest_big_query_view_types import (  # type: ignore[misc]
        UnnormalizedDirectIngestRawDataTableBigQueryView as DirectIngestRawDataTableBigQueryView,
        UnnormalizedDirectIngestRawDataTableLatestView as DirectIngestRawDataTableLatestView,
        UnnormalizedDirectIngestRawDataTableUpToDateView as DirectIngestRawDataTableUpToDateView,
        UnnormalizedDirectIngestPreProcessedIngestView as DirectIngestPreProcessedIngestView,
        UnnormalizedDirectIngestPreProcessedIngestViewBuilder as DirectIngestPreProcessedIngestViewBuilder,
    )
