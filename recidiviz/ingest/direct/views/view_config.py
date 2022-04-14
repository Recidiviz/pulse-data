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
"""Direct ingest metadata view configuration."""
import itertools
from typing import Sequence

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_region_dir_names,
)
from recidiviz.ingest.direct.views.direct_ingest_latest_view_collector import (
    DirectIngestRawDataTableLatestViewCollector,
)

DIRECT_INGEST_VIEW_BUILDERS: Sequence[BigQueryViewBuilder] = list(
    itertools.chain.from_iterable(
        # This returns a list of DirectIngestRawTableLatestViewBuilder, one per raw
        # table in all regions
        DirectIngestRawDataTableLatestViewCollector(
            region_code=region_code, src_raw_tables_sandbox_dataset_prefix=None
        ).collect_view_builders()
        for region_code in get_existing_region_dir_names()
    )
)

VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE: Sequence[
    BigQueryViewBuilder
] = DIRECT_INGEST_VIEW_BUILDERS
