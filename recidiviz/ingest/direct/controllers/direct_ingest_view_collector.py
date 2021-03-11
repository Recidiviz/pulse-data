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
"""An implementation of the BigQueryViewCollector for collecting direct ingest SQL preprocessing views for a given
region.
"""

import os
from typing import List

import recidiviz
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.regions import Region

_INGEST_VIEWS_SUBDIR_NAME = "ingest_views"
_INGEST_VIEW_FILE_PREFIX = "view_"


class DirectIngestPreProcessedIngestViewCollector(
    BigQueryViewCollector[DirectIngestPreProcessedIngestViewBuilder]
):
    """An implementation of the BigQueryViewCollector for collecting direct ingest SQL preprocessing views for a given
    region.
    """

    def __init__(self, region: Region, controller_tag_rank_list: List[str]):
        self.region = region
        self.controller_tag_rank_list = controller_tag_rank_list

    def collect_view_builders(self) -> List[DirectIngestPreProcessedIngestViewBuilder]:
        relative_dir = os.path.relpath(
            os.path.dirname(self.region.region_module.__file__),
            os.path.dirname(recidiviz.__file__),
        )
        relative_dir_path = os.path.join(
            relative_dir, self.region.region_code, _INGEST_VIEWS_SUBDIR_NAME
        )

        ingest_view_builders = self.collect_view_builders_in_dir(
            DirectIngestPreProcessedIngestViewBuilder,
            relative_dir_path,
            _INGEST_VIEW_FILE_PREFIX,
        )

        self._validate_ingest_views(ingest_view_builders)

        return ingest_view_builders

    def _validate_ingest_views(
        self, ingest_view_builders: List[DirectIngestPreProcessedIngestViewBuilder]
    ) -> None:
        found_ingest_view_tags = [builder.file_tag for builder in ingest_view_builders]
        found_ingest_view_tags_set = set(found_ingest_view_tags)
        if len(found_ingest_view_tags) > len(found_ingest_view_tags_set):
            raise ValueError(
                f"Found duplicate tags in the view directory. Found: [{found_ingest_view_tags}]."
            )

        controller_view_tags_set = set(self.controller_tag_rank_list)
        controller_tags_no_view_defined = controller_view_tags_set.difference(
            found_ingest_view_tags_set
        )
        if self.region.is_ingest_launched_in_env() and controller_tags_no_view_defined:
            raise ValueError(
                f"Found controller file tags with no corresponding view defined: {controller_tags_no_view_defined}"
            )
