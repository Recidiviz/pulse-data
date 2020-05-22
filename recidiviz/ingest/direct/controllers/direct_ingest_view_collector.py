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
from recidiviz.ingest.direct.controllers.direct_ingest_big_query_view_types import DirectIngestPreProcessedIngestView
from recidiviz.utils.regions import Region

_RELATIVE_REGIONS_DIR = os.path.relpath(os.path.dirname(recidiviz.ingest.direct.regions.__file__),
                                        os.path.dirname(recidiviz.__file__))
_INGEST_VIEWS_SUBDIR_NAME = 'ingest_views'
_INGEST_VIEW_FILE_PREFIX = 'view_'


class DirectIngestPreProcessedIngestViewCollector(BigQueryViewCollector[DirectIngestPreProcessedIngestView]):
    """An implementation of the BigQueryViewCollector for collecting direct ingest SQL preprocessing views for a given
    region.
    """
    def __init__(self,
                 region: Region,
                 controller_tag_rank_list: List[str]):
        self.region = region
        self.controller_tag_rank_list = controller_tag_rank_list

    def collect_views(self) -> List[DirectIngestPreProcessedIngestView]:
        ingest_views = self.collect_and_build_views_in_dir(
            DirectIngestPreProcessedIngestView,
            os.path.join(_RELATIVE_REGIONS_DIR, self.region.region_code, _INGEST_VIEWS_SUBDIR_NAME),
            _INGEST_VIEW_FILE_PREFIX
        )

        self._validate_ingest_views(ingest_views)

        return ingest_views

    def _validate_ingest_views(self, ingest_views: List[DirectIngestPreProcessedIngestView]):
        found_ingest_view_tags = [ingest_view.file_tag for ingest_view in ingest_views]
        found_ingest_view_tags_set = set(found_ingest_view_tags)
        if len(found_ingest_view_tags) > len(found_ingest_view_tags_set):
            raise ValueError(f'Found duplicate tags in the view directory. Found: [{found_ingest_view_tags}].')

        controller_view_tags_set = set(self.controller_tag_rank_list)
        controller_tags_no_view_defined = controller_view_tags_set.difference(found_ingest_view_tags_set)
        if self.region.are_ingest_view_exports_enabled_in_env() and controller_tags_no_view_defined:
            raise ValueError(
                f'Found controller file tags with no corresponding view defined: {controller_tags_no_view_defined}')

        ingest_views_defined_not_in_controller_list = found_ingest_view_tags_set.difference(controller_view_tags_set)
        if self.region.is_ingest_launched_in_production() and ingest_views_defined_not_in_controller_list:
            raise ValueError(f'Found ingest views defined for launched region that are not in the controller tags '
                             f'list: [{ingest_views_defined_not_in_controller_list}]')
