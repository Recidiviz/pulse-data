# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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

"""Functionality to perform direct ingest.
"""

import datetime
import json
import os

from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.extractor.json_data_extractor import JsonDataExtractor
from recidiviz.ingest.scrape import ingest_utils
from recidiviz.persistence import persistence
from recidiviz.utils import regions


class DirectIngestCollector:
    """Parses and persists individual-level info from direct ingest partners.
    """

    def __init__(self, region_name, yaml_file=None):
        """Initialize the collector.

        Args:
            region_name: (str) the name of the region to be collected.
            yaml_file: (str) path to the file containing data extractor
                mappings. If not set, it's assumed to be
                'regions/[region_name]/[region_name].yaml'
        """
        if not yaml_file:
            yaml_file = os.path.join(os.path.dirname(__file__), 'regions',
                                     region_name, region_name + '.yaml')

        self.region = regions.get_region(region_name, is_direct_ingest=True)
        self.yaml_file = yaml_file

    def parse(self, fp):
        """Currently assumes a JSON input file.
        """
        content = json.load(fp)
        extractor = JsonDataExtractor(self.yaml_file)
        ingest_info = extractor.extract_and_populate_data(content)

        return ingest_info

    def persist(self, ingest_info):
        # TODO #1739 consider using batch persistence here.

        ingest_info_proto = ingest_utils.convert_ingest_info_to_proto(
            ingest_info)

        metadata = IngestMetadata(self.region.region_code,
                                  self.region.jurisdiction_id,
                                  # TODO #1737 obtain from upload time
                                  datetime.datetime.now(),
                                  self.region.get_enum_overrides())

        return persistence.write(ingest_info_proto, metadata)
