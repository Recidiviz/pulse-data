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

"""A module which takes CSV text and extracts from it rows that a user
might care about. The extracted information is put into the ingest data
model and returned.
"""
import csv
import logging
from collections import defaultdict
from typing import Dict, Set

from recidiviz.ingest.extractor.data_extractor import DataExtractor
from recidiviz.ingest.models.ingest_info import IngestInfo


class CsvDataExtractor(DataExtractor):
    """Data extractor for CSV text."""

    def __init__(self, key_mapping_file=None):
        super().__init__(key_mapping_file)

        self.keys_to_ignore = self.manifest.get('keys_to_ignore', [])

        if not self.keys:
            self.keys = {}

        self.all_keys = set(self.keys.keys()) | set(self.keys_to_ignore)

    def extract_and_populate_data(self, content, ingest_info=None):
        """This function does all the work of taking the users yaml file
        and content and returning a populated data class.  This function
        iterates through every field in the object and builds a model based on
        the keys that it sees.

        Args:
            content: CSV-formatted text (string, not a file object)
            ingest_info: An IngestInfo object to use, if None we create a new
                one by default

        Returns:
            A populated ingest data model for a scrape.
        """
        if ingest_info is None:
            ingest_info = IngestInfo()
        self._extract(content, ingest_info)
        return ingest_info.prune()

    def _extract(self, content, ingest_info):
        """Converts entries in |content| and adds data to |ingest_info|."""
        if not isinstance(content, str):
            logging.error("%r is not a string", content)
            return

        rows = csv.DictReader(content.splitlines())
        for row in rows:
            seen_map: Dict[int, Set[str]] = defaultdict(set)
            ingest_info.create_person()
            for k, v in row.items():
                if k not in self.all_keys:
                    raise ValueError("Unmapped key: %s" % k)

                if not v:
                    continue
                self._set_value_if_key_exists(k, v, ingest_info, seen_map)

    def _set_value_if_key_exists(self, lookup_key, value, ingest_info,
                                 seen_map):
        if lookup_key in self.keys:
            self._set_or_create_object(ingest_info, self.keys[lookup_key],
                                       [value], seen_map)
