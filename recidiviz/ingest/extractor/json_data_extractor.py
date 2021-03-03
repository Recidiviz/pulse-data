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

"""A module which takes a JSON object or array and extracts from it key/value
pairs that a user might care about. The extracted information is put into the
ingest data model and returned.
"""
import logging
from collections import defaultdict
from typing import Union, Dict, List

from recidiviz.ingest.extractor.data_extractor import DataExtractor
from recidiviz.ingest.models.ingest_info import IngestInfo


class JsonDataExtractor(DataExtractor):
    """Data extractor for JSON files."""

    def extract_and_populate_data(
        self, content: Union[Dict, List], ingest_info: IngestInfo = None
    ):
        """This function does all the work of taking the users yaml file
        and content and returning a populated data class.  This function
        iterates through every field in the object and builds a model based on
        the keys that it sees.

        Args:
            content: An already parsed JSON object or array
            ingest_info: An IngestInfo object to use, if None we create a new
                one by default

        Returns:
            A populated ingest data model for a scrape.
        """
        if ingest_info is None:
            ingest_info = IngestInfo()
        self._extract(content, ingest_info, defaultdict(set))
        return ingest_info.prune()

    def _extract(self, content, ingest_info, seen_map, current_key=None):
        """Recursively walks |content| and adds data to |ingest_info|."""
        if isinstance(content, list):
            self._extract_list(content, ingest_info, seen_map, current_key)
        elif isinstance(content, dict):
            for k, v in content.items():
                lookup_key = "{}.{}".format(current_key, k) if current_key else k
                if v is None or isinstance(v, str):
                    self._set_value_if_key_exists(lookup_key, v, ingest_info, seen_map)
                elif isinstance(v, dict):
                    self._extract(v, ingest_info, seen_map, lookup_key)
                elif isinstance(v, list):
                    self._extract_list(v, ingest_info, seen_map, lookup_key)
                elif isinstance(v, (float, int)):
                    self._set_value_if_key_exists(
                        lookup_key, str(v), ingest_info, seen_map
                    )
                else:
                    logging.error(
                        "JSON value was not an object, array, int, "
                        "float, or string: %s",
                        v,
                    )
        else:
            logging.error("%s is not a valid JSON value", content)

    def _set_value_if_key_exists(self, lookup_key, value, ingest_info, seen_map):
        if lookup_key in self.keys:
            self._set_or_create_object(
                ingest_info, self.keys[lookup_key], [value], seen_map
            )

    def _extract_list(self, content, ingest_info, seen_map, current_key=None):
        for value in content:
            if isinstance(value, (list, dict)):
                self._extract(value, ingest_info, seen_map, current_key)
            else:
                self._extract({current_key: value}, ingest_info, seen_map)
