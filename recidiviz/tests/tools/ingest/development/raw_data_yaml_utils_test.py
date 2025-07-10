#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2025 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Unit tests for yaml_utils"""
import unittest
from functools import partial

import attr

from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.tests.ingest.direct import fake_regions as fake_regions_module
from recidiviz.tools.ingest.development.raw_data_yaml_utils import (
    raw_data_yaml_attribute_filter,
)


class TestYamlFilter(unittest.TestCase):
    """Test for raw_data_yaml_attribute_filter"""

    def test_yaml_filter(self) -> None:
        region_raw_file_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx", region_module=fake_regions_module
        )
        file_config = region_raw_file_config.raw_file_configs["file_tag_first"]

        attr_dict = attr.asdict(
            file_config,
            filter=partial(
                raw_data_yaml_attribute_filter,
                default_region_config=region_raw_file_config.default_config(),
            ),
            recurse=True,
        )

        # TODO(#45175) turn "_columns" into "columns"
        expected_keys = {
            "file_tag",
            "file_description",
            "data_classification",
            "primary_key_cols",
            "_columns",
            "encoding",
            "table_relationships",
        }

        expected_column_keys = [
            {
                "name",
                "description",
                "known_values",
                "import_blocking_column_validation_exemptions",
            },
            {"name", "description"},
            {"name"},
        ]

        self.assertEqual(set(attr_dict.keys()), expected_keys)
        for i, column in enumerate(attr_dict["_columns"]):
            self.assertEqual(set(column.keys()), expected_column_keys[i])
