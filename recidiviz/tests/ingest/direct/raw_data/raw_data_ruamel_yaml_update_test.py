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
"""Unit tests for ruamel yaml update"""
import unittest

from recidiviz.ingest.direct.raw_data.raw_data_ruamel_yaml_update import (
    convert_raw_file_config_to_dict,
    load_ruaml_yaml,
    update_ruamel_for_raw_file,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.tests.ingest.direct import fake_regions as fake_regions_module


class TestYamlHelpers(unittest.TestCase):
    """tests for ruamel yaml update helpers"""

    def setUp(self) -> None:
        self.region_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx", region_module=fake_regions_module
        )
        self.default_region_config = self.region_config.default_config()
        self.file_tag = "file_tag_first"
        self.file_config = self.region_config.raw_file_configs[self.file_tag]

    def test_convert_raw_file_config_to_dict(self) -> None:
        result = convert_raw_file_config_to_dict(
            self.file_config, self.default_region_config
        )
        self.assertIsInstance(result, dict)
        self.assertIn("columns", result)
        for col in result["columns"]:
            self.assertIsInstance(col, dict)

    def test_update_ruamel_for_raw_file(self) -> None:
        original_dict = convert_raw_file_config_to_dict(
            self.file_config, self.default_region_config
        )
        updated_file_config = self.file_config
        updated_file_config.all_columns[0].description = "Updated description"
        updated_dict = convert_raw_file_config_to_dict(
            updated_file_config, self.default_region_config
        )
        ruamel_data = load_ruaml_yaml(self.file_config.file_path)
        updated_yaml = update_ruamel_for_raw_file(
            ruamel_yaml=ruamel_data,
            original_config_dict=original_dict,
            updated_config_dict=updated_dict,
        )
        self.assertEqual(
            updated_yaml["columns"][0]["description"],
            "Updated description",
        )


if __name__ == "__main__":
    unittest.main()
