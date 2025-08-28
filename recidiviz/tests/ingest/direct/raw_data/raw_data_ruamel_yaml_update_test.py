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
    get_raw_file_config_attribute_to_list_item_identifier_map,
    load_ruaml_yaml,
    update_ruamel_for_raw_file,
    update_ruamel_list,
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

    def test_update_ruamel_list_native_type(self) -> None:
        original_list = ["a", "b", "c"]
        updated_list = ["a", "c", "d"]
        ruamel_list = ["a", "b", "c"]
        update_ruamel_list(
            attribute_name="test_list",
            ruamel_list=ruamel_list,
            original_list=original_list,
            updated_list=updated_list,
            identifier_map={},
        )
        self.assertEqual(ruamel_list, ["a", "c", "d"])

    def test_update_ruamel_list_dict(self) -> None:
        original_list = [{"id": "1", "value": "a"}, {"id": "2", "value": "b"}]
        updated_list = [{"id": "1", "value": "a"}, {"id": "2", "value": "c"}]
        ruamel_list = [{"id": "1", "value": "'a'"}, {"id": "2", "value": "b"}]
        update_ruamel_list(
            attribute_name="test_list",
            ruamel_list=ruamel_list,
            original_list=original_list,
            updated_list=updated_list,
            identifier_map={"test_list": "id"},
        )
        self.assertEqual(
            # we should leave value "'a'" as-is since it didn't change between the original and updated lists
            ruamel_list,
            [{"id": "1", "value": "'a'"}, {"id": "2", "value": "c"}],
        )

    def test_nested_update(self) -> None:
        original_list = [
            {"id": "1", "value": ["a"], "extra": {"field": "value1"}},
            {"id": "2", "value": ["b"], "extra": {"field": "value2"}},
        ]
        updated_list = [
            {"id": "1", "value": ["a"], "extra": {"field": "value1"}},
            {"id": "2", "value": ["b", "c"], "extra": {"field": "value3"}},
        ]
        ruamel_list = [
            {"id": "1", "value": ["'a'"], "extra": {"field": "'value1'"}},
            {"id": "2", "value": ["'b'"], "extra": {"field": "'value2'"}},
        ]
        update_ruamel_list(
            attribute_name="test_list",
            ruamel_list=ruamel_list,
            original_list=original_list,
            updated_list=updated_list,
            identifier_map={"test_list": "id"},
        )
        self.assertEqual(
            ruamel_list,
            [
                {"id": "1", "value": ["'a'"], "extra": {"field": "'value1'"}},
                {"id": "2", "value": ["b", "c"], "extra": {"field": "value3"}},
            ],
        )

    def test_inserted_in_correct_order(self) -> None:
        original_list = [
            {"id": "0", "value": "z"},
            {"id": "1", "value": "a"},
            {"id": "3", "value": "c"},
        ]
        updated_list = [
            {"id": "1", "value": "a"},
            {"id": "2", "value": "b"},
            {"id": "2.5", "value": "bb"},
            {"id": "3", "value": "c"},
            {"id": "4", "value": "d"},
        ]
        ruamel_list = [
            {"id": "0", "value": "z"},
            {"id": "1", "value": "a"},
            {"id": "3", "value": "c"},
        ]
        update_ruamel_list(
            attribute_name="test_list",
            ruamel_list=ruamel_list,
            original_list=original_list,
            updated_list=updated_list,
            identifier_map={"test_list": "id"},
        )
        self.assertEqual(
            ruamel_list,
            [
                {"id": "1", "value": "a"},
                {"id": "2", "value": "b"},
                {"id": "2.5", "value": "bb"},
                {"id": "3", "value": "c"},
                {"id": "4", "value": "d"},
            ],
        )

    def test_get_raw_file_config_attribute_to_list_item_identifier_map(self) -> None:
        expected_dict = {
            "known_values": "value",
            "import_blocking_column_validation_exemptions": "validation_type",
            "update_history": "update_datetime",
            "import_blocking_validation_exemptions": "validation_type",
            "columns": "name",
        }

        result = get_raw_file_config_attribute_to_list_item_identifier_map()

        self.assertEqual(result, expected_dict)

    def test_list_many_deletes(self) -> None:
        original_list = [
            {"value": "0"},
            {"value": "-1"},
            {"value": "2"},
            {"value": "3"},
        ]
        updated_list = [{"value": "-1"}]
        ruamel_list = [{"value": "0"}, {"value": "-1"}, {"value": "2"}, {"value": "3"}]
        update_ruamel_list(
            attribute_name="test_list",
            ruamel_list=ruamel_list,
            original_list=original_list,
            updated_list=updated_list,
            identifier_map={"test_list": "value"},
        )
        self.assertEqual(ruamel_list, [{"value": "-1"}])
