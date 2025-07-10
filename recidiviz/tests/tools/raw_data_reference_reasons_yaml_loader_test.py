# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests for raw_data_reference_reasons_yaml_loader.py."""
import unittest
from collections import defaultdict
from unittest.mock import mock_open, patch

from mock import MagicMock
from ruamel.yaml import YAML

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.tools.raw_data_reference_reasons_yaml_loader import (
    RawDataReferenceReasonsYamlLoader,
)

mock_yaml_content = """
US_XX:
  table1:
    dataset1.table1: |-
      Usage reason unknown.
    dataset2.table2: |-
      Usage reason unknown.
US_YY:
  table2:
    dataset3.table3: |-
      Usage reason unknown.
"""
mock_yaml_invalid_content = """
US_NOT_REAL:
    table1:
        dataset1.table1: |-
            Usage reason unknown.
"""
mock_raw_data = {
    "US_XX": {
        "table1": {
            "dataset1.table1": "Usage reason unknown.",
            "dataset2.table2": "Usage reason unknown.",
        }
    },
    "US_YY": {"table2": {"dataset3.table3": "Usage reason unknown."}},
}
mock_converted_data = {
    StateCode.US_XX: {
        "table1": {
            BigQueryAddress.from_str("dataset1.table1"),
            BigQueryAddress.from_str("dataset2.table2"),
        }
    },
    StateCode.US_YY: {"table2": {BigQueryAddress.from_str("dataset3.table3")}},
}


class TestRawDataReferenceReasonsYamlLoader(unittest.TestCase):
    """Test raw data reference reasons yaml loader."""

    def setUp(self) -> None:
        RawDataReferenceReasonsYamlLoader.reset_data()

    def tearDown(self) -> None:
        RawDataReferenceReasonsYamlLoader.reset_data()

    @patch("builtins.open", new_callable=mock_open, read_data=mock_yaml_content)
    @patch.object(
        YAML,
        "load",
        side_effect=ValueError("error parsing YAML"),
    )
    def test_load_yaml_failure(self, _1: MagicMock, _2: MagicMock) -> None:
        with self.assertRaises(RuntimeError):
            RawDataReferenceReasonsYamlLoader.get_yaml_data()
        with self.assertRaises(RuntimeError):
            RawDataReferenceReasonsYamlLoader.get_raw_yaml_data()

    @patch("builtins.open", new_callable=mock_open, read_data=mock_yaml_invalid_content)
    def test_parse_yaml_failure(self, _: MagicMock) -> None:
        with self.assertRaises(ValueError):
            RawDataReferenceReasonsYamlLoader.get_yaml_data()

    @patch("builtins.open", new_callable=mock_open, read_data=mock_yaml_content)
    def test_load_yaml(self, _: MagicMock) -> None:
        self.assertEqual(
            RawDataReferenceReasonsYamlLoader.get_yaml_data(), mock_converted_data
        )
        self.assertEqual(
            RawDataReferenceReasonsYamlLoader.get_raw_yaml_data(), mock_raw_data
        )

    @patch("builtins.open", new_callable=mock_open, read_data=mock_yaml_content)
    def test_get_downstream_referencing_views(self, _: MagicMock) -> None:
        result = RawDataReferenceReasonsYamlLoader.get_downstream_referencing_views(
            StateCode.US_XX
        )
        self.assertEqual(
            result,
            {
                "table1": {
                    BigQueryAddress.from_str("dataset1.table1"),
                    BigQueryAddress.from_str("dataset2.table2"),
                }
            },
        )

    @patch("builtins.open", new_callable=mock_open, read_data=mock_yaml_content)
    def test_get_downstream_referencing_views_invalid_state(self, _: MagicMock) -> None:
        result = RawDataReferenceReasonsYamlLoader.get_downstream_referencing_views(
            StateCode.US_WW
        )
        self.assertEqual(
            result,
            defaultdict(set),
        )
        self.assertEqual(result["non_existent_file_tag"], set())
