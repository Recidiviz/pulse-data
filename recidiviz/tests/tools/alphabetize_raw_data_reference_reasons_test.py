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
"""Tests for alphabetize_raw_data_reference_reasons.py."""
import unittest
from unittest.mock import mock_open, patch

from mock import MagicMock
from ruamel.yaml import YAML

from recidiviz.tools.alphabetize_raw_data_reference_reasons import sort_yaml_file


class TestAlphabetizeRawDataReferenceReasons(unittest.TestCase):
    """Test for alphabetizing RAW_DATA_REFERENCES_YAML file."""

    @patch("builtins.open", new_callable=mock_open, read_data="data")
    @patch(
        "recidiviz.tools.alphabetize_raw_data_reference_reasons.RawDataReferenceReasonsYamlLoader.get_raw_yaml_data"
    )
    @patch.object(
        YAML,
        "dump",
    )
    def test_sort_yaml_file_already_sorted(
        self,
        mock_yaml_dump: MagicMock,
        mock_get_raw_yaml_data: MagicMock,
        _mock_open: MagicMock,
    ) -> None:
        sorted_data = {
            "a": {"file2": {"addr": "test", "c_addr": "test"}},
            "b": {"file1": {"addr": "test", "b_addr": "test"}},
        }
        mock_get_raw_yaml_data.return_value = sorted_data

        sort_yaml_file()

        mock_yaml_dump.assert_not_called()

    @patch("builtins.open", new_callable=mock_open, read_data="data")
    @patch(
        "recidiviz.tools.alphabetize_raw_data_reference_reasons.RawDataReferenceReasonsYamlLoader.get_raw_yaml_data"
    )
    @patch.object(
        YAML,
        "dump",
    )
    def test_sort_yaml_file_unsorted(
        self,
        mock_yaml_dump: MagicMock,
        mock_get_raw_yaml_data: MagicMock,
        _mock_open: MagicMock,
    ) -> None:
        unsorted_data = {
            "b": {"file1": {"b_addr": "test", "addr": "test"}},
            "a": {"file2": {"c_addr": "test", "addr": "test"}},
        }
        mock_get_raw_yaml_data.return_value = unsorted_data

        sorted_data = {
            "a": {"file2": {"addr": "test", "c_addr": "test"}},
            "b": {"file1": {"addr": "test", "b_addr": "test"}},
        }

        sort_yaml_file()

        mock_yaml_dump.assert_called_once()
        args, _ = mock_yaml_dump.call_args
        self.assertEqual(args[0], sorted_data)
