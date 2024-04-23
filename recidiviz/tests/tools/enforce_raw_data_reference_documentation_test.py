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
"""Tests for enforcing documentation of views that reference raw data."""
import os
import unittest
from typing import Any, Dict, List, Set, Tuple
from unittest.mock import patch

import yaml

import recidiviz
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.tools.find_direct_raw_data_references import (
    find_direct_raw_data_references,
)
from recidiviz.view_registry.deployed_views import all_deployed_view_builders

RAW_DATA_REFERENCES_YAML = "view_registry/raw_data_reference_reasons.yaml"
RAW_DATA_REFERENCES_YAML_PATH = os.path.join(
    os.path.dirname(recidiviz.__file__),
    RAW_DATA_REFERENCES_YAML,
)


class TestEnforceRawDataReferenceDocumentation(unittest.TestCase):
    """Tests for enforcing all deployed views that reference raw data tables/views directly are documented in {RAW_DATA_REFERENCES_YAML}
    and all views documented in {RAW_DATA_REFERENCES_YAML} are deployed."""

    project_id_patcher: Any
    yaml_raw_data: Dict[str, Dict[str, Set[str]]]
    yaml_data: Dict[StateCode, Dict[str, Set[BigQueryAddress]]]
    deployed_views_references: Dict[StateCode, Dict[str, Set[BigQueryAddress]]]

    @classmethod
    def setUpClass(cls) -> None:
        cls.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        cls.project_id_patcher.start().return_value = "recidiviz-testing"
        with open(RAW_DATA_REFERENCES_YAML_PATH, "r", encoding="utf-8") as yaml_file:
            cls.yaml_raw_data = yaml.safe_load(yaml_file)
            cls.yaml_data = (
                TestEnforceRawDataReferenceDocumentation._convert_raw_yaml_data_to_objs(
                    cls.yaml_raw_data
                )
            )
        cls.deployed_views_references = find_direct_raw_data_references(
            all_deployed_view_builders()
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.project_id_patcher.stop()

    def test_verify_yaml_entries_in_alphabetical_order(self) -> None:
        self.assertTrue(
            TestEnforceRawDataReferenceDocumentation._is_sorted(self.yaml_raw_data),
            f"Entries in {RAW_DATA_REFERENCES_YAML} must be in alphabetical order.",
        )

    def test_find_direct_raw_data_references_missing_yaml_entries(self) -> None:
        missing_references = self._find_missing_references(
            self.deployed_views_references, self.yaml_data
        )
        if missing_references:
            self.fail(
                f"\nAll views that reference raw data tables/views directly must be documented in {RAW_DATA_REFERENCES_YAML}. "
                "If this test is failing, you may need to add the following entries, "
                "along with an explanation for why you need to use the raw data directly instead of a state-agnostic dataset, "
                f"to the yaml file:\n\n{self._missing_references_to_str(missing_references)}"
            )

    def test_find_invalid_documented_raw_data_references(self) -> None:
        missing_references = self._find_missing_references(
            self.yaml_data, self.deployed_views_references
        )
        if missing_references:
            self.fail(
                f"Found raw data table references documented in {RAW_DATA_REFERENCES_YAML} which no longer exist. "
                f"You should remove the following entries from the yaml file:\n\n{self._missing_references_to_str(missing_references)}"
            )

    @staticmethod
    def _is_sorted(references: Dict[str, Dict[str, Set[str]]]) -> bool:
        state_codes = references.keys()
        if list(state_codes) != sorted(state_codes):
            return False
        for file_tag_to_addresses in references.values():
            file_tags = file_tag_to_addresses.keys()
            if list(file_tags) != sorted(file_tags):
                return False
            for addresses in file_tag_to_addresses.values():
                if list(addresses) != sorted(addresses):
                    return False
        return True

    @staticmethod
    def _missing_references_to_str(
        missing_references: List[Tuple[StateCode, str, BigQueryAddress]]
    ) -> str:
        return "\n".join(
            [
                f"    {state_code.value}: `{view.to_str()}` references `{file_tag}`"
                for state_code, file_tag, view in missing_references
            ]
        )

    @staticmethod
    def _find_missing_references(
        expected: Dict[StateCode, Dict[str, Set[BigQueryAddress]]],
        actual: Dict[StateCode, Dict[str, Set[BigQueryAddress]]],
    ) -> List[Tuple[StateCode, str, BigQueryAddress]]:
        return [
            (state, file_tag, view)
            for state, file_tags in expected.items()
            for file_tag, views in file_tags.items()
            for view in views
            if view not in actual.get(state, {}).get(file_tag, set())
        ]

    @staticmethod
    def _convert_raw_yaml_data_to_objs(
        references: Dict[str, Dict[str, Set[str]]]
    ) -> Dict[StateCode, Dict[str, Set[BigQueryAddress]]]:
        return {
            StateCode(state_code): {
                file_tag: {BigQueryAddress.from_str(view) for view in views}
                for file_tag, views in file_tags.items()
            }
            for state_code, file_tags in references.items()
        }
