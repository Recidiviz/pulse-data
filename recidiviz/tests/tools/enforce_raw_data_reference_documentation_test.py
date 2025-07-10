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
import unittest
from typing import Any, Dict, List, Set, Tuple
from unittest.mock import patch

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.tools.alphabetize_raw_data_reference_reasons import is_sorted
from recidiviz.tools.find_direct_raw_data_references import (
    find_direct_raw_data_references,
)
from recidiviz.tools.raw_data_reference_reasons_yaml_loader import (
    RAW_DATA_REFERENCES_YAML,
    RawDataReferenceReasonsYamlLoader,
)
from recidiviz.view_registry.deployed_views import all_deployed_view_builders


class TestEnforceRawDataReferenceDocumentation(unittest.TestCase):
    """Tests for enforcing all deployed views that reference raw data tables/views
    directly are documented in {RAW_DATA_REFERENCES_YAML}
    and all views documented in {RAW_DATA_REFERENCES_YAML} are deployed.

    Generally, direct raw data references in our deployed views should be avoided - we
    should aim to capture all state-specific logic in ingest pipelines via ingest view
    queries / mappings or in normalization pipeline logic.
    """

    project_id_patcher: Any
    yaml_raw_data: Dict[str, Dict[str, Set[str]]]
    yaml_data: Dict[StateCode, Dict[str, Set[BigQueryAddress]]]
    deployed_views_references: Dict[StateCode, Dict[str, Set[BigQueryAddress]]]

    @classmethod
    def setUpClass(cls) -> None:
        cls.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        cls.project_id_patcher.start().return_value = "recidiviz-testing"
        cls.yaml_data = RawDataReferenceReasonsYamlLoader.get_yaml_data()
        cls.deployed_views_references = find_direct_raw_data_references(
            all_deployed_view_builders()
        )

    def tearDown(self) -> None:
        RawDataReferenceReasonsYamlLoader.reset_data()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.project_id_patcher.stop()

    def test_verify_yaml_entries_in_alphabetical_order(self) -> None:
        self.assertTrue(
            is_sorted(RawDataReferenceReasonsYamlLoader.get_raw_yaml_data()),
            f"Entries in {RAW_DATA_REFERENCES_YAML} must be in alphabetical order. "
            f"Please run `pipenv run python -m "
            f"recidiviz.tools.alphabetize_raw_data_reference_reasons` to sort the "
            f"entries.",
        )

    def test_find_direct_raw_data_references_missing_yaml_entries(self) -> None:
        missing_references = self._find_missing_references(
            self.deployed_views_references, self.yaml_data
        )
        if missing_references:
            self.fail(
                f"\nAll views that reference raw data tables/views directly must be "
                f"documented in {RAW_DATA_REFERENCES_YAML}. "
                "If this test is failing, you may need to add the following entries, "
                "along with an explanation for why you need to use the raw data "
                "directly instead of a state-agnostic dataset, to the yaml file:"
                f"\n\n{self._missing_references_to_str(missing_references)}"
            )

    def test_find_invalid_documented_raw_data_references(self) -> None:
        missing_references = self._find_missing_references(
            self.yaml_data, self.deployed_views_references
        )
        if missing_references:
            self.fail(
                f"Found raw data table references documented in "
                f"{RAW_DATA_REFERENCES_YAML} which no longer exist. You should remove "
                f"the following entries from the yaml file:"
                f"\n\n{self._missing_references_to_str(missing_references)}"
            )

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
