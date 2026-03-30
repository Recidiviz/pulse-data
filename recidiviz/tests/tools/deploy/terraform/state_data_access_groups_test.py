# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests that the state_data_access_groups.yaml Terraform config is in sync
with the set of direct ingest states."""
import os
import unittest

import recidiviz.big_query.config as _bq_config_pkg
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.utils.yaml_dict import YAMLDict

_CONFIG_PATH = os.path.join(
    os.path.dirname(_bq_config_pkg.__file__),
    "state_data_access_groups.yaml",
)


class StateDataAccessGroupsTest(unittest.TestCase):
    """Validates that the state_data_access_groups.yaml config file stays in
    sync with the set of existing direct ingest states."""

    def setUp(self) -> None:
        self.group_config: dict[str, str] = {
            k: str(v) for k, v in YAMLDict.from_path(_CONFIG_PATH).raw_yaml.items()
        }

    def test_keys_match_existing_states(self) -> None:
        """Every direct ingest state must have an entry in
        state_data_access_groups.yaml, and vice versa."""
        existing_states = {s.value for s in get_existing_direct_ingest_states()}
        config_states = set(self.group_config.keys())

        missing_from_config = existing_states - config_states
        extra_in_config = config_states - existing_states

        self.assertEqual(
            missing_from_config,
            set(),
            f"States missing from state_data_access_groups.yaml: "
            f"{sorted(missing_from_config)}. To fix:\n"
            f"1. Create a Google Group s-{{state}}-data@recidiviz.org in "
            f"Google Workspace admin (admin.google.com > Directory > Groups)\n"
            f"2. Get its resource name: gcloud identity groups describe "
            f'"s-{{state}}-data@recidiviz.org" --format="value(name)"\n'
            f"3. Add the state code and resource name to "
            f"recidiviz/big_query/config/"
            f"state_data_access_groups.yaml",
        )
        self.assertEqual(
            extra_in_config,
            set(),
            f"States in state_data_access_groups.yaml that are not existing "
            f"direct ingest states: {sorted(extra_in_config)}. Remove them "
            f"from the config.",
        )

    def test_group_resource_names_are_unique(self) -> None:
        """Each state must map to a unique group resource name."""
        seen: dict[str, str] = {}
        duplicates: list[str] = []
        for state, group_name in self.group_config.items():
            if group_name in seen:
                duplicates.append(
                    f"{state} and {seen[group_name]} both map to {group_name}"
                )
            seen[group_name] = state

        self.assertEqual(
            duplicates,
            [],
            f"Duplicate group resource names found: {duplicates}",
        )

    def test_group_resource_names_have_expected_format(self) -> None:
        """Group resource names should match the groups/<id> format."""
        for state, group_name in self.group_config.items():
            self.assertTrue(
                group_name.startswith("groups/"),
                f"{state} has invalid group resource name '{group_name}'. "
                f"Expected format: groups/<id>",
            )
