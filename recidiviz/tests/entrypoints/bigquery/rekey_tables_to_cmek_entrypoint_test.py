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
"""Tests for the rekey_tables_to_cmek entrypoint's scoping."""
import unittest
from unittest.mock import patch

from recidiviz.entrypoints.bigquery import rekey_tables_to_cmek_entrypoint
from recidiviz.entrypoints.bigquery.rekey_tables_to_cmek_entrypoint import (
    _datasets_for_scope,
    _pilot_filtered,
)


class RekeyEntrypointScopingTest(unittest.TestCase):
    """The pilot allowlist must bound what any run can touch."""

    def test_pilot_allowlist_bounds_calculation_scope(self) -> None:
        datasets = _pilot_filtered(_datasets_for_scope("calculation_outputs"))
        self.assertEqual(["supplemental_data"], datasets)

    def test_removing_the_allowlist_widens_the_scope(self) -> None:
        with patch.object(
            rekey_tables_to_cmek_entrypoint, "_PILOT_DATASET_ALLOWLIST", None
        ):
            datasets = _pilot_filtered(_datasets_for_scope("calculation_outputs"))
        self.assertIn("dataflow_metrics", datasets)
        self.assertIn("supplemental_data", datasets)

    def test_unknown_scope_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, "Unknown scope"):
            _datasets_for_scope("everything")


if __name__ == "__main__":
    unittest.main()
