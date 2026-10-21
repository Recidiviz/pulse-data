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
)


class RekeyEntrypointScopingTest(unittest.TestCase):
    """The pilot allowlist must bound what any run can touch."""

    def test_pilot_allowlist_bounds_raw_data_scope(self) -> None:
        datasets = _datasets_for_scope("raw_data")
        self.assertEqual(
            [
                "us_ar_raw_data",
                "us_ar_raw_data_secondary",
                "us_ia_raw_data",
                "us_ia_raw_data_secondary",
                "us_ix_raw_data",
                "us_ix_raw_data_secondary",
                "us_me_raw_data",
                "us_me_raw_data_secondary",
                "us_mi_raw_data",
                "us_mi_raw_data_secondary",
                "us_mo_raw_data",
                "us_mo_raw_data_secondary",
                "us_ne_raw_data",
                "us_ne_raw_data_secondary",
                "us_or_raw_data",
                "us_or_raw_data_secondary",
                "us_tn_raw_data",
                "us_tn_raw_data_secondary",
            ],
            sorted(datasets),
        )

    def test_pilot_allowlist_covers_calculation_scope(self) -> None:
        datasets = _datasets_for_scope("calculation_outputs")
        self.assertEqual(["dataflow_metrics", "supplemental_data"], datasets[:2])
        ingest_outputs = datasets[2:]
        self.assertIn("us_me_state", ingest_outputs)
        self.assertIn("us_me_normalized_state", ingest_outputs)
        for dataset in ingest_outputs:
            self.assertRegex(dataset, r"^us_[a-z]+_(normalized_)?state$")

    def test_sandbox_and_raw_datasets_stay_out_of_calculation_scope(self) -> None:
        datasets = _datasets_for_scope("calculation_outputs")
        self.assertNotIn("us_me_raw_data", datasets)
        self.assertFalse(any(d.startswith("phenggeler_") for d in datasets))

    def test_removing_the_allowlist_widens_the_raw_data_scope(self) -> None:
        pinned = set(_datasets_for_scope("raw_data"))
        with patch.object(
            rekey_tables_to_cmek_entrypoint, "_PILOT_RAW_DATA_DATASET_ALLOWLIST", None
        ):
            widened = set(_datasets_for_scope("raw_data"))
        self.assertTrue(pinned < widened)
        for dataset in widened:
            self.assertRegex(dataset, r"^us_[a-z]+_raw_data(_secondary)?$")

    def test_calculation_scope_is_not_pilot_gated(self) -> None:
        with patch.object(
            rekey_tables_to_cmek_entrypoint,
            "_PILOT_RAW_DATA_DATASET_ALLOWLIST",
            frozenset(),
        ):
            datasets = _datasets_for_scope("calculation_outputs")
        self.assertEqual(["dataflow_metrics", "supplemental_data"], datasets[:2])
        self.assertIn("us_me_state", datasets)

    def test_unknown_scope_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, "Unknown scope"):
            _datasets_for_scope("everything")


if __name__ == "__main__":
    unittest.main()
