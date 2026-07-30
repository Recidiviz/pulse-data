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
"""Tests for the ingest feature flag registry."""
import unittest

from recidiviz.ingest.direct.feature_flags_registry import (
    all_feature_flag_names,
    resolve_ingest_feature_flags,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.string_formatting import is_snake_case


class TestFeatureFlagsRegistry(unittest.TestCase):
    """Tests for the ingest feature flag registry."""

    def test_flag_names_well_formed(self) -> None:
        for flag_name in all_feature_flag_names():
            self.assertTrue(
                is_snake_case(flag_name),
                f"Feature flag [{flag_name}] must be a snake_case name.",
            )

    def test_all_feature_flag_names_sorted_and_unique(self) -> None:
        flag_names = all_feature_flag_names()
        self.assertCountEqual(set(flag_names), flag_names)
        self.assertEqual(sorted(flag_names), flag_names)

    def test_resolve_returns_bool_for_every_flag(self) -> None:
        # An unrecognized project id is a legitimate input - some tools and tests
        # build a context for a project that is neither staging nor production.
        for project_id in [
            GCP_PROJECT_STAGING,
            GCP_PROJECT_PRODUCTION,
            "recidiviz-789",
        ]:
            with self.subTest(project_id=project_id):
                feature_flags = resolve_ingest_feature_flags(project_id)
                self.assertCountEqual(all_feature_flag_names(), feature_flags)
                for flag_name, value in feature_flags.items():
                    self.assertIsInstance(
                        value,
                        bool,
                        f"Feature flag [{flag_name}] returned a non-bool for "
                        f"project [{project_id}].",
                    )
