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
from unittest.mock import patch

from recidiviz.common.constants.tenants import Tenant
from recidiviz.ingest.direct.feature_flags_registry import (
    all_feature_flag_names,
    is_identity_service_identity_enabled,
    resolve_ingest_feature_flags,
)
from recidiviz.tests.ingest.direct.activity_pipeline_identity_source_test import (
    IDENTITY_SERVICE_EXPORT_TENANTS_PATCH_TARGET,
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
                feature_flags = resolve_ingest_feature_flags(project_id, Tenant.US_XX)
                self.assertCountEqual(all_feature_flag_names(), feature_flags)
                for flag_name, value in feature_flags.items():
                    self.assertIsInstance(
                        value,
                        bool,
                        f"Feature flag [{flag_name}] returned a non-bool for "
                        f"project [{project_id}].",
                    )

    def test_existing_flags_unchanged_by_per_tenant_resolution(self) -> None:
        for project_id in [GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION]:
            with self.subTest(project_id=project_id):
                feature_flags = resolve_ingest_feature_flags(project_id, Tenant.US_XX)
                self.assertFalse(feature_flags["tomis_2_0_enabled"])
                self.assertFalse(feature_flags["coms_2026_enabled"])

    def test_identity_service_identity_disabled_for_all_tenants_today(self) -> None:
        """Guards the pre-flip state: no tenant's activity pipeline sources
        identity from the Identity Service export yet, in either project."""
        for project_id in [GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION]:
            for tenant in Tenant:
                with self.subTest(project_id=project_id, tenant=tenant):
                    self.assertFalse(
                        is_identity_service_identity_enabled(project_id, tenant)
                    )

    def test_identity_service_identity_enabled_resolves_per_tenant(self) -> None:
        with patch(
            IDENTITY_SERVICE_EXPORT_TENANTS_PATCH_TARGET,
            {
                GCP_PROJECT_STAGING: frozenset({Tenant.US_XX}),
                GCP_PROJECT_PRODUCTION: frozenset(),
            },
        ):
            self.assertTrue(
                is_identity_service_identity_enabled(GCP_PROJECT_STAGING, Tenant.US_XX)
            )
            # A different tenant in the same project stays off.
            self.assertFalse(
                is_identity_service_identity_enabled(GCP_PROJECT_STAGING, Tenant.US_YY)
            )
            # The same tenant in a project where it is not flipped stays off.
            self.assertFalse(
                is_identity_service_identity_enabled(
                    GCP_PROJECT_PRODUCTION, Tenant.US_XX
                )
            )
            # Unknown project ids resolve to False rather than raising.
            self.assertFalse(
                is_identity_service_identity_enabled("recidiviz-789", Tenant.US_XX)
            )
