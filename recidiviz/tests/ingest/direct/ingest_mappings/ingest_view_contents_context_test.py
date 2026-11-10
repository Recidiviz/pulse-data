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
"""Tests for IngestViewContentsContext."""
import unittest
from unittest.mock import patch

from recidiviz.common.constants.states import StateCode
from recidiviz.common.constants.tenants import Tenant
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContext,
)
from recidiviz.tests.ingest.direct.activity_pipeline_identity_source_test import (
    IDENTITY_SERVICE_EXPORT_TENANTS_PATCH_TARGET,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING

_IDENTITY_SERVICE_IDENTITY_ENV_PROPERTY = "feature__identity_service_identity_enabled"


class TestIngestViewContentsContextFeatureFlags(unittest.TestCase):
    """Tests for how IngestViewContentsContext resolves and overrides feature
    flags."""

    def test_flags_resolve_per_tenant_in_same_project(self) -> None:
        with patch(
            IDENTITY_SERVICE_EXPORT_TENANTS_PATCH_TARGET,
            {
                GCP_PROJECT_STAGING: frozenset({Tenant.US_XX}),
                GCP_PROJECT_PRODUCTION: frozenset(),
            },
        ):
            flipped_tenant_context = IngestViewContentsContext.build_for_project(
                project_id=GCP_PROJECT_STAGING,
                is_sandbox=False,
                state_code=StateCode.US_XX,
            )
            self.assertTrue(
                flipped_tenant_context.get_env_property(
                    _IDENTITY_SERVICE_IDENTITY_ENV_PROPERTY
                )
            )

            unflipped_tenant_context = IngestViewContentsContext.build_for_project(
                project_id=GCP_PROJECT_STAGING,
                is_sandbox=False,
                state_code=StateCode.US_YY,
            )
            self.assertFalse(
                unflipped_tenant_context.get_env_property(
                    _IDENTITY_SERVICE_IDENTITY_ENV_PROPERTY
                )
            )

    def test_build_for_tests_resolves_per_tenant(self) -> None:
        with patch(
            IDENTITY_SERVICE_EXPORT_TENANTS_PATCH_TARGET,
            {
                GCP_PROJECT_STAGING: frozenset({Tenant.US_XX}),
                GCP_PROJECT_PRODUCTION: frozenset(),
            },
        ):
            context = IngestViewContentsContext.build_for_tests(
                state_code=StateCode.US_XX
            )
            self.assertTrue(
                context.get_env_property(_IDENTITY_SERVICE_IDENTITY_ENV_PROPERTY)
            )

            context = IngestViewContentsContext.build_for_tests(
                state_code=StateCode.US_XX, project_id=GCP_PROJECT_PRODUCTION
            )
            self.assertFalse(
                context.get_env_property(_IDENTITY_SERVICE_IDENTITY_ENV_PROPERTY)
            )
