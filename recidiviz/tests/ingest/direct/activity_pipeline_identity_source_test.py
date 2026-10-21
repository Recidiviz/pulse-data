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
"""Tests for activity_pipeline_identity_source."""
import unittest
from unittest.mock import patch

from recidiviz.common.constants.tenants import Tenant
from recidiviz.ingest.direct.activity_pipeline_identity_source import (
    _IDENTITY_SERVICE_EXPORT_TENANTS,
    ActivityPipelineIdentitySource,
    activity_pipeline_identity_source,
)
from recidiviz.ingest.direct.direct_ingest_regions import get_direct_ingest_region
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING

_IDENTITY_SERVICE_EXPORT_TENANTS_TARGET = (
    "recidiviz.ingest.direct.activity_pipeline_identity_source"
    "._IDENTITY_SERVICE_EXPORT_TENANTS"
)


class TestActivityPipelineIdentitySource(unittest.TestCase):
    """Tests for activity_pipeline_identity_source."""

    def test_tenant_not_in_export_tenants_resolves_to_legacy_clustering(self) -> None:
        self.assertIs(
            ActivityPipelineIdentitySource.LEGACY_CLUSTERING,
            activity_pipeline_identity_source(
                project_id=GCP_PROJECT_STAGING, tenant=Tenant.US_XX
            ),
        )
        self.assertIs(
            ActivityPipelineIdentitySource.LEGACY_CLUSTERING,
            activity_pipeline_identity_source(
                project_id=GCP_PROJECT_PRODUCTION, tenant=Tenant.US_XX
            ),
        )

    def test_unknown_project_resolves_to_legacy_clustering(self) -> None:
        self.assertIs(
            ActivityPipelineIdentitySource.LEGACY_CLUSTERING,
            activity_pipeline_identity_source(
                project_id="recidiviz-456", tenant=Tenant.US_XX
            ),
        )

    @patch.dict(
        _IDENTITY_SERVICE_EXPORT_TENANTS_TARGET,
        {GCP_PROJECT_STAGING: frozenset({Tenant.US_XX})},
    )
    def test_tenant_in_export_tenants_resolves_to_identity_service_export(self) -> None:
        self.assertIs(
            ActivityPipelineIdentitySource.IDENTITY_SERVICE_EXPORT,
            activity_pipeline_identity_source(
                project_id=GCP_PROJECT_STAGING, tenant=Tenant.US_XX
            ),
        )

    @patch.dict(
        _IDENTITY_SERVICE_EXPORT_TENANTS_TARGET,
        {GCP_PROJECT_STAGING: frozenset({Tenant.US_XX})},
    )
    def test_export_tenants_entry_does_not_apply_to_other_tenants(self) -> None:
        self.assertIs(
            ActivityPipelineIdentitySource.LEGACY_CLUSTERING,
            activity_pipeline_identity_source(
                project_id=GCP_PROJECT_STAGING, tenant=Tenant.US_YY
            ),
        )

    @patch.dict(
        _IDENTITY_SERVICE_EXPORT_TENANTS_TARGET,
        {GCP_PROJECT_STAGING: frozenset({Tenant.US_XX})},
    )
    def test_export_tenants_entry_does_not_apply_to_other_projects(self) -> None:
        self.assertIs(
            ActivityPipelineIdentitySource.LEGACY_CLUSTERING,
            activity_pipeline_identity_source(
                project_id=GCP_PROJECT_PRODUCTION, tenant=Tenant.US_XX
            ),
        )

    def test_export_tenants_have_launchable_identity_views_in_their_project(
        self,
    ) -> None:
        """Any tenant enrolled for the Identity Service export must be a state
        tenant whose identity ingest views are launchable in that project (the
        manifest.yaml flags that gate the identity ingest DAG): the export only
        ever has data for tenants whose identity pipeline runs there."""
        for project_id, tenants in _IDENTITY_SERVICE_EXPORT_TENANTS.items():
            for tenant in tenants:
                # Raises for non-state tenants (e.g. RECIDIVIZ), which never
                # run an activity pipeline.
                state_code = tenant.to_state_code()
                # Raises if the state has no real region directory.
                region = get_direct_ingest_region(state_code.value.lower())
                self.assertTrue(
                    region.has_launchable_identity_ingest_views(project_id=project_id),
                    f"Tenant [{tenant.value}] is enrolled for the Identity "
                    f"Service export in [{project_id}] but its identity ingest "
                    f"views are not launchable there. Flip the "
                    f"has_launchable_identity_ingest_views_* flag in the "
                    f"region's manifest.yaml first.",
                )
