# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Helpers for creating fake regions for use in tests."""
from types import ModuleType
from typing import Optional
from unittest.mock import create_autospec

from recidiviz.ingest.direct.direct_ingest_regions import DirectIngestRegion
from recidiviz.ingest.direct.types.ingest_pipeline_type import IngestPipelineType


def fake_region(
    *,
    region_code: str = "us_xx",
    environment: str = "local",
    region_module: Optional[ModuleType] = None,
    playground: bool = False,
    has_launchable_activity_ingest_views_in_staging: bool = True,
    has_launchable_activity_ingest_views_in_production: bool = True,
    has_launchable_identity_ingest_views_in_staging: bool = False,
    has_launchable_identity_ingest_views_in_production: bool = False,
) -> DirectIngestRegion:
    """Fake Region Object"""
    region = create_autospec(DirectIngestRegion)
    region.region_code = region_code
    region.environment = environment
    region.region_module = region_module
    region.playground = playground
    region.has_launchable_activity_ingest_views_in_staging = (
        has_launchable_activity_ingest_views_in_staging
    )
    region.has_launchable_activity_ingest_views_in_production = (
        has_launchable_activity_ingest_views_in_production
    )
    region.has_launchable_identity_ingest_views_in_staging = (
        has_launchable_identity_ingest_views_in_staging
    )
    region.has_launchable_identity_ingest_views_in_production = (
        has_launchable_identity_ingest_views_in_production
    )

    def fake_is_launched_in_env() -> bool:
        return DirectIngestRegion.is_ingest_launched_in_env(region)

    region.is_ingest_launched_in_env = fake_is_launched_in_env

    def fake_has_launchable_activity_ingest_views(project_id: str) -> bool:
        return DirectIngestRegion.has_launchable_activity_ingest_views(
            region, project_id
        )

    region.has_launchable_activity_ingest_views = (
        fake_has_launchable_activity_ingest_views
    )

    def fake_has_launchable_identity_ingest_views(project_id: str) -> bool:
        return DirectIngestRegion.has_launchable_identity_ingest_views(
            region, project_id
        )

    region.has_launchable_identity_ingest_views = (
        fake_has_launchable_identity_ingest_views
    )

    def fake_has_launchable_ingest_views_for_pipeline(
        pipeline_type: IngestPipelineType, project_id: str
    ) -> bool:
        return DirectIngestRegion.has_launchable_ingest_views_for_pipeline(
            region, pipeline_type, project_id
        )

    region.has_launchable_ingest_views_for_pipeline = (
        fake_has_launchable_ingest_views_for_pipeline
    )

    return region


TEST_STATE_REGION = fake_region(region_code="us_xx")
