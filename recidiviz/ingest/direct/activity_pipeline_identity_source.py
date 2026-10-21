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
"""Which system decides identity (keys and identity data) for a tenant's
activity pipeline.

This module must stay dependency-light (Tenant and environment constants only,
no pipeline imports) so modules like feature_flags_registry.py can read it.
Ingest mapping compilation pulls that registry into tools and servers far
outside pipeline code; co-locating this constant with IngestPipelineParameters
would drag apache_beam into all of them.
"""
from enum import Enum

from recidiviz.common.constants.tenants import Tenant
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING


# TODO(OBT-55069): Delete this module (and the LEGACY_CLUSTERING path it
# dispatches to) once every tenant resolves to IDENTITY_SERVICE_EXPORT in both
# projects.
class ActivityPipelineIdentitySource(Enum):
    """Which system decides identity (keys and identity data) for a tenant's
    activity pipeline."""

    LEGACY_CLUSTERING = "LEGACY_CLUSTERING"
    IDENTITY_SERVICE_EXPORT = "IDENTITY_SERVICE_EXPORT"


# Explicitly enumerated: a tenant appears here only once its activity pipeline
# uses the Identity Service export in the given project.
_IDENTITY_SERVICE_EXPORT_TENANTS: dict[str, frozenset[Tenant]] = {
    GCP_PROJECT_STAGING: frozenset(),
    GCP_PROJECT_PRODUCTION: frozenset(),
}


def activity_pipeline_identity_source(
    *, project_id: str, tenant: Tenant
) -> ActivityPipelineIdentitySource:
    """Returns which identity source the given tenant's activity pipeline uses
    in the given project. Project ids outside the deployed data-platform
    projects (test projects like recidiviz-456 and the emulator project)
    resolve to LEGACY_CLUSTERING rather than raising.
    """
    if tenant in _IDENTITY_SERVICE_EXPORT_TENANTS.get(project_id, frozenset()):
        return ActivityPipelineIdentitySource.IDENTITY_SERVICE_EXPORT
    return ActivityPipelineIdentitySource.LEGACY_CLUSTERING
