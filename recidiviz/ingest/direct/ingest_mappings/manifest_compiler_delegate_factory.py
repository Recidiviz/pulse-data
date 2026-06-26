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
"""Factory that maps an `IngestPipelineType` to the matching concrete
`IngestViewManifestCompilerDelegate` instance for a given region."""
from recidiviz.ingest.direct.direct_ingest_regions import DirectIngestRegion
from recidiviz.ingest.direct.ingest_mappings.activity_ingest_view_manifest_compiler_delegate import (
    ActivityIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.ingest_mappings.identity_ingest_view_manifest_compiler_delegate import (
    IdentityIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    IngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.types.ingest_pipeline_type import IngestPipelineType


def manifest_compiler_delegate_for_pipeline_type(
    *,
    region: DirectIngestRegion,
    ingest_pipeline_type: IngestPipelineType,
) -> IngestViewManifestCompilerDelegate:
    """Returns the `IngestViewManifestCompilerDelegate` that compiles manifests
    for the given pipeline type in the given region."""
    if ingest_pipeline_type is IngestPipelineType.ACTIVITY:
        return ActivityIngestViewManifestCompilerDelegate(region=region)
    if ingest_pipeline_type is IngestPipelineType.IDENTITY:
        return IdentityIngestViewManifestCompilerDelegate(region=region)
    raise ValueError(f"Unexpected ingest_pipeline_type: [{ingest_pipeline_type}]")
