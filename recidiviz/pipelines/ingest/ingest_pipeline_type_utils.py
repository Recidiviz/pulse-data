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
# =============================================================================
"""Factories that map an `IngestPipelineType` to the type-specific entities
module and manifest compiler delegate used by that pipeline."""
from types import ModuleType

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
from recidiviz.persistence.entity.activity import entities as activity_entities_module
from recidiviz.persistence.entity.identity import (
    identity_fragment_entities as identity_fragment_entities_module,
)


def entities_module_for_pipeline_type(
    ingest_pipeline_type: IngestPipelineType,
) -> ModuleType:
    """Returns the entities module whose classes are produced by the given
    `ingest_pipeline_type`'s manifest compiler."""
    if ingest_pipeline_type is IngestPipelineType.ACTIVITY:
        return activity_entities_module
    if ingest_pipeline_type is IngestPipelineType.IDENTITY:
        return identity_fragment_entities_module
    raise ValueError(f"Unexpected ingest_pipeline_type: [{ingest_pipeline_type}]")


def manifest_compiler_delegate_for_pipeline_type(
    *,
    region: DirectIngestRegion,
    ingest_pipeline_type: IngestPipelineType,
) -> IngestViewManifestCompilerDelegate:
    """Returns the `IngestViewManifestCompilerDelegate` that compiles manifests
    for the given `ingest_pipeline_type` in the given `region`."""
    if ingest_pipeline_type is IngestPipelineType.ACTIVITY:
        return ActivityIngestViewManifestCompilerDelegate(region=region)
    if ingest_pipeline_type is IngestPipelineType.IDENTITY:
        return IdentityIngestViewManifestCompilerDelegate(region=region)
    raise ValueError(f"Unexpected ingest_pipeline_type: [{ingest_pipeline_type}]")
