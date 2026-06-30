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
"""Enum identifying a kind of ingest pipeline and the per-region view /
mapping subdirectories it consumes."""
import enum

from recidiviz.pipelines.pipeline_names import (
    IDENTITY_INGEST_PIPELINE_NAME,
    INGEST_PIPELINE_NAME,
)


class IngestPipelineType(enum.Enum):
    """Identifies a kind of ingest pipeline.

    Each value also encodes the per-region subdirectory names that the
    pipeline's view query builders and manifest YAMLs live in (under
    `recidiviz/ingest/direct/regions/{region_code}/`).
    """

    ACTIVITY = "ACTIVITY"
    IDENTITY = "IDENTITY"

    @property
    def view_subdir_name(self) -> str:
        """Returns the subdirectory under a region directory where this
        pipeline's view query builder files (`view_*.py`) live."""
        return _PIPELINE_TYPE_TO_VIEW_SUBDIR[self]

    @property
    def manifest_subdir_name(self) -> str:
        """Returns the subdirectory under a region directory where this
        pipeline's manifest YAML files live."""
        return _PIPELINE_TYPE_TO_MANIFEST_SUBDIR[self]

    @property
    def pipeline_name(self) -> str:
        """Returns the registered pipeline name (as defined in
        `recidiviz/pipelines/pipeline_names.py`) for this pipeline type."""
        return _PIPELINE_TYPE_TO_PIPELINE_NAME[self]


_PIPELINE_TYPE_TO_VIEW_SUBDIR: dict[IngestPipelineType, str] = {
    IngestPipelineType.ACTIVITY: "ingest_views",
    IngestPipelineType.IDENTITY: "identity_views",
}

_PIPELINE_TYPE_TO_MANIFEST_SUBDIR: dict[IngestPipelineType, str] = {
    IngestPipelineType.ACTIVITY: "ingest_mappings",
    IngestPipelineType.IDENTITY: "identity_mappings",
}

_PIPELINE_TYPE_TO_PIPELINE_NAME: dict[IngestPipelineType, str] = {
    IngestPipelineType.ACTIVITY: INGEST_PIPELINE_NAME,
    IngestPipelineType.IDENTITY: IDENTITY_INGEST_PIPELINE_NAME,
}
