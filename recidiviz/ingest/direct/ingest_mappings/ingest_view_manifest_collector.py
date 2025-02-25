# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""The ingest view manifest collector pulls all of the ingest mappings based on the file
system."""
import os
import re
from typing import Dict, List

from recidiviz.common.file_system import is_valid_code_path
from recidiviz.ingest.direct.direct_ingest_regions import DirectIngestRegion
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler import (
    IngestViewManifest,
    IngestViewManifestCompiler,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    IngestViewManifestCompilerDelegate,
    ingest_view_manifest_dir,
)


class IngestViewManifestCollector:
    """Class that collects and generates manifests from all ingest mapping YAML files
    for a given region.
    """

    def __init__(
        self,
        region: DirectIngestRegion,
        delegate: IngestViewManifestCompilerDelegate,
    ) -> None:
        self.region = region
        self._manifest_compiler = IngestViewManifestCompiler(delegate)
        self._ingest_view_to_manifest_path: Dict[str, str] = {
            self._parse_ingest_view_name(manifest_path): manifest_path
            for manifest_path in self._get_manifest_paths(self.region)
        }
        self._ingest_view_to_manifest: Dict[str, IngestViewManifest] = {
            ingest_view_name: self._manifest_compiler.compile_manifest(
                ingest_view_name=ingest_view_name
            )
            for ingest_view_name in self._ingest_view_to_manifest_path.keys()
        }

    @property
    def ingest_view_to_manifest_path(self) -> Dict[str, str]:
        return self._ingest_view_to_manifest_path

    @property
    def ingest_view_to_manifest(self) -> Dict[str, IngestViewManifest]:
        return self._ingest_view_to_manifest

    def _parse_ingest_view_name(self, manifest_path: str) -> str:
        file_name = os.path.basename(manifest_path)
        regex = rf"{self.region.region_code.lower()}_(.+)\.yaml"
        match = re.match(regex, file_name)
        if not match:
            raise ValueError(
                "Manifest path does not match expected format. Expected format should be us_xx_ingest_view_name.yaml"
            )
        return match.group(1)

    @staticmethod
    def _get_manifest_paths(region: DirectIngestRegion) -> List[str]:
        manifest_dir = ingest_view_manifest_dir(region)

        result = []
        for file in os.listdir(manifest_dir):
            if file == "__init__.py" or not is_valid_code_path(file):
                continue
            manifest_path = os.path.join(manifest_dir, file)
            result.append(manifest_path)

        return result
