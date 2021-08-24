# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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

"""Class that parses ingest view file contents into entities based on the provided
yaml mappings file.
"""
import os
from typing import List

from recidiviz.cloud_storage.gcs_file_system import GcsfsFileContentsHandle
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.utils.regions import Region


def yaml_mappings_filepath(region: Region, file_tag: str) -> str:
    return os.path.join(
        os.path.dirname(region.region_module.__file__),
        region.region_code.lower(),
        f"{region.region_code.lower()}_{file_tag}.yaml",
    )


class IngestViewFileParser:
    def __init__(self, region: Region):
        self.region = region

    def parse(
        self,
        *,
        file_tag: str,
        contents_handle: GcsfsFileContentsHandle,
    ) -> List[Entity]:

        raise NotImplementedError("V2 direct ingest parsing not implemented")
