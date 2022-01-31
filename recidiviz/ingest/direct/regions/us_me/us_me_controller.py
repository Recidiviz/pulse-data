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
"""Direct ingest controller implementation for US_ME."""
from typing import List

from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)


class UsMeController(BaseDirectIngestController):
    """Direct ingest controller implementation for US_ME."""

    @classmethod
    def region_code(cls) -> str:
        return StateCode.US_ME.value.lower()

    def __init__(self, ingest_bucket_path: GcsfsBucketPath):
        super().__init__(ingest_bucket_path)

    def get_file_tag_rank_list(self) -> List[str]:
        return ["CLIENT", "CURRENT_STATUS_incarceration_periods"]
