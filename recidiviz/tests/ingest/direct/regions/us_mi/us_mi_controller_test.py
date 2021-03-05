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
"""Unit and integration tests for US_MI direct ingest."""
from typing import Type

from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import (
    GcsfsDirectIngestController,
)
from recidiviz.ingest.direct.regions.us_mi.us_mi_controller import UsMiController
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.tests.ingest.direct.regions.base_direct_ingest_controller_tests import (
    BaseDirectIngestControllerTests,
)

_STATE_CODE_UPPER = "US_MI"


class TestUsMiController(BaseDirectIngestControllerTests):
    """Unit tests for each US_MI file to be ingested."""

    @classmethod
    def region_code(cls) -> str:
        return _STATE_CODE_UPPER.lower()

    @classmethod
    def controller_cls(cls) -> Type[GcsfsDirectIngestController]:
        return UsMiController

    @classmethod
    def schema_type(cls) -> SchemaType:
        return SchemaType.STATE
