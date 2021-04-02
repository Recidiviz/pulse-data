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
"""Unit and integration tests for US_XX direct ingest."""
from typing import Type

from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.tests.ingest.direct.regions.base_direct_ingest_controller_tests import (
    BaseDirectIngestControllerTests,
)

_REGION_CODE_UPPER = "US_XX"


class TestUsXxController(BaseDirectIngestControllerTests):
    """Unit tests for each US_XX file to be ingested."""

    @classmethod
    def region_code(cls) -> str:
        return _REGION_CODE_UPPER.lower()

    @classmethod
    def controller_cls(cls) -> Type[BaseDirectIngestController]:
        pass  # Return instance of UsXxController

    @classmethod
    def schema_type(cls) -> SchemaType:
        raise NotImplementedError("Choose one of StateBase or JailsBase")
