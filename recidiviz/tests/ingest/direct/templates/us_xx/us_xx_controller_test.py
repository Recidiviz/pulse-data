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
from types import ModuleType
from typing import List, Optional, Type

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import templates
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.templates.us_xx.us_xx_controller import UsXxController
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.entity.state import entities
from recidiviz.tests.ingest.direct.regions.region_direct_ingest_controller_test_case import (
    RegionDirectIngestControllerTestCase,
)


class TestUsXxController(RegionDirectIngestControllerTestCase):
    """Unit tests for each US_XX file to be ingested."""

    @classmethod
    def state_code(cls) -> StateCode:
        return StateCode.US_XX

    @classmethod
    def controller_cls(cls) -> Type[BaseDirectIngestController]:
        return UsXxController

    @classmethod
    def region_module_override(cls) -> Optional[ModuleType]:
        return templates

    @classmethod
    def schema_type(cls) -> SchemaType:
        return SchemaType.STATE

    def test_run_full_ingest_all_files_specific_order(self) -> None:
        """Integration test that runs ingest end-to-end for all ingest views defined for
        US_XX.
        """

        # Build expected set of python entities throughout whole test
        expected_root_entities: List[entities.RootEntity] = []

        # NOTE: For each new ingest view, add a block like the one below at the end of
        # the test.

        ######################################
        # <ingest view name here>
        ######################################
        # Arrange

        # < Add to / update expected_root_entities here>

        self.run_test_state_pipeline({}, expected_root_entities)
