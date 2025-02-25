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
"""Ingest pipeline integration test(s) for US_XX."""
from types import ModuleType
from typing import Optional

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import templates
from recidiviz.tests.ingest.direct.regions.state_specific_ingest_pipeline_integration_test_case import (
    StateSpecificIngestPipelineIntegrationTestCase,
)


class UsXxPipelineIntegrationTest(StateSpecificIngestPipelineIntegrationTestCase):
    """Ingest pipeline integration test(s) for US_XX."""

    @classmethod
    def state_code(cls) -> StateCode:
        return StateCode.US_XX

    @classmethod
    def region_module_override(cls) -> Optional[ModuleType]:
        return templates

    def test_run_full_ingest_pipeline(self) -> None:
        """Integration test that runs an ingest pipeline end-to-end for all ingest
        views defined for US_XX.
        """

        # If your test is failing because you changed ingest logic and fixture files
        # containing expected results need to be upgraded, run this function with
        # `create_expected=True` to update the fixture files.
        self.run_test_state_pipeline()
