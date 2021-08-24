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
"""Tests the functions in the us_id_enum_helpers file."""
import unittest

from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.regions.us_id.us_id_controller import UsIdController
from recidiviz.tests.ingest.direct.fixture_util import direct_ingest_fixture_path


class TestUsIdEnumMappings(unittest.TestCase):
    """Tests the mapper functions from the us_id_enum_helpers file which map raw text
    to enums"""

    def setUp(self) -> None:
        self.region_code = StateCode.US_ID.value.lower()

    def test_supervision_termination_reason_mapper(self) -> None:
        fixture_path = direct_ingest_fixture_path(
            region_code=self.region_code,
            file_name="supervision_termination_reason_raw_text.csv",
        )
        enum_overrides = UsIdController.generate_enum_overrides()

        actual_mappings = set()
        with open(fixture_path, "r", encoding="utf-8") as f:
            while True:
                raw_text_value = f.readline().strip()
                if not raw_text_value:
                    break
                mapping = StateSupervisionPeriodTerminationReason.parse(
                    raw_text_value, enum_overrides
                )
                actual_mappings.add(mapping)

        expected_mappings = {
            StateSupervisionPeriodTerminationReason.ABSCONSION,
            StateSupervisionPeriodTerminationReason.COMMUTED,
            StateSupervisionPeriodTerminationReason.DEATH,
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            StateSupervisionPeriodTerminationReason.EXPIRATION,
            StateSupervisionPeriodTerminationReason.PARDONED,
            StateSupervisionPeriodTerminationReason.RETURN_FROM_ABSCONSION,
            StateSupervisionPeriodTerminationReason.RETURN_TO_INCARCERATION,
            StateSupervisionPeriodTerminationReason.TRANSFER_OUT_OF_STATE,
            StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
        }

        self.assertEqual(actual_mappings, expected_mappings)
