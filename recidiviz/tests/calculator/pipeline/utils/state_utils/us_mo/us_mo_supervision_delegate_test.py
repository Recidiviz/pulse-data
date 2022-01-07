# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Tests for the us_mo_supervision_delegate.py file"""
import unittest
from datetime import date

from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_supervision_delegate import (
    UsMoSupervisionDelegate,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod


class TestUsMoSupervisionDelegate(unittest.TestCase):
    """Unit tests for UsMoSupervisionDelegate"""

    def setUp(self) -> None:
        self.delegate = UsMoSupervisionDelegate()

    def test_get_supervising_officer_external_id_for_supervision_period(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            external_id="sp1",
            state_code="US_MO",
            start_date=date(2003, 3, 5),
            termination_date=date(2020, 10, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.PARDONED,
            supervision_type=None,
        )

        supervision_period_to_agent_associations = {
            1: {
                "agent_id": 111,
                "agent_external_id": "111",
                "supervision_period_id": 1,
                "agent_start_date": date(2003, 3, 5),
                "agent_end_date": date(2020, 10, 3),
            },
        }

        expected_agent_id = "111"

        results = (
            self.delegate.get_supervising_officer_external_id_for_supervision_period(
                supervision_period,
                supervision_period_to_agent_associations,
            )
        )
        self.assertEqual(expected_agent_id, results)

    def test_get_supervising_officer_external_id_for_supervision_period_overlapping_periods(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            external_id="sp1",
            state_code="US_MO",
            start_date=date(2003, 3, 5),
            termination_date=date(2020, 10, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.PARDONED,
            supervision_type=None,
        )

        supervision_period_to_agent_associations = {
            1: {
                "agent_id": 111,
                "agent_external_id": "111",
                "supervision_period_id": 1,
                "agent_start_date": date(2003, 3, 5),
                "agent_end_date": None,
            },
            2: {
                "agent_id": 222,
                "agent_external_id": "222",
                "supervision_period_id": 1,
                "agent_start_date": date(2020, 3, 5),
                "agent_end_date": None,
            },
        }

        expected_agent_id = "222"

        results = (
            self.delegate.get_supervising_officer_external_id_for_supervision_period(
                supervision_period,
                supervision_period_to_agent_associations,
            )
        )
        self.assertEqual(expected_agent_id, results)
