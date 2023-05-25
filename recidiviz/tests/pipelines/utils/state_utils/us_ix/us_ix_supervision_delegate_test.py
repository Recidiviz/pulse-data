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
"""Tests for the us_ix_supervision_delegate.py file"""
import unittest
from datetime import date
from typing import Optional

from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.pipelines.metrics.supervision.events import SupervisionPopulationEvent
from recidiviz.pipelines.utils.state_utils.us_ix.us_ix_supervision_delegate import (
    UsIxSupervisionDelegate,
)


class TestUsIxSupervisionDelegate(unittest.TestCase):
    """Unit tests for UsIxSupervisionDelegate"""

    def setUp(self) -> None:
        self.supervision_delegate = UsIxSupervisionDelegate([], [])

    def test_supervision_period_is_out_of_state_with_identifier(self) -> None:
        self.assertTrue(
            self.supervision_delegate.is_supervision_location_out_of_state(
                "INTERSTATE PROBATION - remainder of identifier"
            )
        )

    def test_supervision_period_is_out_of_state_with_incorrect_identifier(
        self,
    ) -> None:
        self.assertFalse(
            self.supervision_delegate.is_supervision_location_out_of_state(
                "Incorrect - remainder of identifier"
            )
        )

    def test_supervision_period_is_out_of_state_with_empty_identifier(
        self,
    ) -> None:
        self.assertFalse(
            self.supervision_delegate.is_supervision_location_out_of_state(None)
        )

    def test_supervision_period_is_out_of_state_with_identifier_interstate(
        self,
    ) -> None:
        self.assertTrue(
            self.supervision_delegate.is_supervision_location_out_of_state(
                "INTERSTATE PROBATION - remainder of identifier"
            )
        )

    def test_supervision_period_is_out_of_state_with_identifier_parole(self) -> None:
        self.assertTrue(
            self.supervision_delegate.is_supervision_location_out_of_state(
                "PAROLE COMMISSION OFFICE - remainder of identifier"
            )
        )

    def test_supervision_period_is_out_of_state_with_partial_identifier(self) -> None:
        self.assertFalse(
            self.supervision_delegate.is_supervision_location_out_of_state(
                "INTERSTATE - remainder of identifier"
            )
        )

    def test_supervision_period_is_out_of_state_with_invalid_identifier(self) -> None:
        self.assertFalse(
            self.supervision_delegate.is_supervision_location_out_of_state("Invalid")
        )

    @staticmethod
    def create_population_event(
        supervising_district_external_id: Optional[str],
        custodial_authority: Optional[StateCustodialAuthority],
    ) -> SupervisionPopulationEvent:
        return SupervisionPopulationEvent(
            state_code="US_IX",
            year=2010,
            month=1,
            event_date=date(2010, 1, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervising_district_external_id=supervising_district_external_id,
            custodial_authority=custodial_authority,
            projected_end_date=None,
        )
