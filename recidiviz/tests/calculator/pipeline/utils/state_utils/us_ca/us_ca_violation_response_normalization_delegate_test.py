#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Tests us_ca_violation_response_normalization_delegate.py."""
import unittest

from recidiviz.calculator.pipeline.utils.state_utils.us_ca.us_ca_violation_response_normalization_delegate import (
    UsCaViolationResponseNormalizationDelegate,
)
from recidiviz.common.constants.states import StateCode

_STATE_CODE = StateCode.US_CA.value


class TestUsCaViolationResponseNormalizationDelegate(unittest.TestCase):
    """Tests functions in UsCaViolationResponseNormalizationDelegate."""

    def setUp(self) -> None:
        self.delegate = UsCaViolationResponseNormalizationDelegate()

    # ~~ Add new tests here ~~
