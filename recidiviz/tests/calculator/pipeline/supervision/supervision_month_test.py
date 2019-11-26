# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Tests for supervision/supervision_month.py."""

from recidiviz.calculator.pipeline.supervision.supervision_month import \
    RevocationReturnSupervisionMonth, NonRevocationReturnSupervisionMonth
from recidiviz.common.constants.state.state_supervision import \
    StateSupervisionType
from recidiviz.common.constants.state.state_supervision_violation import \
    StateSupervisionViolationType
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseRevocationType


def test_supervision_month():
    state_code = 'CA'
    year = 2000
    month = 11
    supervision_type = StateSupervisionType.PROBATION

    supervision_month = NonRevocationReturnSupervisionMonth(
        state_code, year, month, supervision_type)

    assert supervision_month.state_code == state_code
    assert supervision_month.year == year
    assert supervision_month.month == month
    assert supervision_month.supervision_type == supervision_type


def test_revocation_supervision_month():
    state_code = 'CA'
    year = 2000
    month = 11
    supervision_type = StateSupervisionType.PROBATION
    revocation_type = \
        StateSupervisionViolationResponseRevocationType.REINCARCERATION
    source_violation_type = StateSupervisionViolationType.TECHNICAL

    supervision_month = RevocationReturnSupervisionMonth(
        state_code, year, month, supervision_type, revocation_type,
        source_violation_type)

    assert supervision_month.state_code == state_code
    assert supervision_month.year == year
    assert supervision_month.month == month
    assert supervision_month.supervision_type == supervision_type
    assert supervision_month.revocation_type == revocation_type
    assert supervision_month.source_violation_type == source_violation_type


def test_non_revocation_supervision_month():
    state_code = 'UT'
    year = 1999
    month = 3

    supervision_month = NonRevocationReturnSupervisionMonth(
        state_code, year, month)

    assert state_code == supervision_month.state_code
    assert supervision_month.year == year
    assert supervision_month.month == month
    assert not isinstance(supervision_month, RevocationReturnSupervisionMonth)


def test_eq_different_field():
    state_code = 'CA'
    year = 2018
    month = 1
    supervision_type = StateSupervisionType.PROBATION
    revocation_type = \
        StateSupervisionViolationResponseRevocationType.REINCARCERATION
    source_violation_type = StateSupervisionViolationType.TECHNICAL

    first = RevocationReturnSupervisionMonth(
        state_code, year, month, supervision_type, revocation_type,
        source_violation_type)

    second = RevocationReturnSupervisionMonth(
        state_code, year, 4, supervision_type, revocation_type,
        source_violation_type)

    assert first != second


def test_eq_different_results():
    state_code = 'CA'
    year = 2003
    month = 12
    supervision_type = StateSupervisionType.PROBATION
    revocation_type = \
        StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION
    source_violation_type = StateSupervisionViolationType.MUNICIPAL

    first = RevocationReturnSupervisionMonth(
        state_code, year, month, supervision_type, revocation_type,
        source_violation_type)

    second = NonRevocationReturnSupervisionMonth(
        state_code, year, month, supervision_type)

    assert first != second


def test_eq_different_types():
    state_code = 'FL'
    year = 1992
    month = 9
    supervision_type = StateSupervisionType.PROBATION
    revocation_type = \
        StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION
    source_violation_type = StateSupervisionViolationType.MUNICIPAL

    supervision_month = RevocationReturnSupervisionMonth(
        state_code, year, month, supervision_type, revocation_type,
        source_violation_type)

    different = "Everything you do is a banana"

    assert supervision_month != different
