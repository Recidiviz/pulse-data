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
# pylint: disable=unused-import,wrong-import-order

"""Tests for supervision_type_identification.py."""

import unittest

import pytest

from recidiviz.calculator.pipeline.utils.supervision_type_identification import \
    _get_pre_incarceration_supervision_type_from_incarceration_period
from recidiviz.common.constants.state.state_incarceration_period import StateIncarcerationPeriodAdmissionReason
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodSupervisionType
from recidiviz.persistence.entity.state.entities import \
    StateIncarcerationPeriod


class TestSupervisionTypeIdentification(unittest.TestCase):

    def test_getPreIncarcerationSupervisionTypeFromIncarcerationPeriod_complete(self):
        for admission_reason in StateIncarcerationPeriodAdmissionReason:
            incarceration_period = StateIncarcerationPeriod.new_with_defaults(
                admission_reason=admission_reason)
            expected_type = None
            if admission_reason == StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION:
                expected_type = StateSupervisionPeriodSupervisionType.PROBATION
            elif admission_reason == StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION:
                expected_type = StateSupervisionPeriodSupervisionType.PAROLE
            elif admission_reason == StateIncarcerationPeriodAdmissionReason.DUAL_REVOCATION:
                expected_type = StateSupervisionPeriodSupervisionType.DUAL

            if admission_reason == StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY:
                with pytest.raises(ValueError):
                    _get_pre_incarceration_supervision_type_from_incarceration_period(incarceration_period)
            else:
                self.assertEqual(expected_type,
                                 _get_pre_incarceration_supervision_type_from_incarceration_period(
                                     incarceration_period))
