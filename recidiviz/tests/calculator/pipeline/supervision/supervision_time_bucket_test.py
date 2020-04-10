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
"""Tests for supervision/supervision_time_bucket.py."""
import unittest

from recidiviz.calculator.pipeline.supervision.supervision_time_bucket import \
    RevocationReturnSupervisionTimeBucket, \
    NonRevocationReturnSupervisionTimeBucket
from recidiviz.common.constants.state.state_assessment import \
    StateAssessmentType, StateAssessmentLevel
from recidiviz.common.constants.state.state_case_type import \
    StateSupervisionCaseType

from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodSupervisionType
from recidiviz.common.constants.state.state_supervision_violation import \
    StateSupervisionViolationType
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseRevocationType


class TestSupervisionTimeBucket(unittest.TestCase):
    """Unittests for supervision_time_bucket.py."""

    def test_supervision_time_bucket(self):
        state_code = 'CA'
        year = 2000
        month = 11
        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        supervision_time_bucket = NonRevocationReturnSupervisionTimeBucket(
            state_code, year, month, supervision_type, is_on_supervision_last_day_of_month=False)

        assert supervision_time_bucket.state_code == state_code
        assert supervision_time_bucket.year == year
        assert supervision_time_bucket.month == month
        assert supervision_time_bucket.supervision_type == supervision_type


    def test_revocation_supervision_time_bucket(self):
        state_code = 'CA'
        year = 2000
        month = 11
        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION
        case_type = StateSupervisionCaseType.SERIOUS_MENTAL_ILLNESS
        assessment_score = 22
        assessment_level = StateAssessmentLevel.MEDIUM
        assessment_type = StateAssessmentType.LSIR
        supervising_officer_external_id = '13247'
        supervising_district_external_id = 'DISTRICT 3'
        revocation_type = \
            StateSupervisionViolationResponseRevocationType.REINCARCERATION
        source_violation_type = StateSupervisionViolationType.TECHNICAL

        supervision_time_bucket = RevocationReturnSupervisionTimeBucket(
            state_code=state_code,
            year=year,
            month=month,
            supervision_type=supervision_type,
            case_type=case_type,
            assessment_score=assessment_score,
            assessment_level=assessment_level,
            assessment_type=assessment_type,
            supervising_officer_external_id=supervising_officer_external_id,
            supervising_district_external_id=supervising_district_external_id,
            revocation_type=revocation_type,
            source_violation_type=source_violation_type,
            is_on_supervision_last_day_of_month=False)

        assert supervision_time_bucket.state_code == state_code
        assert supervision_time_bucket.year == year
        assert supervision_time_bucket.month == month
        assert supervision_time_bucket.supervision_type == supervision_type
        assert supervision_time_bucket.case_type == case_type
        assert supervision_time_bucket.assessment_score == assessment_score
        assert supervision_time_bucket.assessment_level == assessment_level
        assert supervision_time_bucket.assessment_type == assessment_type
        assert supervision_time_bucket.revocation_type == revocation_type
        assert supervision_time_bucket.source_violation_type == \
            source_violation_type


    def test_non_revocation_supervision_time_bucket(self):
        state_code = 'UT'
        year = 1999
        month = 3

        supervision_time_bucket = NonRevocationReturnSupervisionTimeBucket(
            state_code, year, month, is_on_supervision_last_day_of_month=False)

        assert state_code == supervision_time_bucket.state_code
        assert supervision_time_bucket.year == year
        assert supervision_time_bucket.month == month
        assert not isinstance(supervision_time_bucket,
                              RevocationReturnSupervisionTimeBucket)


    def test_supervision_time_bucket_year(self):
        state_code = 'CA'
        year = 2000
        month = None
        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        supervision_time_bucket = NonRevocationReturnSupervisionTimeBucket(
            state_code, year, month, supervision_type, is_on_supervision_last_day_of_month=False)

        assert supervision_time_bucket.state_code == state_code
        assert supervision_time_bucket.year == year
        assert supervision_time_bucket.month == month
        assert supervision_time_bucket.supervision_type == supervision_type


    def test_eq_different_field(self):
        state_code = 'CA'
        year = 2018
        month = None
        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        first = RevocationReturnSupervisionTimeBucket(
            state_code, year, month, supervision_type, is_on_supervision_last_day_of_month=False)

        second = RevocationReturnSupervisionTimeBucket(
            state_code, year, 4, supervision_type, is_on_supervision_last_day_of_month=False)

        assert first != second


    def test_eq_different_results(self):
        state_code = 'CA'
        year = 2003
        month = 12
        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION
        case_type = StateSupervisionCaseType.DOMESTIC_VIOLENCE
        assessment_score = 19
        assessment_level = StateAssessmentLevel.MEDIUM
        assessment_type = StateAssessmentType.ASI
        revocation_type = \
            StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION
        source_violation_type = StateSupervisionViolationType.MUNICIPAL
        supervising_officer_id = 'XXX'
        supervising_district_id = 'DISTRICTX'

        first = RevocationReturnSupervisionTimeBucket(
            state_code=state_code,
            year=year,
            month=month,
            supervision_type=supervision_type,
            case_type=case_type,
            assessment_score=assessment_score,
            assessment_level=assessment_level,
            assessment_type=assessment_type,
            supervising_officer_external_id=supervising_officer_id,
            supervising_district_external_id=supervising_district_id,
            revocation_type=revocation_type,
            source_violation_type=source_violation_type,
            is_on_supervision_last_day_of_month=False)

        second = NonRevocationReturnSupervisionTimeBucket(
            state_code, year, month, supervision_type, is_on_supervision_last_day_of_month=False)

        assert first != second

    def test_eq_different_types(self):
        state_code = 'FL'
        year = 1992
        month = 9
        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION
        case_type = StateSupervisionCaseType.SEX_OFFENDER
        assessment_score = 11
        assessment_level = StateAssessmentLevel.LOW
        assessment_type = StateAssessmentType.LSIR
        revocation_type = \
            StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION
        source_violation_type = StateSupervisionViolationType.MUNICIPAL
        supervising_officer_id = 'XXX'
        supervising_district_id = 'SLKDJFLAK'

        supervision_time_bucket = RevocationReturnSupervisionTimeBucket(
            state_code=state_code,
            year=year,
            month=month,
            supervision_type=supervision_type,
            case_type=case_type,
            assessment_score=assessment_score,
            assessment_level=assessment_level,
            assessment_type=assessment_type,
            supervising_officer_external_id=supervising_officer_id,
            supervising_district_external_id=supervising_district_id,
            revocation_type=revocation_type,
            source_violation_type=source_violation_type,
            is_on_supervision_last_day_of_month=True)

        different = "Everything you do is a banana"

        assert supervision_time_bucket != different
