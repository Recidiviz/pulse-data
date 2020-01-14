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

from recidiviz.calculator.pipeline.supervision.supervision_time_bucket import \
    RevocationReturnSupervisionTimeBucket, \
    NonRevocationReturnSupervisionTimeBucket
from recidiviz.common.constants.state.state_assessment import \
    StateAssessmentType
from recidiviz.common.constants.state.state_supervision import \
    StateSupervisionType
from recidiviz.common.constants.state.state_supervision_violation import \
    StateSupervisionViolationType
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseRevocationType


def test_supervision_time_bucket():
    state_code = 'CA'
    year = 2000
    month = 11
    supervision_type = StateSupervisionType.PROBATION

    supervision_time_bucket = NonRevocationReturnSupervisionTimeBucket(
        state_code, year, month, supervision_type)

    assert supervision_time_bucket.state_code == state_code
    assert supervision_time_bucket.year == year
    assert supervision_time_bucket.month == month
    assert supervision_time_bucket.supervision_type == supervision_type


def test_revocation_supervision_time_bucket():
    state_code = 'CA'
    year = 2000
    month = 11
    supervision_type = StateSupervisionType.PROBATION
    assessment_score = 22
    assessment_type = StateAssessmentType.LSIR
    supervising_officer_external_id = '13247'
    supervising_district_external_id = 'DISTRICT 3'
    revocation_type = \
        StateSupervisionViolationResponseRevocationType.REINCARCERATION
    source_violation_type = StateSupervisionViolationType.TECHNICAL

    supervision_time_bucket = RevocationReturnSupervisionTimeBucket(
        state_code, year, month, supervision_type,
        assessment_score, assessment_type, supervising_officer_external_id,
        supervising_district_external_id, revocation_type,
        source_violation_type)

    assert supervision_time_bucket.state_code == state_code
    assert supervision_time_bucket.year == year
    assert supervision_time_bucket.month == month
    assert supervision_time_bucket.supervision_type == supervision_type
    assert supervision_time_bucket.revocation_type == revocation_type
    assert supervision_time_bucket.source_violation_type == \
           source_violation_type


def test_non_revocation_supervision_time_bucket():
    state_code = 'UT'
    year = 1999
    month = 3

    supervision_time_bucket = NonRevocationReturnSupervisionTimeBucket(
        state_code, year, month)

    assert state_code == supervision_time_bucket.state_code
    assert supervision_time_bucket.year == year
    assert supervision_time_bucket.month == month
    assert not isinstance(supervision_time_bucket,
                          RevocationReturnSupervisionTimeBucket)


def test_supervision_time_bucket_year():
    state_code = 'CA'
    year = 2000
    month = None
    supervision_type = StateSupervisionType.PROBATION

    supervision_time_bucket = NonRevocationReturnSupervisionTimeBucket(
        state_code, year, month, supervision_type)

    assert supervision_time_bucket.state_code == state_code
    assert supervision_time_bucket.year == year
    assert supervision_time_bucket.month == month
    assert supervision_time_bucket.supervision_type == supervision_type


def test_eq_different_field():
    state_code = 'CA'
    year = 2018
    month = None
    supervision_type = StateSupervisionType.PROBATION

    first = RevocationReturnSupervisionTimeBucket(
        state_code, year, month, supervision_type)

    second = RevocationReturnSupervisionTimeBucket(
        state_code, year, 4, supervision_type)

    assert first != second


def test_eq_different_results():
    state_code = 'CA'
    year = 2003
    month = 12
    supervision_type = StateSupervisionType.PROBATION
    assessment_score = 19
    assessment_type = StateAssessmentType.ASI
    revocation_type = \
        StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION
    source_violation_type = StateSupervisionViolationType.MUNICIPAL

    first = RevocationReturnSupervisionTimeBucket(
        state_code, year, month, supervision_type,
        assessment_score, assessment_type,
        revocation_type,
        source_violation_type)

    second = NonRevocationReturnSupervisionTimeBucket(
        state_code, year, month, supervision_type)

    assert first != second


def test_eq_different_types():
    state_code = 'FL'
    year = 1992
    month = 9
    supervision_type = StateSupervisionType.PROBATION
    assessment_score = 11
    assessment_type = StateAssessmentType.LSIR
    revocation_type = \
        StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION
    source_violation_type = StateSupervisionViolationType.MUNICIPAL

    supervision_time_bucket = RevocationReturnSupervisionTimeBucket(
        state_code, year, month, supervision_type,
        assessment_score,
        assessment_type,
        revocation_type,
        source_violation_type)

    different = "Everything you do is a banana"

    assert supervision_time_bucket != different
