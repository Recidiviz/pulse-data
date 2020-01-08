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
"""Buckets of time on supervision that may have included a revocation."""
from typing import Optional

import attr

from recidiviz.common.attr_mixins import BuildableAttr
from recidiviz.common.constants.state.state_assessment import \
    StateAssessmentType
from recidiviz.common.constants.state.state_supervision import \
    StateSupervisionType
from recidiviz.common.constants.state.state_supervision_violation import \
    StateSupervisionViolationType
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseRevocationType


@attr.s(frozen=True)
class SupervisionTimeBucket(BuildableAttr):
    """Models details related to a bucket of time on supervision.

    Describes either a year or a month in which a person spent any amount of
    time on supervision. This includes the information pertaining to time on
    supervision that we will want to track when calculating supervision and
    revocation metrics."""

    # The state where the supervision took place
    state_code: str = attr.ib()

    # Year for when the person was on supervision
    year: int = attr.ib()

    # Month for when the person was on supervision
    month: Optional[int] = attr.ib()

    # The type of supervision the person was on
    supervision_type: Optional[StateSupervisionType] = attr.ib(default=None)

    # Most recent assessment score
    assessment_score: Optional[int] = attr.ib(default=None)

    # Type of the most recent assessment score
    assessment_type: Optional[StateAssessmentType] = attr.ib(default=None)


@attr.s(frozen=True)
class RevocationReturnSupervisionTimeBucket(SupervisionTimeBucket):
    """Models a SupervisionTimeBucket where the person was incarcerated for a
    revocation."""

    # The type of revocation of supervision
    revocation_type: Optional[StateSupervisionViolationResponseRevocationType] \
        = attr.ib(default=None)

    # StateSupervisionViolationType enum for the type of violation that
    # eventually caused the revocation of supervision
    source_violation_type: Optional[StateSupervisionViolationType] = \
        attr.ib(default=None)

    # External ID of the officer who was supervising the people described by
    # this metric
    supervising_officer_external_id: Optional[str] = attr.ib(default=None)

    # External ID of the district of the officer that was supervising the
    # people described by this metric
    supervising_district_external_id: Optional[str] = attr.ib(default=None)

    @staticmethod
    def for_year(state_code: str, year: int,
                 supervision_type: Optional[StateSupervisionType] = None,
                 assessment_score: Optional[int] = None,
                 assessment_type: Optional[StateAssessmentType] = None,
                 revocation_type:
                 Optional[StateSupervisionViolationResponseRevocationType] =
                 None,
                 source_violation_type:
                 Optional[StateSupervisionViolationType] = None,
                 supervising_officer_external_id: Optional[str] = None,
                 supervising_district_external_id: Optional[str] = None) -> \
            'RevocationReturnSupervisionTimeBucket':
        return RevocationReturnSupervisionTimeBucket(
            state_code=state_code,
            year=year,
            month=None,
            supervision_type=supervision_type,
            assessment_score=assessment_score,
            assessment_type=assessment_type,
            revocation_type=revocation_type,
            source_violation_type=source_violation_type,
            supervising_officer_external_id=supervising_officer_external_id,
            supervising_district_external_id=supervising_district_external_id

        )

    @staticmethod
    def for_year_from_month_assessment_override(
            month_bucket:
            'RevocationReturnSupervisionTimeBucket',
            assessment_score: Optional[int] = None,
            assessment_type: Optional[StateAssessmentType] = None) -> \
            'RevocationReturnSupervisionTimeBucket':
        return RevocationReturnSupervisionTimeBucket(
            state_code=month_bucket.state_code,
            year=month_bucket.year,
            month=None,
            supervision_type=month_bucket.supervision_type,
            assessment_score=assessment_score,
            assessment_type=assessment_type,
            revocation_type=month_bucket.revocation_type,
            source_violation_type=month_bucket.source_violation_type,
            supervising_officer_external_id=
            month_bucket.supervising_officer_external_id,
            supervising_district_external_id=
            month_bucket.supervising_district_external_id
        )

    @staticmethod
    def for_month(state_code: str, year: int, month: int,
                  supervision_type: Optional[StateSupervisionType] = None,
                  assessment_score: Optional[int] = None,
                  assessment_type: Optional[StateAssessmentType] = None,
                  revocation_type:
                  Optional[StateSupervisionViolationResponseRevocationType] =
                  None,
                  source_violation_type:
                  Optional[StateSupervisionViolationType] = None,
                  supervising_officer_external_id: Optional[str] = None,
                  supervising_district_external_id: Optional[str] = None) -> \
            'RevocationReturnSupervisionTimeBucket':
        return RevocationReturnSupervisionTimeBucket(
            state_code=state_code,
            year=year,
            month=month,
            supervision_type=supervision_type,
            assessment_score=assessment_score,
            assessment_type=assessment_type,
            revocation_type=revocation_type,
            source_violation_type=source_violation_type,
            supervising_officer_external_id=supervising_officer_external_id,
            supervising_district_external_id=supervising_district_external_id
        )


@attr.s(frozen=True)
class NonRevocationReturnSupervisionTimeBucket(SupervisionTimeBucket):
    """Models a SupervisionTimeBucket where the person was not incarcerated for
    a revocation."""

    @staticmethod
    def for_year(state_code: str, year: int,
                 supervision_type: Optional[StateSupervisionType] = None,
                 assessment_score: Optional[int] = None,
                 assessment_type: Optional[StateAssessmentType] = None
                 ) -> \
            'NonRevocationReturnSupervisionTimeBucket':
        return NonRevocationReturnSupervisionTimeBucket(
            state_code=state_code,
            year=year,
            month=None,
            supervision_type=supervision_type,
            assessment_score=assessment_score,
            assessment_type=assessment_type
        )

    @staticmethod
    def for_year_from_month_assessment_override(
            month_bucket:
            'NonRevocationReturnSupervisionTimeBucket',
            assessment_score: Optional[int] = None,
            assessment_type: Optional[StateAssessmentType] = None) -> \
            'NonRevocationReturnSupervisionTimeBucket':
        return NonRevocationReturnSupervisionTimeBucket(
            state_code=month_bucket.state_code,
            year=month_bucket.year,
            month=None,
            supervision_type=month_bucket.supervision_type,
            assessment_score=assessment_score,
            assessment_type=assessment_type
        )

    @staticmethod
    def for_month(state_code: str, year: int, month: int,
                  supervision_type: Optional[StateSupervisionType] = None,
                  assessment_score: Optional[int] = None,
                  assessment_type: Optional[StateAssessmentType] = None,
                  ) -> \
            'NonRevocationReturnSupervisionTimeBucket':
        return NonRevocationReturnSupervisionTimeBucket(
            state_code=state_code,
            year=year,
            month=month,
            supervision_type=supervision_type,
            assessment_score=assessment_score,
            assessment_type=assessment_type
        )


@attr.s(frozen=True)
class ProjectedSupervisionCompletionBucket(SupervisionTimeBucket):
    """Models a month and year in which supervision was projected to complete.

    Describes whether or not the supervision was successfully completed or not,
    as well as other details about the time on supervision.
    """
    # Whether or not the supervision was completed successfully
    successful_completion: bool = attr.ib(default=True)

    # External ID of the officer who was supervising the people described by
    # this metric
    supervising_officer_external_id: Optional[str] = attr.ib(default=None)

    # External ID of the district of the officer that was supervising the
    # people described by this metric
    supervising_district_external_id: Optional[str] = attr.ib(default=None)

    @staticmethod
    def for_month(state_code: str, year: int, month: int,
                  supervision_type: Optional[StateSupervisionType],
                  successful_completion: bool,
                  supervising_officer_external_id: Optional[str] = None,
                  supervising_district_external_id: Optional[str] = None) -> \
            'ProjectedSupervisionCompletionBucket':
        return ProjectedSupervisionCompletionBucket(
            state_code=state_code,
            year=year,
            month=month,
            supervision_type=supervision_type,
            successful_completion=successful_completion,
            supervising_officer_external_id=supervising_officer_external_id,
            supervising_district_external_id=supervising_district_external_id
        )
