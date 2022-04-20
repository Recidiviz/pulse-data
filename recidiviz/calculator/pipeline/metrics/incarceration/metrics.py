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
"""Incarceration metrics we calculate."""
import abc
from datetime import date
from typing import List, Optional

import attr

from recidiviz.calculator.pipeline.metrics.utils.metric_utils import (
    AssessmentMetricMixin,
    PersonLevelMetric,
    RecidivizMetric,
    RecidivizMetricType,
    SecondaryPersonExternalIdMetric,
)
from recidiviz.calculator.pipeline.utils.event_utils import (
    SupervisionLocationMixin,
    ViolationHistoryMixin,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
)


class IncarcerationMetricType(RecidivizMetricType):
    """The type of incarceration metrics."""

    INCARCERATION_ADMISSION = "INCARCERATION_ADMISSION"
    INCARCERATION_COMMITMENT_FROM_SUPERVISION = (
        "INCARCERATION_COMMITMENT_FROM_SUPERVISION"
    )
    INCARCERATION_POPULATION = "INCARCERATION_POPULATION"
    INCARCERATION_RELEASE = "INCARCERATION_RELEASE"


@attr.s
class IncarcerationMetric(
    RecidivizMetric[IncarcerationMetricType],
    PersonLevelMetric,
    SecondaryPersonExternalIdMetric,
):
    """Base model for incarceration metrics."""

    # Required characteristics
    metric_type_cls = IncarcerationMetricType

    # The type of IncarcerationMetric
    metric_type: IncarcerationMetricType = attr.ib(default=None)

    # Year
    year: int = attr.ib(default=None)

    # Month
    month: int = attr.ib(default=None)

    # Whether the period corresponding to the metric is counted in the state's population
    included_in_state_population: bool = attr.ib(default=True)

    # Optional characteristics

    # Facility
    facility: Optional[str] = attr.ib(default=None)

    # County of residence
    county_of_residence: Optional[str] = attr.ib(default=None)

    @classmethod
    @abc.abstractmethod
    def get_description(cls) -> str:
        """Should be implemented by metric subclasses to return a description of the metric."""


@attr.s
class IncarcerationPopulationMetric(IncarcerationMetric):
    """Subclass of IncarcerationMetric that contains incarceration population information on a given date."""

    @classmethod
    def get_description(cls) -> str:
        return """
The `IncarcerationPopulationMetric` stores information about a single day that an individual spent incarcerated. This metric tracks each day on which an individual was counted in the state’s incarcerated population, and includes information related to the stay in a facility.

With this metric, we can answer questions like:

- How has the population of a DOC facility changed over time?
- How many people are being held in a state prison for a parole board hold today?
- What proportion of individuals incarcerated in a state in 2010 were women?

This metric is derived from the `StateIncarcerationPeriod` entities, which store information about periods of time that an individual was in an incarceration facility. All population metrics are end date exclusive, meaning a person is not counted in a facility’s population on the day that they are released from that facility. The population metrics are start date inclusive, meaning that a person is counted in a facility’s population on the date that they are admitted to the facility.
"""

    # Required characteristics

    # The type of IncarcerationMetric
    metric_type: IncarcerationMetricType = attr.ib(
        init=False, default=IncarcerationMetricType.INCARCERATION_POPULATION
    )

    # Date of the incarceration population count
    date_of_stay: date = attr.ib(default=None)

    # Optional characteristics

    # The most recent "official" admission reason for this time of incarceration
    admission_reason: Optional[StateIncarcerationPeriodAdmissionReason] = attr.ib(
        default=None
    )

    # Raw text value of the most recent "official" admission reason for this time of incarceration
    admission_reason_raw_text: Optional[str] = attr.ib(default=None)

    # Supervision type at the time of commitment from supervision to incarceration if the most recent "official"
    # admission reason for this time of incarceration was a commitment from supervision.
    commitment_from_supervision_supervision_type: Optional[
        StateSupervisionPeriodSupervisionType
    ] = attr.ib(default=None)

    # Area of jurisdictional coverage of the court that sentenced the person to this incarceration
    judicial_district_code: Optional[str] = attr.ib(default=None)

    # TODO(#3275): Rename to purpose_for_incarceration
    # Specialized purpose for incarceration
    specialized_purpose_for_incarceration: Optional[
        StateSpecializedPurposeForIncarceration
    ] = attr.ib(default=None)

    # Custodial authority
    custodial_authority: Optional[StateCustodialAuthority] = attr.ib(default=None)


@attr.s
class IncarcerationAdmissionMetric(IncarcerationMetric):
    """Subclass of IncarcerationMetric that contains admission information."""

    @classmethod
    def get_description(cls) -> str:
        return """
The `IncarcerationAdmissionMetric` stores information about an admission to incarceration. This metric tracks each time that an individual was admitted to an incarceration facility, and includes information related to the admission.

With this metric, we can answer questions like:

- How many people were admitted to Facility X in January 2020?
- What percent of admissions to prison in 2017 were due to parole revocations?
- Of all admissions to prison due to a new court sentence in August 2014, what percent were by people who are Black?

This metric is derived from the `StateIncarcerationPeriod` entities, which store information about periods of time that an individual was in an incarceration facility. 

If a person was admitted to Facility X on 2021-01-01, was transferred out of Facility X and into Facility Z on 2021-03-31, and is still being held in Facility Z, then there will be a single `IncarcerationAdmissionMetric` for this person on 2021-01-01 into Facility X. Transfer admissions are not included in this metric.
"""

    # Required characteristics

    # The type of IncarcerationMetric
    metric_type: IncarcerationMetricType = attr.ib(
        init=False, default=IncarcerationMetricType.INCARCERATION_ADMISSION
    )

    # Most relevant admission reason for a continuous stay in prison. For example, in some states, if the initial
    # incarceration period has an admission reason of TEMPORARY_CUSTODY, the admission reason is drawn from the
    # subsequent admission period, if present.
    admission_reason: Optional[StateIncarcerationPeriodAdmissionReason] = attr.ib(
        default=None
    )

    # Admission reason raw text
    admission_reason_raw_text: Optional[str] = attr.ib(default=None)

    # TODO(#3275): Rename to purpose_for_incarceration
    # Specialized purpose for incarceration
    specialized_purpose_for_incarceration: Optional[
        StateSpecializedPurposeForIncarceration
    ] = attr.ib(default=None)

    # Admission date
    admission_date: date = attr.ib(default=None)

    # Type of supervision the person was committed from if the admission was a commitment from supervision
    supervision_type: Optional[StateSupervisionPeriodSupervisionType] = attr.ib(
        default=None
    )


@attr.s
class IncarcerationCommitmentFromSupervisionMetric(
    IncarcerationAdmissionMetric,
    SupervisionLocationMixin,
    AssessmentMetricMixin,
    ViolationHistoryMixin,
):
    """Subclass of IncarcerationAdmissionMetric for admissions to incarceration that
    qualify as a commitment from supervision. Tracks information about the supervision
    that preceded the admission to incarceration."""

    @classmethod
    def get_description(cls) -> str:
        return """
The `IncarcerationCommitmentFromSupervisionMetric` stores information about all admissions to incarceration that qualify as a commitment from supervision. A commitment from supervision is when an individual that is on supervision is admitted to prison in response to a mandate from either the court or the parole board. This includes an admission to prison from supervision for any of the following reasons:

- Revocation of any kind (ex: probation, parole, community corrections, dual, etc) to serve a full/remainder of a sentence in prison (`REVOCATION`)
- Treatment mandated by either the court or the parole board (`SANCTION_ADMISSION`)
- Shock incarceration mandated by either the court or the parole board (`SANCTION_ADMISSION`)

Admissions to temporary parole board holds are not considered commitments from supervision admissions. If a person enters a parole board hold and then has their parole revoked by the parole board, then there will be a `IncarcerationCommitmentFromSupervisionMetric` for a `REVOCATION` on the date of the revocation.

With this metric, we can answer questions like:

- For all probation revocation admissions in 2013, what percent were from individuals supervised by District X?
- How have the admissions for treatment mandated by the parole board changed over time for individuals with an `ALCOHOL_DRUG` supervision case type?
- What was the distribution of supervision levels for all of the people admitted to prison due to a probation revocation in 2020?

This metric is a subset of the `IncarcerationAdmissionMetric`. This means that every admission in the `IncarcerationAdmissionMetric` output that qualifies as a commitment from supervision admission has a corresponding entry in the `IncarcerationCommitmentFromSupervisionMetrics`. This metric is used to track information about the supervision that preceded the admission to incarceration, as well as other information related to the type of commitment from supervision the admission represents. 

If a person was admitted to Facility X on 2021-01-01 for a `REVOCATION` from parole, then there will be an `IncarcerationAdmissionMetric` for this person on 2021-01-01 into Facility X and an associated `IncarcerationCommitmentFromSupervisionMetric` for this person on 2021-01-01 that stores all of the supervision information related to this commitment from supervision admission. If a person enters a parole board hold in Facility X on 2021-01-01, and then has their parole revoked by the parole board on 2021-02-13, then there will be an `IncarcerationAdmissionMetric` for the admission to the `PAROLE_BOARD_HOLD` in Facility X on 2021-01-01, another `IncarcerationAdmissionMetric` for the `REVOCATION` admission to Facility X on 2021-02-13, and an associated `IncarcerationCommitmentFromSupervisionMetric` for the `REVOCATION` on 2021-02-13 that stores all of the supervision information related to this commitment from supervision admission. The `supervision_type` for all of these metrics would be `PAROLE`.
"""

    # The type of IncarcerationMetric
    metric_type: IncarcerationMetricType = attr.ib(
        init=False,
        default=IncarcerationMetricType.INCARCERATION_COMMITMENT_FROM_SUPERVISION,
    )

    # A string subtype to capture more information about the
    # specialized_purpose_for_incarceration, e.g. the length of stay for a
    # SHOCK_INCARCERATION admission
    purpose_for_incarceration_subtype: Optional[str] = attr.ib(default=None)

    # Type of supervision the person was committed from
    supervision_type: Optional[StateSupervisionPeriodSupervisionType] = attr.ib(
        default=None
    )

    # The type of supervision case
    case_type: Optional[StateSupervisionCaseType] = attr.ib(default=None)

    # Level of supervision
    supervision_level: Optional[StateSupervisionLevel] = attr.ib(default=None)

    # Raw text of the level of supervision
    supervision_level_raw_text: Optional[str] = attr.ib(default=None)

    # External ID of the officer who was supervising the person described by this
    # metric.
    supervising_officer_external_id: Optional[str] = attr.ib(default=None)

    # A string representation of the violations recorded in the period leading up to the
    # commitment to incarceration, which is the number of each of the represented types
    # separated by a semicolon
    violation_history_description: Optional[str] = attr.ib(default=None)

    # A list of a list of strings for each violation type and subtype recorded during
    # the period leading up to the commitment admission. The elements of the outer list
    # represent every StateSupervisionViolation that was reported in the period leading
    # up to the admission. Each inner list represents all of the violation types and
    # conditions that were listed on the given violation. For example, 3 violations may
    # be represented as: [['FELONY', 'TECHNICAL'], ['MISDEMEANOR'],
    # ['ABSCONDED', 'MUNICIPAL']]
    violation_type_frequency_counter: Optional[List[List[str]]] = attr.ib(default=None)

    # The most severe decision on the most recent response leading up to the commitment
    # admission
    most_recent_response_decision: Optional[
        StateSupervisionViolationResponseDecision
    ] = attr.ib(default=None)


@attr.s
class IncarcerationReleaseMetric(IncarcerationMetric):
    """Subclass of IncarcerationMetric that contains release information."""

    @classmethod
    def get_description(cls) -> str:
        return """
The `IncarcerationReleaseMetric` stores information about a release from incarceration. This metric tracks each time that an individual was released from an incarceration facility, and includes information related to the release.

With this metric, we can answer questions like:

- What was the most common release reason for all individuals released from prison in 2019?
- For all individuals released from Facility X in 2015, what was the average number of days each person spent incarcerated prior to their release?
- How did the number of releases per month change after the state implemented a new release policy?

This metric is derived from the `StateIncarcerationPeriod` entities, which store information about periods of time that an individual was in an incarceration facility.  

If a person was admitted to Facility X on 2021-01-01, was transferred out of Facility X and into Facility Z on 2021-03-31, and then was released from Facility Z on 2021-06-09, then there will be a single `IncarcerationReleaseMetric` for this person on 2021-06-09 from Facility Z. Transfer releases are not included in this metric.
"""

    # Required characteristics

    # The type of IncarcerationMetric
    metric_type: IncarcerationMetricType = attr.ib(
        init=False, default=IncarcerationMetricType.INCARCERATION_RELEASE
    )

    # Release date
    release_date: date = attr.ib(default=None)

    # Release reason
    release_reason: Optional[StateIncarcerationPeriodReleaseReason] = attr.ib(
        default=None
    )

    # Release reason raw text
    release_reason_raw_text: Optional[str] = attr.ib(default=None)

    # Type of incarceration the release was from
    purpose_for_incarceration: Optional[
        StateSpecializedPurposeForIncarceration
    ] = attr.ib(default=None)

    # Supervision type at the time of release, if any.
    supervision_type_at_release: Optional[
        StateSupervisionPeriodSupervisionType
    ] = attr.ib(default=None)

    # Most relevant admission reason for a continuous stay in prison. For example, in some states, if the initial
    # incarceration period has an admission reason of TEMPORARY_CUSTODY, the admission reason is drawn from the
    # subsequent admission period, if present.
    admission_reason: Optional[StateIncarcerationPeriodAdmissionReason] = attr.ib(
        default=None
    )

    # The length, in days, of the continuous stay in prison.
    total_days_incarcerated: Optional[int] = attr.ib(default=None)

    # Supervision type at the time of commitment from supervision to incarceration prior to release, if applicable.
    commitment_from_supervision_supervision_type: Optional[
        StateSupervisionPeriodSupervisionType
    ] = attr.ib(default=None)
