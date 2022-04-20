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
"""Supervision metrics we calculate."""
import abc
from datetime import date
from typing import Optional

import attr

from recidiviz.calculator.pipeline.metrics.utils.metric_utils import (
    AssessmentMetricMixin,
    PersonLevelMetric,
    RecidivizMetric,
    RecidivizMetricType,
)
from recidiviz.calculator.pipeline.utils.event_utils import (
    InPopulationMixin,
    SupervisionLocationMixin,
    ViolationHistoryMixin,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)


class SupervisionMetricType(RecidivizMetricType):
    """The type of supervision metrics."""

    SUPERVISION_COMPLIANCE = "SUPERVISION_COMPLIANCE"
    SUPERVISION_POPULATION = "SUPERVISION_POPULATION"
    SUPERVISION_OUT_OF_STATE_POPULATION = "SUPERVISION_OUT_OF_STATE_POPULATION"
    SUPERVISION_START = "SUPERVISION_START"
    SUPERVISION_SUCCESS = "SUPERVISION_SUCCESS"
    SUPERVISION_TERMINATION = "SUPERVISION_TERMINATION"
    SUPERVISION_DOWNGRADE = "SUPERVISION_DOWNGRADE"


@attr.s
class SupervisionMetric(
    RecidivizMetric[SupervisionMetricType], SupervisionLocationMixin, PersonLevelMetric
):
    """Models a single supervision metric.

    Contains all of the identifying characteristics of the metric, including required characteristics for
    normalization as well as optional characteristics for slicing the data.
    """

    # Required characteristics
    metric_type_cls = SupervisionMetricType

    # The type of SupervisionMetric
    metric_type: SupervisionMetricType = attr.ib(default=None)

    # Year
    year: int = attr.ib(default=None)

    # Month
    month: int = attr.ib(default=None)

    # Optional characteristics

    # Supervision Type
    supervision_type: Optional[StateSupervisionPeriodSupervisionType] = attr.ib(
        default=None
    )

    # The type of supervision case
    case_type: Optional[StateSupervisionCaseType] = attr.ib(default=None)

    # Level of supervision
    supervision_level: Optional[StateSupervisionLevel] = attr.ib(default=None)

    # Raw text of the level of supervision
    supervision_level_raw_text: Optional[str] = attr.ib(default=None)

    # External ID of the officer who was supervising the person described by this metric.
    supervising_officer_external_id: Optional[str] = attr.ib(default=None)

    # Area of jurisdictional coverage of the court that sentenced the person to this supervision
    judicial_district_code: Optional[str] = attr.ib(default=None)

    # The type of government entity that has responsibility for this period of supervision
    custodial_authority: Optional[StateCustodialAuthority] = attr.ib(default=None)

    @classmethod
    @abc.abstractmethod
    def get_description(cls) -> str:
        """Should be implemented by metric subclasses to return a description of the metric."""


@attr.s
class SupervisionPopulationMetric(
    SupervisionMetric, ViolationHistoryMixin, AssessmentMetricMixin
):
    """Subclass of SupervisionMetric that contains supervision population information."""

    @classmethod
    def get_description(cls) -> str:
        return """
The `SupervisionPopulationMetric` stores information about a single day that an individual spent on supervision. This metric tracks each day on which an individual was counted in the state’s supervision population, and includes information related to the period of supervision.

With this metric, we can answer questions like:

- How has the state’s probation population changed over time?
- How many people are actively being supervised by District X today?
- What proportion of individuals on parole in the state in 2010 were over the age of 60?

This metric is derived from the `StateSupervisionPeriod` entities, which store information about periods of time that an individual was supervised. All population metrics are end date exclusive, meaning a person is not counted in the supervision population on the day that their supervision is terminated. The population metrics are start date inclusive, meaning that a person is counted in the population on the date that they start supervision.

A person is excluded from the supervision population on any days that they are also incarcerated, unless that incarceration is under the custodial authority of the supervision authority in the state. For example, say a person is incarcerated on 2021-01-01, where the custodial authority of the incarceration is the `STATE_PRISON`. If this person is simultaneously serving a probation term while they are incarcerated, they will not be counted in the supervision population because they are in prison under the state prison custodial authority. However, if instead the custodial authority of this incarceration is the `SUPERVISION_AUTHORITY` of the state, then this incarceration will not exclude the person from being counted in the supervision population on this day. 
"""

    # Required characteristics

    # The type of SupervisionMetric
    metric_type: SupervisionMetricType = attr.ib(
        init=False, default=SupervisionMetricType.SUPERVISION_POPULATION
    )

    # Date of the supervision population count
    date_of_supervision: date = attr.ib(default=None)

    # Optional characteristics

    # The projected end date for the person's supervision term.
    projected_end_date: Optional[date] = attr.ib(default=None)


@attr.s
class SupervisionOutOfStatePopulationMetric(SupervisionPopulationMetric):
    """Subclass of SupervisionPopulationMetric that contains supervision information for
    people who are serving their supervisions in another state."""

    @classmethod
    def get_description(cls) -> str:
        return """
The `SupervisionOutOfStatePopulationMetric` stores information about a single day that an individual spent on supervision, where the person is serving their supervision in another state. This metric tracks each day on which an individual was counted in the state’s “out of state” supervision population, and includes information related to the period of supervision. 

With this metric, we can answer questions like:

- How many people are actively serving their US_XX supervision in a state other than US_XX?
- How has the number of people serving probation in another state changed over time for state US_YY?
- What proportion of individuals serving parole in another state in 2010 were men?

This metric is mutually exclusive from the `SupervisionPopulationMetric`. If a person is in the `SupervisionOutOfStatePopulationMetric` on a given day then they will not be counted in the `SupervisionPopulationMetric` for that day.

This metric is derived from the `StateSupervisionPeriod` entities, which store information about periods of time that an individual was supervised. All population metrics are end date exclusive, meaning a person is not counted in the supervision population on the day that their supervision is terminated. The population metrics are start date inclusive, meaning that a person is counted in the population on the date that they start supervision.

A person is excluded from the out of state supervision population on any days that they are also incarcerated, unless that incarceration is under the custodial authority of the supervision authority in the state. For example, say a person is incarcerated on 2021-01-01, where the custodial authority of the incarceration is the `STATE_PRISON`. If this person is simultaneously serving a probation term (that they have been serving while out of the state) while they are incarcerated, they will not be counted in the out of state supervision population because they are in prison under the state prison custodial authority. However, if instead the custodial authority of this incarceration is the `SUPERVISION_AUTHORITY` of the state, then this incarceration will not exclude the person from being counted in the out of state supervision population on this day. 
"""

    # Required characteristics

    # The type of SupervisionMetric
    metric_type: SupervisionMetricType = attr.ib(
        init=False, default=SupervisionMetricType.SUPERVISION_OUT_OF_STATE_POPULATION
    )


@attr.s
class SupervisionSuccessMetric(SupervisionMetric, PersonLevelMetric):
    """Subclass of SupervisionMetric that contains information about whether
    supervision that was projected to end in a given month was completed
    successfully."""

    @classmethod
    def get_description(cls) -> str:
        return """
The `SupervisionSuccessMetric` stores information about whether supervision that was projected to end in a given month was completed successfully. This metric tracks the month in which supervision was scheduled to be completed, whether the completion was successful, and other information about the supervision that was completed.

With this metric, we can answer questions like:

- Of all of the people that were supposed to complete supervision last month, what percent completed it successfully?
- How has the rate of successful completion of supervision changed over time?
- Do the rates of successful completion of supervision vary by the race of the person on supervision?

The calculations for this metric use both `StateSupervisionSentence` and `StateIncarcerationSentence` entities to determine projected supervision completion dates, and rely on related `StateSupervisionPeriod` entities to determine whether or not the supervision was completed successfully. 

In order for a sentence to be included in these calculations it must:

- Have a set `start_date`, indicating that the sentence was started
- Have a set `completion_date`, indicating that the sentence has been completed
- Have the values required to calculate the projected date of completion (`projected_completion_date` for supervision sentences, and `max_length_days` for incarceration sentences)
- Have supervision periods with `start_date` and `termination_date` values that are within the bounds of the sentence (start and end dates inclusive here)

If a sentence meets these criteria, then the `termination_reason` on the `StateSupervisionPeriod` with the latest `termination_date` is used to determine whether the sentence was completed successfully. For example, if the last supervision period that falls within the bounds of a sentence is terminated with a reason of `DISCHARGE`, then this is marked as a successful completion. If the last supervision period within the bounds of a sentence is terminated with a reason of `REVOCATION`, then this is marked as an unsuccessful completion.

If a person has a `StateSupervisionSentence` with a `projected_completion_date` of 2020-02-28 and a `completion_date` of 2020-02-18, has one `StateSupervisionPeriod` that terminated on 2018-03-13 with a reason of `REVOCATION`, and another `StateSupervisionPeriod` that terminated on 2020-02-17 with a reason of `DISCHARGE`, then there would be a `SupervisionSuccessMetric` for the `year=2020`, the `month=02`, with the `successful_completion` boolean marked as `True`. If a person has a `StateIncarcerationSentence` with a `start_date` of 2019-01-01 and a `max_length_days` value of `800`, then their projected date of completion is 2021-03-11. If their latest `StateSupervisionPeriod` within the bounds of this sentence was terminated on 2020-11-18 with a reason of `REVOCATION`, then there would be a `SupervisionSuccessMetric` for the `year=2021` and `month=03`, where the `successful_completion` is marked as `False`.
"""

    # Required characteristics

    # The type of SupervisionMetric
    metric_type: SupervisionMetricType = attr.ib(
        init=False, default=SupervisionMetricType.SUPERVISION_SUCCESS
    )

    # Whether this represents a successful completion
    successful_completion: bool = attr.ib(default=None)

    # Days served for this sentence
    sentence_days_served: int = attr.ib(default=None)


@attr.s
class SupervisionTerminationMetric(
    SupervisionMetric,
    ViolationHistoryMixin,
    InPopulationMixin,
    AssessmentMetricMixin,
):
    """Subclass of SupervisionMetric that contains information about a supervision that
    has been terminated."""

    @classmethod
    def get_description(cls) -> str:
        return """
The `SupervisionTerminationMetric` stores information about when a person ends supervision. This metric tracks the date on which an individual ended a period of supervision, and includes information related to the period of supervision that ended.

With this metric, we can answer questions like:

- How many periods of supervision ended this month because the person absconded?
- How many people finished serving their supervision terms this year because they were discharged from their supervision earlier than their sentence’s end date?
- How many people on Officer X’s caseload this month had their supervisions successfully terminated?

This metric is derived from the `termination_date` on `StateSupervisionPeriod` entities, which store information about periods of time that an individual was supervised. A `SupervisionTerminationMetric` is created for the day that a person ended a supervision period, even if the person was incarcerated on the date the supervision ended. There are two attributes (`in_incarceration_population_on_date` and `in_supervision_population_on_date`) on this metric that indicate whether the person was counted in the incarceration and/or supervision population on the date that the supervision ended.

The presence of this metric does not mean that a person has fully completed their supervision. There is a `SupervisionTerminationMetric` for all terminations of supervision, regardless of the `termination_reason` on the period (for example, there are many terminations with a `termination_reason` of `TRANSFER_WITHIN_STATE` that mark when a person is being transferred from one supervising officer to another). 

If a person has a supervision period with a `termination_date` of 2017-10-18, then there will be a `SupervisionTerminationMetric` with a `termination_date` of 2017-10-18 that include all details of the supervision that was started. 
"""

    # Required characteristics

    # The type of SupervisionMetric
    metric_type: SupervisionMetricType = attr.ib(
        init=False, default=SupervisionMetricType.SUPERVISION_TERMINATION
    )

    # Optional characteristics

    # Change in scores between the assessment right before termination and first
    # reliable assessment while on supervision. The first "reliable" assessment is
    # determined by state-specific logic.
    assessment_score_change: float = attr.ib(default=None)

    # The reason the supervision was terminated
    termination_reason: Optional[StateSupervisionPeriodTerminationReason] = attr.ib(
        default=None
    )

    # The date the supervision was terminated
    termination_date: date = attr.ib(default=None)


@attr.s
class SupervisionStartMetric(SupervisionMetric, InPopulationMixin):
    """Subclass of SupervisionMetric that contains information about the start of
    supervision."""

    @classmethod
    def get_description(cls) -> str:
        return """
The `SupervisionStartMetric` stores information about when a person starts supervision. This metric tracks the date on which an individual started a period of supervision, and includes information related to the period of supervision that was started.

With this metric, we can answer questions like:

- How many people started serving probation due to a new court sentence this year?
- How many people started being supervised by Officer X this month?
- What proportion of individuals that started serving parole in 2020 were white men?

This metric is derived from the `start_date` on `StateSupervisionPeriod` entities, which store information about periods of time that an individual was supervised. A `SupervisionStartMetric` is created for the day that a person started a supervision period, even if the person was incarcerated on the date the supervision started. There are two attributes (`in_incarceration_population_on_date` and `in_supervision_population_on_date`) on this metric that indicate whether the person was counted in the incarceration and/or supervision population on the date that the supervision started.

The presence of this metric does not mean that a person is starting to serve a new sentence of supervision. There is a `SupervisionStartMetric` for all starts of supervision, regardless of the  `admission_reason` on the period (for example, there are many starts with a `admission_reason` of `TRANSFER_WITHIN_STATE` that mark when a person is being transferred from one supervising officer to another). 

If a person has a supervision period with a `start_date` of 2017-10-18, then there will be a `SupervisionStartMetric` with a `start_date` of 2017-10-18 that include all details of the supervision that was started. 
"""

    # Required characteristics

    # The type of SupervisionMetric
    metric_type: SupervisionMetricType = attr.ib(
        init=False, default=SupervisionMetricType.SUPERVISION_START
    )

    # Optional characteristics

    # The reason the supervision was started
    is_official_supervision_admission: bool = attr.ib(default=False)

    # The reason the supervision began
    admission_reason: Optional[StateSupervisionPeriodAdmissionReason] = attr.ib(
        default=None
    )

    # The date the supervision was started
    start_date: date = attr.ib(default=None)


@attr.s
class SupervisionCaseComplianceMetric(SupervisionPopulationMetric):
    """Subclass of SupervisionPopulationMetric for people who are on supervision on a
    given day that records information regarding whether a supervision case is
    meeting compliance standards."""

    @classmethod
    def get_description(cls) -> str:
        return """
The `SupervisionCaseComplianceMetric` is a subclass of the `SupervisionPopulationMetric` that stores information related to the compliance standards of the supervision case for a day that a person is counted in the supervision population. This metric records information relevant to the contact and assessment frequency required for the supervision case, and whether certain case compliance requirements are being met on the `date_of_evaluation`.

With this metric, we can answer questions like:

- What percent of people in the supervision population have supervision cases that are meeting the face-to-face contact standard for the state?
- At the end of last month how many people were overdue on having an assessment done on them?
- What proportion of individuals on Officer X’s caseload are actively eligible for a supervision downgrade?

This metric is derived from the `StateSupervisionPeriod` entities, which store information about periods of time that an individual was supervised, the `StateAssessment` entities, which store instances of when a person was assessed, and `StateSupervisionContact` entities, which store information about when a supervision officer makes contact with a person on supervision (or with someone in the person’s community). 

The calculation of the attributes of this metric relies entirely on the state-specific implementation of the `StateSupervisionCaseComplianceManager`. All compliance guidelines are state-specific, and the details of these state-specific calculations can be found in the file with the `StateSupervisionCaseComplianceManager` implementation for each state.
"""

    # Required characteristics

    # The type of SupervisionMetric
    metric_type: SupervisionMetricType = attr.ib(
        init=False, default=SupervisionMetricType.SUPERVISION_COMPLIANCE
    )

    # The date the on which the case's compliance was evaluated
    date_of_evaluation: date = attr.ib(default=None)

    # The number of risk assessments conducted on this person on the date_of_evaluation
    assessment_count: int = attr.ib(default=None)

    # The number of face-to-face contacts with this person on the date_of_evaluation
    face_to_face_count: int = attr.ib(default=None)

    # The number of home visits conducted on this person on the date_of_evaluation
    home_visit_count: int = attr.ib(default=None)

    # Optional characteristics

    # The date that the last assessment happened. If no assessment has yet happened, this is None.
    most_recent_assessment_date: Optional[date] = attr.ib(default=None)

    # If this value is set, then this is the next recommended assessment date
    # according to department policy. If this value is in the past, this implies that the
    # assessment is overdue.
    next_recommended_assessment_date: Optional[date] = attr.ib(default=None)

    # The date that the last face-to-face contact happened. If no meetings have yet happened, this is None.
    most_recent_face_to_face_date: Optional[date] = attr.ib(default=None)

    # When the next recommended face-to-face contact should happen according to compliance standards.
    # Should be unset if we do not know the compliance standards for this person, or no further contact is required.
    next_recommended_face_to_face_date: Optional[date] = attr.ib(default=None)

    # The date that the last home visit contact happened. If no meetings have yet happened, this is None.
    most_recent_home_visit_date: Optional[date] = attr.ib(default=None)

    # When the next recommended home visit should happen according to compliance standards.
    # SHould be unset if we do not know the compliance standards for this person, or no further contact is required.
    next_recommended_home_visit_date: Optional[date] = attr.ib(default=None)

    # The date that the last collateral contact happened with a treatment provider. If no meetings have yet happened, this is None.
    most_recent_treatment_collateral_contact_date: Optional[date] = attr.ib(
        default=None
    )

    # When the next recommended treatment provider collateral contact should happen according to compliance standards.
    # Should be unset if we do not know the compliance standards for this person or no further contact is required.
    next_recommended_treatment_collateral_contact_date: Optional[date] = attr.ib(
        default=None
    )

    # If the person on supervision is eligible for a downgrade, the level they should be
    # downgraded to.
    # This value is set to None if we do not know how to calculate recommended
    # supervision level status for this person.
    recommended_supervision_downgrade_level: Optional[StateSupervisionLevel] = attr.ib(
        default=None
    )


@attr.s
class SupervisionDowngradeMetric(SupervisionMetric):
    """
    Subclass of SupervisionMetric for people whose supervision level has been
    downgraded.

    Note: This metric only identifies supervision level downgrades for states where a
    new supervision period is created if the supervision level changes.
    """

    @classmethod
    def get_description(cls) -> str:
        return """
The `SupervisionDowngradeMetric` stores instances of when the supervision level of a person’s supervision is changed to a level that is less restrictive. This metric tracks the day that the downgrade occurred, as well as other information related to the supervision that was downgraded.

With this metric, we can answer questions like:

- How many people were downgraded from a supervision level of `MEDIUM` to `MINIMUM` in 2020?
- How did the number of people downgraded to the `UNSUPERVISED` supervision level change after the state requirements for the downgrade were updated in 2019?
- Were there more supervision downgrades for men or women last month?

This metric is derived from the `supervision_level` values on the `StateSupervisionPeriod` entities, which store information about periods of time that an individual was supervised. The calculations for this metric only identify supervision level downgrades for states where a new supervision period is created when the supervision level changes. If the `supervision_level` value of a period is updated in place when a downgrade occurs, then these calculations will not be able to capture that downgrade in this metric. We also consider supervision level downgrades between two adjacent supervision periods, where the `termination_date` of one period is the same as the `start_date` of the next period. If a person ends a supervision period with a `MAXIMUM` supervision level on 2018-04-12, and then starts a new supervision period with a `MEDIUM` supervision level months later, on 2018-11-17, then this is not counted as a supervision level downgrade.

We are only able to calculate supervision downgrades between two supervision levels that fall within the clear level hierarchy (e.g. `MAXIMUM`, `HIGH`, `MEDIUM`, etc). We are not able to identify if a downgrade occurred if a person is on a supervision level of, say, `DIVERSION`, which doesn’t have an explicit correlation to a degree of restriction for the supervision. 

Say a person started serving a period of probation with a `supervision_level` of `MEDIUM` on 2020-01-01, and then on 2020-08-13 this supervision period is terminated and they start a new supervision period with a `supervision_level` of `MINIMUM`. There would be a `SupervisionDowngradeMetric` for this downgrade of supervision levels with a `date_of_downgrade` of 2020-08-13, a `previous_supervision_level` of `MEDIUM` and a `supervision_level` of `MINIMUM`. 
"""

    # The type of SupervisionMetric
    metric_type: SupervisionMetricType = attr.ib(
        init=False, default=SupervisionMetricType.SUPERVISION_DOWNGRADE
    )

    # The date on which the downgrade in supervision level took place
    date_of_downgrade: date = attr.ib(default=None)

    # The previous supervision level, prior to the downgrade
    previous_supervision_level: StateSupervisionLevel = attr.ib(default=None)

    # The new supervision level, after the downgrade
    supervision_level: StateSupervisionLevel = attr.ib(default=None)
