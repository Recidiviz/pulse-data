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

"""Recidivism metrics we calculate."""
import abc
from datetime import date
from typing import Optional

import attr

from recidiviz.calculator.pipeline.metrics.utils.metric_utils import (
    PersonLevelMetric,
    RecidivizMetric,
    RecidivizMetricType,
)


class ReincarcerationRecidivismMetricType(RecidivizMetricType):
    """The type of reincarceration recidivism metrics."""

    REINCARCERATION_COUNT = "REINCARCERATION_COUNT"
    REINCARCERATION_RATE = "REINCARCERATION_RATE"


# TODO(#1841): Implement rearrest recidivism metrics
# TODO(#1842): Implement reconviction recidivism metrics
@attr.s
class ReincarcerationRecidivismMetric(
    RecidivizMetric[ReincarcerationRecidivismMetricType], PersonLevelMetric
):
    """Models a single recidivism metric."""

    # Required characteristics
    metric_type_cls = ReincarcerationRecidivismMetricType

    # The type of ReincarcerationRecidivismMetric
    metric_type: ReincarcerationRecidivismMetricType = attr.ib(default=None)

    # Optional characteristics

    # The bucket string of the persons' incarceration stay length (in months), e.g.,
    # '<12' or '36-48'
    stay_length_bucket: Optional[str] = attr.ib(default=None)

    # The facility the person was released from
    release_facility: Optional[str] = attr.ib(default=None)

    # County of residence
    county_of_residence: Optional[str] = attr.ib(default=None)

    @classmethod
    @abc.abstractmethod
    def get_description(cls) -> str:
        """Should be implemented by metric subclasses to return a description of the
        metric."""


# TODO(#10727): Update this metric description when we move IP transfer collapsing
#  out of entity normalization
@attr.s
class ReincarcerationRecidivismCountMetric(ReincarcerationRecidivismMetric):
    """The ReincarcerationRecidivismCountMetric stores information about when a
    person who has previously been released from prison is admitted to prison again.
    """

    @classmethod
    def get_description(cls) -> str:
        return """
The `ReincarcerationRecidivismCountMetric` stores information about when a person who has previously been released from prison is admitted to prison again. This metric tracks the date of the reincarceration, as well as other details about the reincarceration.

The term “recidivism” is defined in many ways throughout the criminal justice system. This metric explicitly calculates instances of reincarceration recidivism, which is defined as a person being incarcerated again after having previously been released from incarceration. That is, each time a single person is released from prison — to supervision and/or to full liberty — and is later reincarcerated in prison, that is counted as an instance of recidivism.

With this metric, we can answer questions like:

- How many people were admitted to prison in 2019 that had previously been incarcerated?
- For all of the people with reincarceration admissions in 2020, how many days, on average, had they spent at liberty since the time they were last released from prison?
- Does the number of reincarceration admissions in the state vary by the county in which the people lived while they were at liberty?

This metric is derived from the `StateIncarcerationPeriod` entities, which store information about periods of time that an individual was in an incarceration facility. When normalizing the `StateIncarcerationPeriod` entities to create this metric, transfers between facilities are collapsed so that we can easily identify releases from and admissions to prison where the person is not being transferred into or out of another facility. This means that if a person has two chronologically consecutive `StateIncarcerationPeriod` entities, where the `release_reason` from the first period is `TRANSFER`, and the `admission_reason` on the second period is also `TRANSFER`, then the two periods are collapsed into one continuous period of time spent incarcerated.

These calculations work by looking at all releases from prison, where the prison stay qualifies as a formal stay in incarceration, and the release is either to supervision or to liberty. We then look for admissions back into formal incarceration that followed these official releases from formal incarceration, which are reincarceration admissions. Parole board holds or other forms of temporary custody are not included as formal stays in incarceration, even if they occur in a state prison facility. Admissions to parole board holds or other forms of temporary custody are not considered reincarceration admissions. Admissions for parole revocations that may follow time spent in a parole board hold, however, do qualify has reincarceration recidivism. Admissions for probation revocations can also be classified as reincarceration recidivism if the person was previously incarcerated. Sanction admissions from supervision for treatment or shock incarceration can also be considered reincarceration recidivism if the person was previously incarceration.

Say a person was released to liberty on 2008-01-03 from a state prison, where they had served the entirety of a new sentence. Then 10 years later, on 2018-01-26, they are admitted to prison for a new sentence. In this case, there will be one `ReincarcerationRecidivismCountMetric` produced with a `reincarceration_date` of 2018-01-26, the date of the reincarceration admission to prison.

Say a person was released to parole on 2015-02-18 from a state prison, where they were being held to serve a new sentence. Then, 8 months later on 2015-10-16, they are brought into a parole board hold in response to a parole violation. They are held in the parole board hold for 20 days while they wait for a decision from the parole board. On 2015-11-05 they are formally revoked by the parole board, and are officially admitted to the prison for the parole revocation. In this case, there will be one `ReincarcerationRecidivismCountMetric` produced with a `reincarceration_date` of 2015-11-05, the date of the formal parole revocation admission.
"""

    # Required characteristics

    metric_type: ReincarcerationRecidivismMetricType = attr.ib(
        init=False, default=ReincarcerationRecidivismMetricType.REINCARCERATION_COUNT
    )

    # Year
    year: int = attr.ib(default=None)

    # Month
    month: int = attr.ib(default=None)

    # The days at liberty between release and reincarceration
    days_at_liberty: int = attr.ib(default=None)

    # Date of reincarceration
    reincarceration_date: date = attr.ib(default=None)


# TODO(#10727): Update this metric description when we move IP transfer collapsing
#  out of entity normalization
@attr.s
class ReincarcerationRecidivismRateMetric(ReincarcerationRecidivismMetric):
    """The ReincarcerationRecidivismRateMetric stores information about whether or not
    a person is ever readmitted to prison within a certain number of years after being
    officially released from incarceration.
    """

    @classmethod
    def get_description(cls) -> str:
        return """
The `ReincarcerationRecidivismRateMetric` stores information about whether or not a person is ever readmitted to prison within a certain number of years after being officially released from incarceration. This metric tracks the date that the person was released from incarceration, the calendar year release cohort that the individual was included in, and the number of years after the date of release during which recidivism was measured.

The term “recidivism” is defined in many ways throughout the criminal justice system. This metric explicitly calculates instances of reincarceration recidivism, which is defined as a person being incarcerated again after having previously been released from incarceration. That is, each time a single person is released from prison — to supervision and/or to full liberty — and is later reincarcerated in prison, that is counted as an instance of recidivism.

With this metric, we can answer questions like:

- How many people that were released from prison in 2015 had returned to prison within the next 3 years?
- Over the last 10 years, what was the average rate of recidivism within 1 year of release?
- How does the 5 year recidivism rate differ by gender in this state?

This metric is derived from the `StateIncarcerationPeriod` entities, which store information about periods of time that an individual was in an incarceration facility. When normalizing the `StateIncarcerationPeriod` entities to create this metric, transfers between facilities are collapsed so that we can easily identify releases from and admissions to prison where the person is not being transferred into or out of another facility. This means that if a person has two chronologically consecutive `StateIncarcerationPeriod` entities, where the `release_reason` from the first period is `TRANSFER`, and the `admission_reason` on the second period is also `TRANSFER`, then the two periods are collapsed into one continuous period of time spent incarcerated.

These calculations work by looking at all releases from prison, where the prison stay qualifies as a formal stay in incarceration, and the release is either to supervision or to liberty. For each qualified release, we look for admissions back into formal incarceration following the release that are reincarceration admissions.

Parole board holds or other forms of temporary custody are not included as formal stays in incarceration, even if they occur in a state prison facility. Admissions to parole board holds or other forms of temporary custody are not considered reincarceration admissions. Admissions for parole revocations that may follow time spent in a parole board hold, however, do qualify has reincarceration recidivism. Admissions for probation revocations can also be classified as reincarceration recidivism if the person was previously incarcerated. Sanction admissions from supervision for treatment or shock incarceration can also be considered reincarceration recidivism if the person was previously incarceration.

For each qualified release from prison, we produce `ReincarcerationRecidivismRateMetrics` for up to 10 follow-up-periods, for each integer year since the release. We only produce a `ReincarcerationRecidivismRateMetric` for a follow-up-period if the period has already completed or is in progress. For example, if a person was released on 2018-01-01, and today is 2021–01-01, then the following follow-up-periods have completed:

- `follow_up_period=1`: 2018-01-01 to 2018-12-31
- `follow_up_period=2`: 2018-01-01 to 2019-12-31
- `follow_up_period=3`: 2018-01-01 to 2020-12-31

This follow-up-period has just started:

- `follow_up_period=4`: 2018-01-01 to 2021-12-31

For this release, we will create a `ReincarcerationRecidivismRateMetric` for the follow-up-periods 1 through 4. We will not create metrics for any follow-up-period values 5 or greater, since those periods are not yet in progress.

For each of the relevant follow-up-period windows, we create a `ReincarcerationRecidivismRateMetric` indicating whether the person was reincarcerated within the number of years after the release. If the person was not reincarcerated within the follow-up-period window, then there will be a single `ReincarcerationRecidivismRateMetric` created where `did_recidivate=False`. If a person was reincarcerated within the follow-up-period window, then one `ReincarcerationRecidivismRateMetric` is produced for each unique reincarceration admission within the window. 

Say a person was released to liberty on 2008-01-03 from a state prison, where they had served the entirety of a new sentence. Then, a little more than 6 years later, on 2014-01-26, they are admitted to prison for a new sentence, and they are still incarcerated from this admission today. In this case, there will be 10 `ReincarcerationRecidivismRateMetrics` produced with the following attributes: 

| release_cohort | follow_up_period | did_recidivate |
| -------------- | ---------------- | -------------- |
| 2008           | 1                | False          |
| 2008           | 2                | False          |
| 2008           | 3                | False          |
| 2008           | 4                | False          |
| 2008           | 5                | False          |
| 2008           | 6                | False          |
| 2008           | 7                | True           |
| 2008           | 8                | True           |
| 2008           | 9                | True           |
| 2008           | 10               | True           |

Say a person was released to liberty on 2008-01-03 from a state prison, where they had served the entirety of a new sentence. Then, a little more than 2 years later, on 2010-01-13, they are admitted to prison for a new sentence. They are later released from prison back to liberty on 2016-09-08, and then reincarcerated a second time on 2017-05-13. In this case, if we run the recidivism calculations on 2021-01-01, then the following metrics will be produced:

| release_cohort | follow_up_period | did_recidivate |
| -------------- | ---------------- | -------------- |
| 2008           | 1                | False          |
| 2008           | 2                | False          |
| 2008           | 3                | True           |
| 2008           | 4                | True           |
| 2008           | 5                | True           |
| 2008           | 6                | True           |
| 2008           | 7                | True           |
| 2008           | 8                | True           |
| 2008           | 9                | True           |
| 2008           | 10*              | True           |
| 2008           | 10*              | True           |
| 2016           | 1                | True           |
| 2016           | 2                | True           |
| 2016           | 3                | True           |
| 2016           | 4                | True           |
| 2016           | 5                | True           |

*Note that there are two metrics produced for `release_cohort=2008` and `follow_up_period=10`, since there are two reincarcerations within the 10-year follow-up-period window. 
"""

    # Required characteristics

    # The type of ReincarcerationRecidivismMetric
    metric_type: ReincarcerationRecidivismMetricType = attr.ib(
        init=False, default=ReincarcerationRecidivismMetricType.REINCARCERATION_RATE
    )

    # The integer year during which the persons were released
    release_cohort: int = attr.ib(default=None)  # non-nullable

    # The integer number of years after date of release during which recidivism was
    # measured
    follow_up_period: int = attr.ib(default=None)  # non-nullable

    # Required metric values

    # Whether or not the person recidivated within the follow_up_period number of
    # years following the release
    did_recidivate: bool = attr.ib(default=None)  # non-nullable

    # Date of release
    release_date: date = attr.ib(default=None)  # non-nullable
