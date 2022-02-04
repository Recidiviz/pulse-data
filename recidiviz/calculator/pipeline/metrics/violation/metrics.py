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
"""Metrics related to violations and their responses and decisions."""
import abc
import datetime

import attr

from recidiviz.calculator.pipeline.utils.event_utils import ViolationResponseMixin
from recidiviz.calculator.pipeline.utils.metric_utils import (
    PersonLevelMetric,
    RecidivizMetric,
    RecidivizMetricType,
)


class ViolationMetricType(RecidivizMetricType):
    """The type of violation metrics."""

    VIOLATION = "VIOLATION"


@attr.s
class ViolationMetric(RecidivizMetric[ViolationMetricType], PersonLevelMetric):
    """Models a single violation metric.

    Contains all of the identifying characteristics of the metric, including
    required characteristics for normalization as well as optional
    characteristics for slicing the data.
    """

    # Required characteristics
    metric_type_cls = ViolationMetricType

    # The type of ViolationMetric
    metric_type: ViolationMetricType = attr.ib(default=None)

    # Year
    year: int = attr.ib(default=None)

    # Optional characteristics

    # Month
    month: int = attr.ib(default=None)

    # Violation Id
    supervision_violation_id: int = attr.ib(default=None)

    @classmethod
    @abc.abstractmethod
    def get_description(cls) -> str:
        """Should be implemented by metric subclasses to return a description of the metric."""


@attr.s
class ViolationWithResponseMetric(ViolationMetric, ViolationResponseMixin):
    """Subclass of ViolationMetric that stores information about a violation and its response."""

    @classmethod
    def get_description(cls) -> str:
        return """
The `ViolationWithResponseMetric` stores information about when a report has been submitted about a person on supervision violating one or more conditions of their supervision. This metric tracks the date of the earliest response to a violation incident for each type of violation associated with the incident.

With this metric, we can answer questions like:

- Has Person X ever been written up for a substance use violation?
- How has the number of reported technical violations changed over time?
- Does the frequency of reported municipal violations vary by the age of the person on supervision?

This metric is derived from the `StateSupervisionViolation` and `StateSupervisionViolationResponse` entities. The `StateSupervisionViolation` entities store information about the violation incident that occurred, and the `StateSupervisionViolationResponse` entities store information about how agents within the corrections system responded to the violation. There is one `ViolationWithResponseMetric` created for each of the violation types present on each `StateSupervisionViolation`, through the `supervision_violation_types` relationship to the `StateSupervisionViolationTypeEntry` entities. The date associated with each `ViolationWithResponseMetric` is the `response_date` of the earliest `StateSupervisionViolationResponse` associated with the violation. This is generally the date that the first violation report or citation was submitted describing the violating behavior. We track violations by the date of the first report since the date a report was submitted is required in most state databases, whereas the date of the violating behavior is not consistently reported. A violation must have a report with a set `response_date` to be included in the calculations.

Some states have defined state-specific violation type subtypes. For example, in Pennsylvania the `TECHNICAL` violations are broken down into categories of `HIGH_TECH`, `MED_TECH` and `LOW_TECH`. In these states, one `ViolationWithResponseMetric` is produced for each violation type subtype present on the `StateSupervisionViolation`.

If a parole officer submitted a violation report on 2018-09-07 describing violating behavior that occurred on 2018-09-01, where the behavior included both `TECHNICAL` and `MUNICIPAL` violations, then there will be two `ViolationWithResponseMetrics` produced for the date `2018-09-07`, one with `violation_type=TECHNICAL` and the other with `violation_type=MUNICIPAL`. Both metrics will also record the date of the violation, with `violation_date=2018-09-01`. 

If a probation officer in US_PA submitted a violation report on 2019-11-19 describing violating behavior that fell into both the `LOW_TECH` and `MED_TECH` categories of violation subtypes for the state, then there will be two `ViolationWithResponseMetrics` produced for the date `2019-11-19` with `violation_type=TECHNICAL`. One will have `violation_type_subtype=LOW_TECH` and the other will have `violation_type_subtype=MED_TECH`. Neither metrics will have a set `violation_date` field, since the date of the violating behavior was not recorded on the violation report.
"""

    metric_type: ViolationMetricType = attr.ib(
        init=False, default=ViolationMetricType.VIOLATION
    )

    # Earliest response date associated with the supervision_violation_id
    response_date: datetime.date = attr.ib(default=None)
