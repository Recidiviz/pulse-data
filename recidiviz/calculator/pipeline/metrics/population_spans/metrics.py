# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Population span metrics we calculate."""
import abc
import datetime
from typing import Optional

import attr

from recidiviz.calculator.pipeline.metrics.utils.metric_utils import (
    PersonLevelMetric,
    RecidivizMetric,
    RecidivizMetricType,
    SecondaryPersonExternalIdMetric,
)
from recidiviz.calculator.pipeline.utils.identifier_models import (
    SupervisionLocationMixin,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodSupervisionType,
)


class PopulationSpanMetricType(RecidivizMetricType):
    """The type of population span metrics."""

    INCARCERATION_POPULATION_SPAN = "INCARCERATION_POPULATION_SPAN"
    SUPERVISION_POPULATION_SPAN = "SUPERVISION_POPULATION_SPAN"


@attr.s
class PopulationSpanMetric(
    RecidivizMetric[PopulationSpanMetricType],
    PersonLevelMetric,
    SecondaryPersonExternalIdMetric,
):
    """Base model for population span metrics."""

    # Required characteristics
    metric_type_cls = PopulationSpanMetricType

    # The type of PopulationSpanMetric
    metric_type: PopulationSpanMetricType = attr.ib(default=None)

    start_date_inclusive: Optional[datetime.date] = attr.ib(default=None)
    end_date_exclusive: Optional[datetime.date] = attr.ib(default=None)

    # Whether the period corresponding to the metric is counted in the state's population
    included_in_state_population: bool = attr.ib(default=True)

    @classmethod
    @abc.abstractmethod
    def get_description(cls) -> str:
        """Should be implemented by metric subclasses to return a description of the metric."""


@attr.s
class IncarcerationPopulationSpanMetric(PopulationSpanMetric):
    """Subclass of PopulationSpanMetric that represents the span of a person being incarcerated."""

    @classmethod
    def get_description(cls) -> str:
        return """
The `IncarcerationPopulationSpanMetric` stores information about a span of time that an individual spent incarcerated. This metric tracks the period of time on which an individual was incarcerated and whether they're counted towards the state's incarceration population during that time, and includes information related to the stay in a facility. Note, the overall span of time spent contiguously in a facility may be overall more than a year, but spans are split based on a person's age at that time if the birthdate exists for that person.

With this metric, we can answer questions like:

- How long does a person spent in a DOC facility on average?
- Who was incarcerated in facility X on day Y?

This metric is derived from the `StateIncarcerationPeriod` entities, which store information about periods of time that an individual was in an incarceration facility. All population span metrics are end date exclusive, meaning that a person is not counted in a facility's population on the date that they are released from the facility."""

    # Required characteristics

    # The type of PopulationSpanMetric
    metric_type: PopulationSpanMetricType = attr.ib(
        init=False, default=PopulationSpanMetricType.INCARCERATION_POPULATION_SPAN
    )

    # Optional characteristics

    # Facility
    facility: Optional[str] = attr.ib(default=None)

    # Purpose for incarceration
    purpose_for_incarceration: Optional[
        StateSpecializedPurposeForIncarceration
    ] = attr.ib(default=None)

    # Custodial authority
    custodial_authority: Optional[StateCustodialAuthority] = attr.ib(default=None)

    # Area of jurisdictional coverage of the court that sentenced the person to this
    # incarceration
    judicial_district_code: Optional[str] = attr.ib(default=None)


@attr.s
class SupervisionPopulationSpanMetric(PopulationSpanMetric, SupervisionLocationMixin):
    """Subclass of PopulationSpanMetric that represents the span of a person on supervision."""

    @classmethod
    def get_description(cls) -> str:
        return """
The `SupervisionPopulationSpanMetric` stores information about a span of time that an individual spent on supervision. This metric tracks the period of time on which an individual was on supervision and whether they're counted towards the state's supervision population during that time, and includes information related to their supervision type and level. Note, the overall span of time spent contiguously on supervision with certain characteristics may be overall more than a year, but spans are split based on a person's age at that time if the birthdate exists for that person.

With this metric, we can answer questions like:

- How long does a person spend on supervision on average?
- Who was on supervision in district X on day Y?

This metric is derived from the `StateSupervisionPeriod` entities, which store information about periods of time that an individual was on supervision. All population span metrics are end date exclusive, meaning that a person is not counted in population on the date that their supervision period is terminated."""

    # Required characteristics

    # The type of PopulationSpanMetric
    metric_type: PopulationSpanMetricType = attr.ib(
        init=False, default=PopulationSpanMetricType.SUPERVISION_POPULATION_SPAN
    )

    # Optional characteristics

    # Supervision Type
    supervision_type: Optional[StateSupervisionPeriodSupervisionType] = attr.ib(
        default=None
    )

    # Supervision Level
    supervision_level: Optional[StateSupervisionLevel] = attr.ib(default=None)

    # Supervision Level Raw Text
    supervision_level_raw_text: Optional[str] = attr.ib(default=None)

    # Case Type
    case_type: Optional[StateSupervisionCaseType] = attr.ib(default=None)

    # Custodial Authority
    custodial_authority: Optional[StateCustodialAuthority] = attr.ib(default=None)

    # External ID of the officer who is supervising the person during this span of time
    supervising_officer_external_id: Optional[str] = attr.ib(default=None)

    # Area of jurisdictional coverage of the court that sentenced the person to this
    # period of supervision
    judicial_district_code: Optional[str] = attr.ib(default=None)
