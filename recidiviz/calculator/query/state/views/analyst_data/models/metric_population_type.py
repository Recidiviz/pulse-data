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
"""Constants related to a MetricPopulationType."""
from enum import Enum
from typing import Dict, List

import attr

from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.common import attr_validators
from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)


class MetricPopulationType(Enum):
    """The type of population over which to calculate metrics."""

    INCARCERATION = "INCARCERATION"
    SUPERVISION = "SUPERVISION"
    JUSTICE_INVOLVED = "JUSTICE_INVOLVED"


@attr.define(frozen=True, kw_only=True)
class MetricPopulation:
    """
    Class that stores information about a population defined by attributes of `compartment_sessions`
    along with functions to help generate SQL fragments
    """

    # Enum describing the type of population
    population_type: MetricPopulationType

    # Dictionary in which keys are columns names in `compartment_sessions`, and values are lists of
    # string filter values for those columns
    conditions_dict: Dict[str, List[str]] = attr.field(
        validator=attr_validators.is_dict
    )

    @property
    def population_name_short(self) -> str:
        return self.population_type.value.lower()

    @property
    def population_name_title(self) -> str:
        return self.population_type.value.title().replace("_", " ")

    def get_conditions_query_string(self) -> str:
        return "\n AND ".join(
            [
                f"{attribute} IN ({list_to_query_string(self.conditions_dict[attribute], quoted=True)})"
                for attribute in self.conditions_dict
            ]
        )


METRIC_POPULATIONS_BY_TYPE = {
    MetricPopulationType.INCARCERATION: MetricPopulation(
        population_type=MetricPopulationType.INCARCERATION,
        conditions_dict={
            "compartment_level_1": ["INCARCERATION"],
            "compartment_level_2": [
                StateSpecializedPurposeForIncarceration.GENERAL.value,
                StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD.value,
                StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION.value,
                StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON.value,
                StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY.value,
                StateSpecializedPurposeForIncarceration.WEEKEND_CONFINEMENT.value,
                StateSupervisionPeriodSupervisionType.COMMUNITY_CONFINEMENT.value,
            ],
        },
    ),
    MetricPopulationType.SUPERVISION: MetricPopulation(
        population_type=MetricPopulationType.SUPERVISION,
        conditions_dict={
            "compartment_level_1": ["SUPERVISION"],
            "compartment_level_2": [
                StateSupervisionPeriodSupervisionType.COMMUNITY_CONFINEMENT.value,
                StateSupervisionPeriodSupervisionType.DUAL.value,
                StateSupervisionPeriodSupervisionType.INFORMAL_PROBATION.value,
                StateSupervisionPeriodSupervisionType.PAROLE.value,
                StateSupervisionPeriodSupervisionType.PROBATION.value,
            ],
        },
    ),
    MetricPopulationType.JUSTICE_INVOLVED: MetricPopulation(
        population_type=MetricPopulationType.JUSTICE_INVOLVED,
        conditions_dict={
            # every compartment in the union of incarceration and supervision
            "compartment_level_1": ["INCARCERATION", "SUPERVISION"],
            "compartment_level_2": [
                StateSpecializedPurposeForIncarceration.GENERAL.value,
                StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD.value,
                StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION.value,
                StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON.value,
                StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY.value,
                StateSpecializedPurposeForIncarceration.WEEKEND_CONFINEMENT.value,
                StateSupervisionPeriodSupervisionType.COMMUNITY_CONFINEMENT.value,
                StateSupervisionPeriodSupervisionType.DUAL.value,
                StateSupervisionPeriodSupervisionType.INFORMAL_PROBATION.value,
                StateSupervisionPeriodSupervisionType.PAROLE.value,
                StateSupervisionPeriodSupervisionType.PROBATION.value,
            ],
        },
    ),
}
