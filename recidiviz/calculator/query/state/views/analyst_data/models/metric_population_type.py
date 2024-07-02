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

from recidiviz.calculator.query.state.views.analyst_data.models.span_selector import (
    SpanSelector,
)
from recidiviz.calculator.query.state.views.analyst_data.models.span_type import (
    SpanType,
)
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
    # Use `CUSTOM` enum for ad-hoc population definitions
    CUSTOM = "CUSTOM"

    @property
    def population_name_short(self) -> str:
        return self.value.lower()

    @property
    def population_name_title(self) -> str:
        return self.value.title().replace("_", " ")


# TODO(#23055): Add state_code and person_id filters

POPULATION_TYPE_TO_SPAN_SELECTOR_LIST: Dict[
    MetricPopulationType, List[SpanSelector]
] = {
    MetricPopulationType.INCARCERATION: [
        SpanSelector(
            span_type=SpanType.COMPARTMENT_SESSION,
            span_conditions_dict={
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
        SpanSelector(
            span_type=SpanType.WORKFLOWS_USER_REGISTRATION_SESSION,
            span_conditions_dict={},
        ),
    ],
    MetricPopulationType.SUPERVISION: [
        SpanSelector(
            span_type=SpanType.COMPARTMENT_SESSION,
            span_conditions_dict={
                "compartment_level_1": ["SUPERVISION"],
                "compartment_level_2": [
                    StateSupervisionPeriodSupervisionType.COMMUNITY_CONFINEMENT.value,
                    StateSupervisionPeriodSupervisionType.DUAL.value,
                    StateSupervisionPeriodSupervisionType.PAROLE.value,
                    StateSupervisionPeriodSupervisionType.PROBATION.value,
                    StateSupervisionPeriodSupervisionType.WARRANT_STATUS.value,
                ],
            },
        ),
        SpanSelector(
            span_type=SpanType.SUPERVISION_OFFICER_INFERRED_LOCATION_SESSION,
            span_conditions_dict={},
        ),
        SpanSelector(
            span_type=SpanType.WORKFLOWS_USER_REGISTRATION_SESSION,
            span_conditions_dict={},
        ),
    ],
    MetricPopulationType.JUSTICE_INVOLVED: [
        SpanSelector(
            span_type=SpanType.COMPARTMENT_SESSION,
            span_conditions_dict={
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
        SpanSelector(
            span_type=SpanType.WORKFLOWS_USER_REGISTRATION_SESSION,
            span_conditions_dict={},
        ),
    ],
}

POPULATION_TYPE_TO_SPAN_SELECTOR_BY_UNIT_OF_OBSERVATION = {
    population_type: {u.unit_of_observation_type: u for u in span_selectors}
    for population_type, span_selectors in POPULATION_TYPE_TO_SPAN_SELECTOR_LIST.items()
}
