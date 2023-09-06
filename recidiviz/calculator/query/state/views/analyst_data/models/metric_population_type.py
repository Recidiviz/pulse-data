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
from typing import Dict, List, Sequence, Union

import attr

from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.views.analyst_data.models.span_type import (
    SpanType,
)
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
    # Use `CUSTOM` enum for ad-hoc population definitions
    CUSTOM = "CUSTOM"


# TODO(#23055): Add state_code and person_id filters
@attr.define(frozen=True, kw_only=True)
class MetricPopulation:
    """
    Class that stores information about a population defined by attributes of `compartment_sessions`
    along with functions to help generate SQL fragments
    """

    # Enum describing the type of population
    population_type: MetricPopulationType

    # Dictionary in which keys are `SpanType` enums, and values are dictionaries
    # mapping span attributes to filters
    person_spans_dict: Dict[SpanType, Dict[str, Union[List[str], str]]] = attr.field(
        validator=attr_validators.is_dict
    )

    @property
    def population_name_short(self) -> str:
        return self.population_type.value.lower()

    @property
    def population_name_title(self) -> str:
        return self.population_type.value.title().replace("_", " ")

    def get_source_span_types(self) -> List[str]:
        return [span.value for span in self.person_spans_dict]

    def get_population_query(self) -> str:
        """Returns query for the intersection of input person_spans, with attribute filters applied"""
        population_conditions = []
        for span_type, attribute_dict in self.person_spans_dict.items():
            condition_strings = [f'span = "{span_type.value}"']
            for attribute, conditions in attribute_dict.items():
                if isinstance(conditions, str):
                    attribute_condition_string = conditions
                elif isinstance(conditions, Sequence):
                    attribute_condition_string = (
                        f"IN ({list_to_query_string(conditions, quoted=True)})"
                    )
                else:
                    raise TypeError(
                        "All attribute filters must have type str or Sequence[str]"
                    )
                condition_strings.append(
                    f"""JSON_EXTRACT_SCALAR(span_attributes, "$.{attribute}") {attribute_condition_string}"""
                )
            # Apply all conditions via AND to a single span type
            condition_strings_query_fragment = "\n        AND ".join(condition_strings)
            population_conditions.append(f"({condition_strings_query_fragment})")
        # Combine all span type filters via an OR condition, creating a "long" table of
        # stacked spans. In the subsequent cte, we take the intersection of all stacked spans
        # to ensure that a given span of time satisfies all span conditions at once.
        population_conditions_query_fragment = "\n    OR ".join(population_conditions)
        query_template = f"""
WITH filtered_spans AS (
    SELECT *, end_date AS end_date_exclusive
    FROM `{{project_id}}.analyst_data.person_spans_materialized`
    WHERE {population_conditions_query_fragment}
)
,
{create_sub_sessions_with_attributes("filtered_spans", end_date_field_name="end_date_exclusive",index_columns=["state_code", "person_id"])}
,
# Get all span types represented by a single sub-sessionized span of time
sub_sessions_deduped AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive,
        ARRAY_AGG(span) AS span_types,
    FROM
        sub_sessions_with_attributes
    GROUP BY 1, 2, 3, 4
)
# Filter to only sub-sessions where all span types are covered, indicating that all span filter conditions are met.
SELECT * EXCEPT(span_types) FROM sub_sessions_deduped
WHERE (
    SELECT LOGICAL_AND(
        span IN UNNEST([{list_to_query_string(sorted(self.get_source_span_types()), quoted=True)}])
    ) FROM UNNEST(span_types) 
span)
"""
        return query_template


METRIC_POPULATIONS_BY_TYPE = {
    MetricPopulationType.INCARCERATION: MetricPopulation(
        population_type=MetricPopulationType.INCARCERATION,
        person_spans_dict={
            SpanType.COMPARTMENT_SESSION: {
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
        },
    ),
    MetricPopulationType.SUPERVISION: MetricPopulation(
        population_type=MetricPopulationType.SUPERVISION,
        person_spans_dict={
            SpanType.COMPARTMENT_SESSION: {
                "compartment_level_1": ["SUPERVISION"],
                "compartment_level_2": [
                    StateSupervisionPeriodSupervisionType.COMMUNITY_CONFINEMENT.value,
                    StateSupervisionPeriodSupervisionType.DUAL.value,
                    StateSupervisionPeriodSupervisionType.INFORMAL_PROBATION.value,
                    StateSupervisionPeriodSupervisionType.PAROLE.value,
                    StateSupervisionPeriodSupervisionType.PROBATION.value,
                ],
            },
        },
    ),
    MetricPopulationType.JUSTICE_INVOLVED: MetricPopulation(
        population_type=MetricPopulationType.JUSTICE_INVOLVED,
        person_spans_dict={
            SpanType.COMPARTMENT_SESSION: {
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
        },
    ),
}
