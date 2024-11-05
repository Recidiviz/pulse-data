# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Defines an ObservationSelector object used to filter rows from an observations table.
"""
import abc
from typing import Generic

from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.observations.metric_unit_of_observation import MetricUnitOfObservation
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)
from recidiviz.observations.observation_type_utils import (
    ObservationTypeT,
    attributes_column_name_for_observation_type,
    observation_type_name_column_for_observation_type,
)


class ObservationSelector(Generic[ObservationTypeT]):
    """
    Class that stores information that can be used to generate query fragments that
    allow us to select a targeted set of observations.
    """

    @property
    @abc.abstractmethod
    def observation_type(self) -> ObservationTypeT:
        """The type of the observation we're selecting"""

    @property
    @abc.abstractmethod
    def observation_conditions_dict(self) -> dict[str, list[str] | str]:
        """Dictionary mapping observation attributes to their associated conditions.
        Only observations with these attribute values will be selected.
        """

    @property
    def unit_of_observation_type(self) -> MetricUnitOfObservationType:
        """Returns the MetricUnitOfObservationType associated with the span type"""
        return self.observation_type.unit_of_observation_type

    @property
    def unit_of_observation(self) -> MetricUnitOfObservation:
        """Returns the MetricUnitOfObservation object associated with the span type"""
        return MetricUnitOfObservation(type=self.unit_of_observation_type)

    def generate_observation_conditions_query_fragment(
        self,
        filter_by_observation_type: bool,
        # TODO(#34498), TODO(#29291): Shouldn't need this value (always assume it is
        #  False) once we are only reading from single observation tables and the single
        #  observation tables do not package their attributes into JSON.
        read_attributes_from_json: bool,
    ) -> str:
        """Returns a query fragment that filters a query that contains observation rows
        based on configured observation conditions.

        Args:
            filter_by_observation_type: Must be True if and only if we're querying from
                the all_* observations view that contains all observations.
            read_attributes_from_json: Whether we're querying from observation rows that
                are storing their attributes in JSON (e.g. the event_attributes column)
                vs having attributes already unpacked into individual columns.
        """
        condition_strings = []

        # TODO(#29291): Shouldn't need to filter by observation_type once we're always
        #  querying from the type-specific view.
        if filter_by_observation_type:
            name_column = observation_type_name_column_for_observation_type(
                self.observation_type
            )
            condition_strings.append(f'{name_column} = "{self.observation_type.value}"')

        for attribute, conditions in self.observation_conditions_dict.items():
            if isinstance(conditions, str):
                attribute_condition_string = conditions
            elif isinstance(conditions, list):
                attribute_condition_string = (
                    f"IN ({list_to_query_string(conditions, quoted=True)})"
                )
            else:
                raise TypeError("All attribute filters must have type str or list[str]")

            if read_attributes_from_json:
                attributes_column = attributes_column_name_for_observation_type(
                    self.observation_type
                )
                condition_string = f"""JSON_EXTRACT_SCALAR({attributes_column}, "$.{attribute}") {attribute_condition_string}"""
            else:
                condition_string = f"""{attribute} {attribute_condition_string}"""

            condition_strings.append(condition_string)

        if not condition_strings:
            return "TRUE"

        # Apply all conditions via AND to a single observation type
        condition_strings_query_fragment = "\n        AND ".join(condition_strings)
        return condition_strings_query_fragment
