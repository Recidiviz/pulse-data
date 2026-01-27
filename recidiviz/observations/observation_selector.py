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
import re
from typing import Generic, Sequence

from more_itertools import one

from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.observations.metric_unit_of_observation import MetricUnitOfObservation
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)
from recidiviz.observations.observation_type_utils import (
    ObservationType,
    ObservationTypeT,
    date_column_names_for_observation_type,
    materialized_view_address_for_observation,
    observation_attribute_value_clause,
    observation_type_name_column_for_observation_type,
)
from recidiviz.utils.string_formatting import fix_indent


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
        # TODO(#35914): Shouldn't need this value (always assume it is
        #  False) once we are only reading from single observation tables.
        read_attributes_from_json: bool,
        strip_newlines: bool,
    ) -> str:
        """Returns a query fragment that filters a query that contains observation rows
        based on configured observation conditions.

        Args:
            filter_by_observation_type: Must be True if and only if we're querying from
                the all_* observations view that contains all observations.
            read_attributes_from_json: Whether we're querying from observation rows that
                are storing their attributes in JSON (e.g. the event_attributes column)
                vs having attributes already unpacked into individual columns.
            strip_newlines: If True, the resulting fragment will have newlines removed
                and the full clause will be a single line.
        """
        condition_strings = []

        # TODO(#35914): Shouldn't need to filter by observation_type once we're always
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

            attribute_value_clause = observation_attribute_value_clause(
                observation_type=self.observation_type,
                attribute=attribute,
                read_attributes_from_json=read_attributes_from_json,
            )
            condition_string = (
                f"""{attribute_value_clause} {attribute_condition_string}"""
            )

            condition_strings.append(condition_string)

        if not condition_strings:
            return "TRUE"

        # Apply all conditions via AND to a single observation type
        condition_strings_query_fragment = "\n        AND ".join(condition_strings)

        if strip_newlines:
            condition_strings_query_fragment = re.sub(
                r"\s+|\n+", " ", condition_strings_query_fragment
            )

        return condition_strings_query_fragment

    @classmethod
    def build_selected_observations_query_template(
        cls,
        *,
        observation_type: ObservationType,
        observation_selectors: Sequence["ObservationSelector"],
        output_attribute_columns: list[str],
        include_project_id_format_arg: bool = True,
    ) -> str:
        """Given a set of ObservationSelector that *all have the same observation_type*,
        returns a query template (with a project_id format arg) that will select any
        observation row of that type that matches ANY of the selector conditions.

        Args:
            observation_type: The observation type to query
            observation_selectors: Selectors with the given observation_type
            output_attribute_columns: The set of attribute columns to include in the
                result. All primary key columns for |observation_type| will also be
                returned.
            include_project_id_format_arg: If True, includes project_id in all bigquery
                view addresses. This should generally be True unless the query template
                is being used in the Looker context.
        """
        if not observation_selectors:
            raise ValueError("Must provide at least one selector.")

        for selector in observation_selectors:
            if selector.observation_type != observation_type:
                raise ValueError(
                    f"Can only pass selectors that match the single expected "
                    f"observation type to build_selected_observations_query(). Found "
                    f"selector with observation_type [{selector.observation_type}] "
                    f"which does not match expected type [{observation_type}]."
                )

        unit_of_observation: MetricUnitOfObservation = MetricUnitOfObservation(
            type=observation_type.unit_of_observation_type
        )

        observations_address = materialized_view_address_for_observation(
            observation_type
        )

        source_table = (
            observations_address.format_address_for_query_template()
            if include_project_id_format_arg
            else observations_address.to_str()
        )

        filter_clauses = [
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=False,
                read_attributes_from_json=False,
                strip_newlines=True,
            )
            for selector in observation_selectors
        ]

        if len(filter_clauses) == 1:
            filters_str = one(filter_clauses)
        else:
            filters_str = "\nOR ".join(f"( {clause} )" for clause in filter_clauses)

        output_columns = [
            *unit_of_observation.primary_key_columns_ordered,
            *date_column_names_for_observation_type(observation_type),
            *sorted(output_attribute_columns),
        ]
        output_columns_str = ",\n".join(output_columns)

        return f"""
SELECT
{fix_indent(output_columns_str, indent_level=4)}
FROM 
    `{source_table}`
WHERE
{fix_indent(filters_str, indent_level=4)}
"""
