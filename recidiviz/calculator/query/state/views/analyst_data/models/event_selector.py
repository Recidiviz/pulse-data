# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Defines EventSelector object used to filter rows from an events table"""

from typing import Dict, List, Sequence, Union

import attr

from recidiviz.calculator.query.sessions_query_fragments import list_to_query_string
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType
from recidiviz.observations.metric_unit_of_observation import MetricUnitOfObservation
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)


@attr.define(frozen=True, kw_only=True)
class EventSelector:
    """
    Class that stores information about conditions on a events table, in order to
    generate query fragments for filtering events.
    """

    # The EventType used to filter to a subset of events
    event_type: EventType

    # Dictionary mapping event attributes to their associated conditions
    event_conditions_dict: Dict[str, Union[List[str], str]] = attr.ib()

    @property
    def unit_of_observation_type(self) -> MetricUnitOfObservationType:
        """Returns the MetricUnitOfObservationType associated with the event type"""
        return self.event_type.unit_of_observation_type

    @property
    def unit_of_observation(self) -> MetricUnitOfObservation:
        """Returns the MetricUnitOfObservation object associated with the event type"""
        return MetricUnitOfObservation(type=self.unit_of_observation_type)

    def generate_event_conditions_query_fragment(
        self, filter_by_event_type: bool
    ) -> str:
        """Returns a query fragment that filters a query based on configured event conditions"""
        condition_strings = []

        # TODO(#29291): Shouldn't need to filter by span_type once we're querying from
        #  the type-specific view. In order to do this, we will need to support having
        #  an empty conditions fragment returned (or throw if this is called when
        #  self.event_conditions_dict is empty).
        if filter_by_event_type:
            condition_strings.append(f'event = "{self.event_type.value}"')

        for attribute, conditions in self.event_conditions_dict.items():
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
                f"""JSON_EXTRACT_SCALAR(event_attributes, "$.{attribute}") {attribute_condition_string}"""
            )
        # Apply all conditions via AND to a single event type
        condition_strings_query_fragment = "\n        AND ".join(condition_strings)
        return condition_strings_query_fragment

    def generate_event_selector_query(self) -> str:
        """Returns a standalone query that filters the appropriate events table based on configured event conditions"""
        event_observations_address = (
            EventObservationBigQueryViewBuilder.materialized_view_address_for_event(
                self.event_type
            )
        )
        conditions_fragment = self.generate_event_conditions_query_fragment(
            filter_by_event_type=False
        )
        filter_clause = (
            f"WHERE {conditions_fragment}" if self.event_conditions_dict else ""
        )

        return f"""
SELECT *, end_date AS end_date_exclusive
FROM `{{project_id}}.{event_observations_address.to_str()}`
{filter_clause}
"""
