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
"""Utils for getting information about a given observation type (EventType or SpanType).
"""
from typing import TypeVar

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType
from recidiviz.observations.span_observation_big_query_view_builder import (
    SpanObservationBigQueryViewBuilder,
)
from recidiviz.observations.span_type import SpanType

ObservationTypeT = TypeVar("ObservationTypeT", SpanType, EventType)


def attributes_column_name_for_observation_type(
    observation_type: EventType | SpanType,
) -> str:
    """Returns the column in any observation view that contains JSON-packaged attributes
    about that observation.
    """
    if isinstance(observation_type, EventType):
        return EventObservationBigQueryViewBuilder.EVENT_ATTRIBUTES_OUTPUT_COL_NAME
    if isinstance(observation_type, SpanType):
        return SpanObservationBigQueryViewBuilder.SPAN_ATTRIBUTES_OUTPUT_COL_NAME

    raise ValueError(
        f"Unexpected type [{type(observation_type)}] for observation_type "
        f"[{observation_type}]"
    )


# TODO(#34498), TODO(#29291): We should be able to delete this clause (it will become
#  trivial) once we are only reading from single observation tables and the single
#  observation tables do not package their attributes into JSON.
def observation_attribute_value_clause(
    observation_type: EventType | SpanType,
    attribute: str,
    read_attributes_from_json: bool,
) -> str:
    """Gives a SQL clause that will return the value of a given observation
    attribute.
    """
    if not read_attributes_from_json:
        return attribute
    attributes_column = attributes_column_name_for_observation_type(observation_type)
    return f'JSON_EXTRACT_SCALAR({attributes_column}, "$.{attribute}")'


def observation_type_name_column_for_observation_type(
    observation_type: EventType | SpanType,
) -> str:
    """Returns the column in any unioned all_* observation views (e.g. all_person_spans)
    that gives the observation type for a given row.
    """
    if isinstance(observation_type, EventType):
        return "event"
    if isinstance(observation_type, SpanType):
        return "span"

    raise ValueError(
        f"Unexpected type [{type(observation_type)}] for observation_type "
        f"[{observation_type}]"
    )


def materialized_view_address_for_observation(
    observation_type: EventType | SpanType,
) -> BigQueryAddress:
    """The BigQuery address that should be used to query for observations of the given
    type.
    """
    if isinstance(observation_type, EventType):
        return EventObservationBigQueryViewBuilder.materialized_view_address_for_event(
            observation_type
        )
    if isinstance(observation_type, SpanType):
        return SpanObservationBigQueryViewBuilder.materialized_view_address_for_span(
            observation_type
        )

    raise ValueError(
        f"Unexpected type [{type(observation_type)}] for observation_type "
        f"[{observation_type}]"
    )
