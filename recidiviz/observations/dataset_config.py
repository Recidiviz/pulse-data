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
"""Helpers for getting observation view datasets."""
from typing import Type

from recidiviz.observations.event_type import EventType
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)
from recidiviz.observations.span_type import SpanType


def dataset_for_observation_type(observation_type: SpanType | EventType) -> str:
    """Returns the dataset for an observations view with the given observation type."""
    return dataset_for_observation_type_cls(
        unit_of_observation=observation_type.unit_of_observation_type,
        observation_type_cls=type(observation_type),
    )


def dataset_for_observation_type_cls(
    unit_of_observation: MetricUnitOfObservationType,
    observation_type_cls: Type[SpanType] | Type[EventType],
) -> str:
    """Returns the dataset for any observations view with the given
    unit_of_observation and observation_type class.
    """
    observation_category = observation_type_cls.observation_type_category()
    unit_of_observation_str = unit_of_observation.value.lower()
    return f"observations__{unit_of_observation_str}_{observation_category}"
