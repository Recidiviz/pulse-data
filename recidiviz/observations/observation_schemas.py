# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Shared schema helpers for observation view builders."""

from recidiviz.big_query.big_query_view_column import BigQueryViewColumn, Date
from recidiviz.observations.metric_unit_of_observation import MetricUnitOfObservation
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)


def span_observation_base_schema(
    unit_of_observation_type: MetricUnitOfObservationType,
) -> list[BigQueryViewColumn]:
    """Returns base schema (PKs + dates) for a span observation view.
    Append attribute columns after calling this."""
    obs = MetricUnitOfObservation(type=unit_of_observation_type)
    return [
        *obs.primary_key_columns,
        Date(
            name="start_date",
            description="The start date for this observation span (inclusive).",
            mode="NULLABLE",
        ),
        Date(
            name="end_date",
            description="The end date for this observation span (exclusive).",
            mode="NULLABLE",
        ),
    ]


def event_observation_base_schema(
    unit_of_observation_type: MetricUnitOfObservationType,
) -> list[BigQueryViewColumn]:
    """Returns base schema (PKs + event_date) for an event observation view.
    Append attribute columns after calling this."""
    obs = MetricUnitOfObservation(type=unit_of_observation_type)
    return [
        *obs.primary_key_columns,
        Date(
            name="event_date",
            description="The date the observed event occurred.",
            mode="NULLABLE",
        ),
    ]
