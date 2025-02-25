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
"""Tests for MetricUnitOfObservation."""
import unittest

from recidiviz.observations.metric_unit_of_observation import MetricUnitOfObservation
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)


class MetricUnitOfObservationTest(unittest.TestCase):
    """Tests for MetricUnitOfObservation."""

    def test_get_index_columns_query_string(self) -> None:
        my_metric_observation_level = MetricUnitOfObservation(
            type=MetricUnitOfObservationType.SUPERVISION_OFFICER,
        )
        query_string = my_metric_observation_level.get_primary_key_columns_query_string(
            prefix="my_prefix"
        )
        expected_query_string = "my_prefix.state_code, my_prefix.officer_id"
        self.assertEqual(query_string, expected_query_string)
