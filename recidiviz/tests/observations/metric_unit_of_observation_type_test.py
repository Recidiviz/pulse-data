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
"""Tests for MetricUnitOfObservationType."""
import re
import unittest

from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)


class MetricUnitOfObservationTypeTest(unittest.TestCase):
    """Tests for MetricUnitOfObservationType."""

    # check that short_name only has valid character types
    def test_short_name_char_types(self) -> None:
        for unit_of_observation_type in MetricUnitOfObservationType:
            if not re.match(r"^\w+$", unit_of_observation_type.short_name):
                raise ValueError(
                    "All characters in MetricUnitOfObservationType value must be alphanumeric or underscores."
                )
