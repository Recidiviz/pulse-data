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
"""Class that stores information about a unit of observation, along with functions
to help generate SQL fragments.
"""
from typing import FrozenSet, Optional

import attr

from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)


@attr.define(frozen=True, kw_only=True)
class MetricUnitOfObservation:
    """Class that stores information about a unit of observation, along with functions
    to help generate SQL fragments.
    """

    # The enum for the type of unit of observation
    type: MetricUnitOfObservationType

    def get_primary_key_columns_query_string(self, prefix: Optional[str] = None) -> str:
        """Returns string containing comma separated primary key column names with optional prefix"""
        return list_to_query_string(
            self.primary_key_columns_ordered, table_prefix=prefix
        )

    @property
    def primary_key_columns(self) -> FrozenSet[str]:
        """List of columns that serve as the primary keys of a table containing
        information about the unit of observation.
        """
        match self.type:
            case MetricUnitOfObservationType.PERSON_ID:
                return frozenset(["state_code", "person_id"])
            case MetricUnitOfObservationType.SUPERVISION_OFFICER:
                return frozenset(["state_code", "officer_id"])
            case MetricUnitOfObservationType.WORKFLOWS_USER:
                return frozenset(["state_code", "email_address"])

    @property
    def primary_key_columns_ordered(self) -> list[str]:
        if "state_code" not in self.primary_key_columns:
            raise ValueError(
                f"Expected all primary_key_columns for unit of observation type "
                f"[{self}] to include a state_code column."
            )

        return ["state_code"] + sorted(
            c for c in self.primary_key_columns if c != "state_code"
        )