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

from recidiviz.big_query.big_query_view_column import (
    BigQueryViewColumn,
    Integer,
    String,
)
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)

_STATE_CODE_COLUMN = String(
    name="state_code",
    description="The U.S. state code for the unit being observed.",
    mode="NULLABLE",
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
            self.primary_key_column_names_ordered, table_prefix=prefix
        )

    @property
    def primary_key_columns(self) -> list[BigQueryViewColumn]:
        """List of columns (with explicit types and descriptions) that serve as the
        primary keys of a table containing information about the unit of observation.

        Columns are returned in a stable order suitable for use in JOIN / WHERE clauses
        and as clustering fields for observation view output (state_code is last because
        it is the least-differentiated value, see
        https://cloud.google.com/blog/topics/developers-practitioners/bigquery-admin-reference-guide-query-optimization).
        """
        match self.type:
            case MetricUnitOfObservationType.PERSON_ID:
                return [
                    Integer(
                        name="person_id",
                        description="The Recidiviz internal person id for the person being observed.",
                        mode="NULLABLE",
                    ),
                    _STATE_CODE_COLUMN,
                ]
            case MetricUnitOfObservationType.SUPERVISION_OFFICER:
                return [
                    String(
                        name="officer_id",
                        description="The external id for the supervision officer being observed.",
                        mode="NULLABLE",
                    ),
                    _STATE_CODE_COLUMN,
                ]
            case MetricUnitOfObservationType.WORKFLOWS_PRIMARY_USER:
                return [
                    String(
                        name="email_address",
                        description="The email address for the Workflows primary user being observed.",
                        mode="NULLABLE",
                    ),
                    _STATE_CODE_COLUMN,
                ]
            case MetricUnitOfObservationType.WORKFLOWS_PROVISIONED_USER:
                return [
                    String(
                        name="email_address",
                        description="The email address for the Workflows provisioned user being observed.",
                        mode="NULLABLE",
                    ),
                    _STATE_CODE_COLUMN,
                ]
            case MetricUnitOfObservationType.WORKFLOWS_SURFACEABLE_CASELOAD:
                return [
                    String(
                        name="caseload_id",
                        description="The id for the Workflows surfaceable caseload being observed.",
                        mode="NULLABLE",
                    ),
                    _STATE_CODE_COLUMN,
                ]
            case MetricUnitOfObservationType.INSIGHTS_PRIMARY_USER:
                return [
                    String(
                        name="email_address",
                        description="The email address for the Insights primary user being observed.",
                        mode="NULLABLE",
                    ),
                    _STATE_CODE_COLUMN,
                ]
            case MetricUnitOfObservationType.INSIGHTS_PROVISIONED_USER:
                return [
                    String(
                        name="email_address",
                        description="The email address for the Insights provisioned user being observed.",
                        mode="NULLABLE",
                    ),
                    _STATE_CODE_COLUMN,
                ]
            case MetricUnitOfObservationType.TASKS_PRIMARY_USER:
                return [
                    String(
                        name="email_address",
                        description="The email address for the Tasks primary user being observed.",
                        mode="NULLABLE",
                    ),
                    _STATE_CODE_COLUMN,
                ]
            case MetricUnitOfObservationType.TASKS_PROVISIONED_USER:
                return [
                    String(
                        name="email_address",
                        description="The email address for the Tasks provisioned user being observed.",
                        mode="NULLABLE",
                    ),
                    _STATE_CODE_COLUMN,
                ]
            case MetricUnitOfObservationType.GLOBAL_PROVISIONED_USER:
                return [
                    String(
                        name="email_address",
                        description="The email address for the globally provisioned user being observed.",
                        mode="NULLABLE",
                    ),
                    _STATE_CODE_COLUMN,
                ]
            case MetricUnitOfObservationType.JII_TABLET_APP_PROVISIONED_USER:
                return [
                    Integer(
                        name="person_id",
                        description="The Recidiviz internal person id for the JII tablet app provisioned user being observed.",
                        mode="NULLABLE",
                    ),
                    _STATE_CODE_COLUMN,
                ]

    @property
    def primary_key_column_names(self) -> FrozenSet[str]:
        """Set of column names that serve as the primary keys of a table containing
        information about the unit of observation.
        """
        return frozenset(c.name for c in self.primary_key_columns)

    @property
    def primary_key_column_names_ordered(self) -> list[str]:
        """Provides primary key column names in a stable order that should be used in
        JOIN / WHERE clauses and will be used as the clustering order for observation
        view output.
        """
        if "state_code" not in self.primary_key_column_names:
            raise ValueError(
                f"Expected all primary_key_columns for unit of observation type "
                f"[{self}] to include a state_code column."
            )
        return [c.name for c in self.primary_key_columns]
