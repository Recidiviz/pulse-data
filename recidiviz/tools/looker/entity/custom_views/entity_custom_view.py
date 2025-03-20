# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Abstract base classes for building custom derived table views
and providing join relationships between custom views and their parent view."""
import abc
from typing import Type

from recidiviz.looker.lookml_explore_parameter import ExploreParameterJoin
from recidiviz.looker.lookml_view import LookMLView
from recidiviz.persistence.entity.base_entity import Entity


class EntityCustomViewBuilder:
    """Abstract base class for building a custom derived table view."""

    @classmethod
    @abc.abstractmethod
    def from_schema(cls, dataset_id: str, bq_schema: dict) -> "EntityCustomViewBuilder":
        """Creates a custom view builder from the given dataset and BigQuery schema."""

    @abc.abstractmethod
    def build(self) -> LookMLView:
        """Builds the view."""


class EntityCustomViewJoinProvider:
    """Abstract base class for providing join relationships between custom views and their parent view."""

    @abc.abstractmethod
    def get_join_relationship(
        self, entity_cls: Type[Entity]
    ) -> ExploreParameterJoin | None:
        """If the provided |entity_cls| is the parent view of the custom view the class represents,
        returns the join relationship between the parent view and the custom view."""
