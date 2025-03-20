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
"""Manages custom derived table views and their join relationships for a given dataset."""
from types import ModuleType
from typing import Type

import attr
from google.cloud.bigquery import SchemaField

from recidiviz.common import attr_validators
from recidiviz.looker.lookml_explore_parameter import ExploreParameterJoin
from recidiviz.looker.lookml_view import LookMLView
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.tools.looker.entity.custom_views.entity_custom_view import (
    EntityCustomViewBuilder,
    EntityCustomViewJoinProvider,
)
from recidiviz.tools.looker.entity.custom_views.person_periods import (
    PersonPeriodsJoinProvider,
    PersonPeriodsLookMLViewBuilder,
)


@attr.define
class EntityCustomViewManager:
    """
    Manages custom derived table views and their join relationships for a given dataset.

    Attributes:
        dataset_id (str): The ID of the BigQuery dataset.
        view_builders (List[Type[CustomViewBuilder]]): List of view builder classes.
        join_providers (List[JoinRelationshipProvider]): List of join relationship providers.
    """

    dataset_id: str = attr.ib(validator=attr_validators.is_str)
    view_builders: list[Type[EntityCustomViewBuilder]] = attr.ib(
        validator=attr_validators.is_list
    )
    join_providers: list[EntityCustomViewJoinProvider] = attr.ib(
        validator=attr_validators.is_list
    )

    def build_custom_views(
        self, schema_map: dict[str, list[SchemaField]]
    ) -> list["LookMLView"]:
        """Returns list of manually defined derived table LookML views"""
        return [
            builder.from_schema(
                dataset_id=self.dataset_id, bq_schema=schema_map
            ).build()
            for builder in self.view_builders
        ]

    def get_custom_view_join_relationships(
        self, entity_cls: Type["Entity"]
    ) -> list[ExploreParameterJoin]:
        """Returns a list of join relationships between the provided entity and any custom views."""
        results = []
        for provider in self.join_providers:
            if relationship := provider.get_join_relationship(entity_cls):
                results.append(relationship)
        return results


def get_entity_custom_view_manager(
    dataset_id: str, entities_module: ModuleType
) -> EntityCustomViewManager:
    if entities_module in [state_entities, normalized_entities]:
        return EntityCustomViewManager(
            dataset_id=dataset_id,
            view_builders=[PersonPeriodsLookMLViewBuilder],
            join_providers=[PersonPeriodsJoinProvider(dataset_id)],
        )
    raise ValueError(f"Unsupported entities module [{entities_module.__name__}]")
