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
"""Code for building LookML explores for entities."""
from typing import Dict, List, Tuple, Type

import attr

from recidiviz.looker.lookml_explore import LookMLExplore
from recidiviz.looker.lookml_explore_parameter import (
    ExploreParameterJoin,
    JoinCardinality,
    LookMLExploreParameter,
    LookMLJoinParameter,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entities_module_context import EntitiesModuleContext
from recidiviz.persistence.entity.entity_utils import (
    get_association_table_id,
    get_child_entity_classes,
    is_many_to_many_relationship,
    is_many_to_one_relationship,
    is_one_to_many_relationship,
)
from recidiviz.persistence.entity.root_entity_utils import (
    get_root_entity_class_for_entity,
)

STATE_GROUP_LABEL = "State"


def explore_name_for_root_entity(root_entity_cls: Type[Entity]) -> str:
    """Returns the LookML explore name for the given root entity class.
    Adds template suffix to make clear that the explore needs to be extended."""
    return f"{root_entity_cls.get_entity_name()}_template"


@attr.define
class EntityLookMLExploreBuilder:
    """Builds a LookML explore template for a root entity class and it's child entities.
    Generates an explore for every entity, and assumes all entities should be joined
    using a left join (the LookML default).

    https://cloud.google.com/looker/docs/reference/param-explore-explore

    Attributes:
        module_context: The context for the entities module.
        root_entity_cls: The root entity class for which to build the LookML explore.
        group_label: The group label for the LookML explore used to group explores in the Looker UI.
        _table_name_to_explore: A mapping from table name to LookML explore.
    """

    module_context: EntitiesModuleContext = attr.ib()
    root_entity_cls: Type[Entity] = attr.ib(validator=attr.validators.instance_of(type))
    group_label: str = attr.ib(default=STATE_GROUP_LABEL)
    _entity_name_to_explore: Dict[str, LookMLExplore] = attr.ib(factory=dict)

    @staticmethod
    def _build_join_parameter(
        parent_table: str,
        child_table: str,
        join_field: str,
        join_cardinality: JoinCardinality,
    ) -> ExploreParameterJoin:
        sql_on_text = (
            f"${{{parent_table}.{join_field}}} = ${{{child_table}.{join_field}}}"
        )
        sql_on = LookMLJoinParameter.sql_on(sql_on_text)

        return ExploreParameterJoin(
            view_name=child_table,
            join_params=[sql_on, LookMLJoinParameter.relationship(join_cardinality)],
        )

    @classmethod
    def _build_association_table_explore(
        cls, association_table_id: str, child_cls: Type[Entity]
    ) -> LookMLExplore:
        join_parameter = cls._build_join_parameter(
            parent_table=association_table_id,
            child_table=child_cls.get_entity_name(),
            join_field=child_cls.get_class_id_name(),
            join_cardinality=JoinCardinality.MANY_TO_ONE,
        )
        return LookMLExplore(
            explore_name=association_table_id,
            parameters=[join_parameter],
            extension_required=True,
        )

    @staticmethod
    def _get_entity_relationship_cardinality(
        parent_cls: Type[Entity], child_cls: Type[Entity]
    ) -> JoinCardinality:
        """Returns the cardinality of the relationship between the parent and child entities."""
        if is_one_to_many_relationship(parent_cls, child_cls):
            return JoinCardinality.ONE_TO_MANY
        if is_many_to_one_relationship(parent_cls, child_cls):
            return JoinCardinality.MANY_TO_ONE
        if is_many_to_many_relationship(parent_cls, child_cls):
            return JoinCardinality.MANY_TO_MANY
        return JoinCardinality.ONE_TO_ONE

    def _build_explores(
        self,
        entity_cls: Type[Entity],
    ) -> None:
        """Builds LookML explores for the given entity class and its related entities."""
        table_name = entity_cls.get_entity_name()
        if table_name in self._entity_name_to_explore:
            return

        explore = LookMLExplore(
            explore_name=table_name, parameters=[], extension_required=True
        )
        self._entity_name_to_explore[table_name] = explore

        def _join_details(
            entity_relationship_cardinality: JoinCardinality,
            parent_cls: Type[Entity],
            child_cls: Type[Entity],
        ) -> Tuple[str, JoinCardinality]:
            """Returns the join table name and cardinality for the relationship.
            If the entities have a many-to-many relationship, we need to join through an association table.
            """
            if entity_relationship_cardinality == JoinCardinality.MANY_TO_MANY:
                return (
                    get_association_table_id(
                        parent_cls, child_cls, self.module_context
                    ),
                    JoinCardinality.ONE_TO_MANY,
                )

            return child_cls.get_entity_name(), entity_relationship_cardinality

        for child_entity_cls in sorted(
            get_child_entity_classes(entity_cls, self.module_context),
            key=lambda cls: cls.__name__,
        ):
            entity_relationship_cardinality = self._get_entity_relationship_cardinality(
                entity_cls, child_entity_cls
            )
            join_table, join_cardinality = _join_details(
                entity_relationship_cardinality, entity_cls, child_entity_cls
            )

            explore.parameters.append(
                self._build_join_parameter(
                    parent_table=table_name,
                    child_table=join_table,
                    join_field=entity_cls.get_class_id_name(),
                    join_cardinality=join_cardinality,
                )
            )
            explore.extended_explores.append(join_table)

            if entity_relationship_cardinality == JoinCardinality.MANY_TO_MANY:
                self._entity_name_to_explore[
                    join_table
                ] = self._build_association_table_explore(join_table, child_entity_cls)
            else:
                self._build_explores(child_entity_cls)

    def _fill_root_explore_details(
        self,
    ) -> None:
        """Fills in the details for the root explore."""
        entity_name = self.root_entity_cls.get_entity_name()
        root_explore = self._entity_name_to_explore[entity_name]

        root_explore.explore_name = explore_name_for_root_entity(self.root_entity_cls)
        # View name needs to be specified since explore name is different
        root_explore.view_name = entity_name
        root_explore.parameters = [
            # Group label is used to group explores in the Looker UI
            LookMLExploreParameter.group_label(self.group_label),
            *root_explore.parameters,
        ]

    def build(self) -> List[LookMLExplore]:
        """Builds a LookML explore template for a root entity class and it's child entities."""
        if self.root_entity_cls != get_root_entity_class_for_entity(
            self.root_entity_cls
        ):
            raise ValueError(
                f"Entity class [{self.root_entity_cls.__name__}] is not a root entity"
            )

        self._build_explores(self.root_entity_cls)
        self._fill_root_explore_details()

        return list(self._entity_name_to_explore.values())
