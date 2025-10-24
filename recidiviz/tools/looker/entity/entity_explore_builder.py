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
from recidiviz.tools.looker.entity.entity_custom_view_manager import (
    EntityCustomViewManager,
    get_entity_custom_view_manager,
)
from recidiviz.tools.looker.entity.entity_views_builder import (
    view_name_for_entity_table,
)

STATE_GROUP_LABEL = "State"
DEPRECATED_MULTIPARENT_ENTITIES = {
    # TODO(#51203) Remove once multi-parent entities are supported or state_charge is fully deprecated
    "state_charge",
    "normalized_state_charge",
    # TODO(#20526) Remove once multi-parent entities are supported or state_early_discharge is migrated
    # to hang off state_person
    "state_early_discharge",
    "normalized_state_early_discharge",
}


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
        custom_view_manager: The custom view manager for the dataset, used to get custom view join relationships.
        group_label: The group label for the LookML explore used to group explores in the Looker UI.
        _view_name_to_explore: A mapping from view name to LookML explore.
    """

    module_context: EntitiesModuleContext = attr.ib()
    root_entity_cls: Type[Entity] = attr.ib(validator=attr.validators.instance_of(type))
    custom_view_manager: EntityCustomViewManager = attr.ib(
        validator=attr.validators.instance_of(EntityCustomViewManager)
    )
    group_label: str = attr.ib(default=STATE_GROUP_LABEL)
    _view_name_to_explore: Dict[str, LookMLExplore] = attr.ib(factory=dict)

    @staticmethod
    def _build_association_table_explore(
        association_table_id: str, child_cls: Type[Entity]
    ) -> LookMLExplore:
        join_parameter = ExploreParameterJoin.build_join_parameter(
            parent_view=association_table_id,
            child_view=child_cls.get_entity_name(),
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
        view_name = entity_cls.get_entity_name()
        if (
            view_name in self._view_name_to_explore
            # It's a non-trivial fix to allow for multiparent entities
            # since the only offending entities are in the process of being
            # deprecated, we can just note that the results may be incorrect
            # on the dashboard
            and view_name not in DEPRECATED_MULTIPARENT_ENTITIES
        ):
            raise ValueError(
                f"Looker view [{view_name}] should not have multiple parents."
                " Looker only joins a view to the root entity via a single path when"
                " pulling data into a dashboard, which may result in missing rows"
            )

        explore = LookMLExplore(
            explore_name=view_name, parameters=[], extension_required=True
        )
        self._view_name_to_explore[view_name] = explore

        if relationships := self.custom_view_manager.get_custom_view_join_relationships(
            entity_cls
        ):
            explore.parameters.extend(relationships)

        def _join_details(
            entity_relationship_cardinality: JoinCardinality,
            parent_cls: Type[Entity],
            child_cls: Type[Entity],
        ) -> Tuple[str, JoinCardinality]:
            """Returns the join table view name and cardinality for the relationship.
            If the entities have a many-to-many relationship, we need to join through an association table view.
            """
            if entity_relationship_cardinality == JoinCardinality.MANY_TO_MANY:
                association_table_id = get_association_table_id(
                    parent_cls, child_cls, self.module_context
                )
                return (
                    view_name_for_entity_table(
                        self.module_context.entities_module(), association_table_id
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
            join_table_view, join_cardinality = _join_details(
                entity_relationship_cardinality, entity_cls, child_entity_cls
            )

            explore.parameters.append(
                ExploreParameterJoin.build_join_parameter(
                    parent_view=view_name,
                    child_view=join_table_view,
                    join_field=entity_cls.get_class_id_name(),
                    join_cardinality=join_cardinality,
                )
            )
            explore.extended_explores.append(join_table_view)

            if entity_relationship_cardinality == JoinCardinality.MANY_TO_MANY:
                self._view_name_to_explore[
                    join_table_view
                ] = self._build_association_table_explore(
                    join_table_view, child_entity_cls
                )
            else:
                self._build_explores(child_entity_cls)

    def _fill_root_explore_details(
        self,
    ) -> None:
        """Fills in the details for the root explore."""
        entity_name = self.root_entity_cls.get_entity_name()
        root_explore = self._view_name_to_explore[entity_name]

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

        return list(self._view_name_to_explore.values())


def generate_entity_explores(
    module_context: EntitiesModuleContext,
    root_entity_cls: Type[Entity],
    dataset_id: str,
) -> List[LookMLExplore]:
    """Generates LookML explores for the given root entity class and it's child entities."""
    return EntityLookMLExploreBuilder(
        module_context=module_context,
        root_entity_cls=root_entity_cls,
        custom_view_manager=get_entity_custom_view_manager(
            dataset_id, module_context.entities_module()
        ),
    ).build()
