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
"""Builder for an entity LookML dashboard."""
from typing import Type

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.attr_mixins import attribute_field_type_reference_for_class
from recidiviz.common.str_field_utils import snake_to_title
from recidiviz.looker.lookml_dashboard import LookMLDashboard
from recidiviz.looker.lookml_dashboard_builder import (
    LookMLDashboardElementMetadata,
    SingleExploreLookMLDashboardBuilder,
)
from recidiviz.looker.lookml_dashboard_element import dict_to_scoped_field_names
from recidiviz.looker.lookml_view import LookMLView
from recidiviz.looker.lookml_view_source_table import SqlTableAddress
from recidiviz.persistence.database.schema_utils import is_association_table
from recidiviz.persistence.entity.base_entity import (
    Entity,
    EnumEntity,
    ExternalIdEntity,
    HasMultipleExternalIdsEntity,
)
from recidiviz.persistence.entity.entities_module_context import EntitiesModuleContext
from recidiviz.persistence.entity.entity_utils import (
    get_child_entity_classes,
    get_entity_class_in_module_with_table_id,
    get_external_id_entity_class,
)
from recidiviz.persistence.entity.root_entity_utils import (
    get_root_entity_class_for_entity,
)
from recidiviz.persistence.entity.state.state_entity_mixins import StateEntityMixin
from recidiviz.tools.looker.constants import PROJECT_ID_VAR_STRING
from recidiviz.tools.looker.entity.state_dataset_dashboard_element_providers import (
    get_elements_provider,
)


@attr.define
class EntityLookMLDashboardBuilder:
    """Builder for an entity LookML dashboard."""

    module_context: EntitiesModuleContext
    root_entity_cls: Type[Entity]
    views: list[LookMLView]
    dataset_id: str

    @property
    def root_entity_name(self) -> str:
        return self.root_entity_cls.get_entity_name()

    def _get_filter_fields(self) -> list[str]:
        """Returns the filter fields for the dashboard consisting of the root entity's
        primary key, state entity fields and the external id entity fields.
        """
        filter_fields: dict[str, list[str]] = {}

        if not issubclass(self.root_entity_cls, StateEntityMixin):
            raise ValueError(
                f"Entity {self.root_entity_cls} is not a subclass of StateEntityMixin."
            )

        filter_fields[self.root_entity_name] = [
            self.root_entity_cls.get_primary_key_column_name()
        ] + attribute_field_type_reference_for_class(StateEntityMixin).sorted_fields

        if not issubclass(self.root_entity_cls, HasMultipleExternalIdsEntity):
            raise ValueError(
                f"Entity {self.root_entity_cls} is not a subclass of HasMultipleExternalIdsEntity."
            )

        external_id_entity_cls = get_external_id_entity_class(
            module_context=self.module_context, root_entity_cls=self.root_entity_cls
        )
        if not issubclass(external_id_entity_cls, ExternalIdEntity):
            raise ValueError(
                f"Entity {external_id_entity_cls} is not a subclass of ExternalIdEntity."
            )

        filter_fields[
            external_id_entity_cls.get_entity_name()
        ] = attribute_field_type_reference_for_class(ExternalIdEntity).sorted_fields

        return dict_to_scoped_field_names(filter_fields)

    def _build_dashboard_element_metadata(self) -> list[LookMLDashboardElementMetadata]:
        """Builds the metadata for the dashboard elements. Includes all views that have the root_entity_cls
        as its root entity and do not represent association tables or EnumEntities"""
        metadata: list[LookMLDashboardElementMetadata] = []
        for view in self.views:
            table = view.table
            if not isinstance(table, SqlTableAddress) or not isinstance(
                table.address, BigQueryAddress
            ):
                # Skip views that are not based on BigQuery tables
                continue

            table_id = table.address.table_id
            if is_association_table(table_name=table_id):
                continue

            entity_cls = get_entity_class_in_module_with_table_id(
                self.module_context.entities_module(), table_id=table_id
            )

            if get_root_entity_class_for_entity(
                entity_cls
            ) != self.root_entity_cls or issubclass(entity_cls, EnumEntity):
                continue

            element_metadata = LookMLDashboardElementMetadata.from_view(view)
            for child_cls in get_child_entity_classes(
                entity_cls=entity_cls, entities_module_context=self.module_context
            ):
                # TODO(#40272) Aggregate enum values as list of strings
                if issubclass(child_cls, EnumEntity):
                    element_metadata.add_count_field(
                        view_name=child_cls.get_entity_name()
                    )

            metadata.append(element_metadata)

        return metadata

    def build_and_validate(self) -> LookMLDashboard:
        """Builds the entity dashboard and validates that all fields referenced
        by the dashboard exist in the corresponding views."""
        filter_fields = self._get_filter_fields()
        dashboard = SingleExploreLookMLDashboardBuilder(
            dashboard_name=self.root_entity_name,
            dashboard_title=snake_to_title(self.root_entity_name),
            explore_name=self.root_entity_name,
            model_name=PROJECT_ID_VAR_STRING,
            filter_fields=filter_fields,
            element_provider=get_elements_provider(
                self.root_entity_cls,
                self.dataset_id,
                self._build_dashboard_element_metadata(),
            ),
        ).build()

        dashboard.validate_referenced_fields_exist_in_views(self.views)

        return dashboard
