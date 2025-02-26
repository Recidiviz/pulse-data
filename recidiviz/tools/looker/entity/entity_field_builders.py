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
"""Builders for LookML fields for entities and association tables."""
import abc
from typing import List, Optional

import attr
from google.cloud import bigquery

from recidiviz.common import attr_validators
from recidiviz.looker.lookml_bq_utils import lookml_view_field_for_schema_field
from recidiviz.looker.lookml_view_field import (
    DimensionGroupLookMLViewField,
    DimensionLookMLViewField,
    LookMLFieldParameter,
    LookMLViewField,
    MeasureLookMLViewField,
)
from recidiviz.persistence.entity.entity_metadata_helper import (
    AssociationTableMetadataHelper,
    EntityMetadataHelper,
)
from recidiviz.tools.looker.state.state_dataset_custom_view_fields import (
    StateEntityLookMLCustomFieldProvider,
)


def _rank_for_field_type(f: LookMLViewField) -> int:
    """Returns a rank for a field type to sort dimensions before measures."""
    if isinstance(f, (DimensionLookMLViewField, DimensionGroupLookMLViewField)):
        return 0
    if isinstance(f, MeasureLookMLViewField):
        return 1
    raise NotImplementedError(f"Unsupported field type: {type(f)}")


@attr.define
class LookMLFieldBuilder:
    """
    Abstract base class for building LookML view fields.
    """

    @abc.abstractmethod
    def build_view_fields(self) -> List[LookMLViewField]:
        """Builds and returns a list of LookML view fields."""


@attr.define
class EntityLookMLFieldBuilder(LookMLFieldBuilder):
    """
    Builds LookML fields for an entity's schema fields and the entity's metadata.
    """

    metadata: EntityMetadataHelper = attr.ib(
        validator=attr.validators.instance_of(EntityMetadataHelper)
    )
    # TODO(#23292): Allow support for different types of custom field providers (eg normalized state)
    custom_field_provider: StateEntityLookMLCustomFieldProvider = attr.ib(
        validator=attr.validators.instance_of(StateEntityLookMLCustomFieldProvider)
    )
    table_id: str = attr.ib(validator=attr_validators.is_str)
    schema_fields: List[bigquery.SchemaField] = attr.ib(
        validator=attr_validators.is_list
    )

    def _get_custom_field_params(
        self, column_name: str
    ) -> Optional[List[LookMLFieldParameter]]:
        """
        Returns a list of custom LookMLFieldParameters for a given column.
        We need to denote the primary key for use in joining tables and filtering data,
        and mark the root entity primary key as hidden in order to declutter the explore.
        """
        if column_name == self.metadata.primary_key:
            return [LookMLFieldParameter.primary_key(True)]
        if column_name == self.metadata.root_entity_primary_key:
            return [LookMLFieldParameter.hidden(True)]

        return None

    def build_view_fields(self) -> List[LookMLViewField]:
        """
        Constructs LookML fields based on entity attributes and relationships, as well as any
        custom fields defined for that table. If a custom field and a schema field have the same name,
        the custom field will take precedence.
        Returns a list of LookML view fields sorted alphabetically with dimensions first, then measures.
        """
        custom_fields = self.custom_field_provider.get(self.table_id)
        custom_field_names = {field.field_name for field in custom_fields}

        dimension_fields = [
            lookml_view_field_for_schema_field(
                schema_field, self._get_custom_field_params(schema_field.name)
            )
            for schema_field in self.schema_fields
            if schema_field.name not in custom_field_names
        ]

        return sorted(
            dimension_fields + custom_fields,
            key=lambda f: (_rank_for_field_type(f), f.field_name),
        )


@attr.define
class AssociationTableLookMLFieldBuilder(LookMLFieldBuilder):
    """Builds LookML fields for an association table based on it's schema fields and associated entities' metadata."""

    metadata: AssociationTableMetadataHelper = attr.ib(
        validator=attr.validators.instance_of(AssociationTableMetadataHelper)
    )
    # TODO(#23292): Allow support for different types of custom field providers (eg normalized state)
    custom_field_provider: StateEntityLookMLCustomFieldProvider = attr.ib(
        validator=attr.validators.instance_of(StateEntityLookMLCustomFieldProvider)
    )
    table_id: str = attr.ib(validator=attr_validators.is_str)
    schema_fields: List[bigquery.SchemaField] = attr.ib(
        validator=attr_validators.is_list
    )

    def _build_primary_key_field(self) -> LookMLViewField:
        """Returns a primary key field for the association table. Since there is no primary key in the schema,
        we create a new one by concatenating the two foreign keys. Defining a primary key for a view is best practice
        as it can reduce the total group by clauses in the generated SQL.
        """
        (
            associated_class_id_a,
            associated_class_id_b,
        ) = self.metadata.associated_entities_class_ids
        return DimensionLookMLViewField.for_compound_primary_key(
            associated_class_id_a, associated_class_id_b
        )

    def build_view_fields(self) -> List[LookMLViewField]:
        """Build LookML fields for an association table, as well as any
        custom fields defined for that table. If a custom field and a schema field have the same name,
        the custom field will take precedence.
        Returns a list of LookML view fields sorted alphabetically with dimensions first, then measures.
        """
        custom_fields = self.custom_field_provider.get(self.table_id)
        custom_field_names = {field.field_name for field in custom_fields}

        dimension_fields: List[LookMLViewField] = [
            lookml_view_field_for_schema_field(schema_field)
            for schema_field in self.schema_fields
            if schema_field.name not in custom_field_names
        ] + [self._build_primary_key_field()]

        return sorted(
            dimension_fields + custom_fields,
            key=lambda f: (_rank_for_field_type(f), f.field_name),
        )
