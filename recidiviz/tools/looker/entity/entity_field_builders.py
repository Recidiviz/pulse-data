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
    DimensionLookMLViewField,
    LookMLFieldParameter,
    LookMLViewField,
    MeasureLookMLViewField,
)
from recidiviz.looker.lookml_view_field_parameter import LookMLFieldType
from recidiviz.persistence.entity.entity_metadata_helper import (
    AssociationTableMetadataHelper,
    EntityMetadataHelper,
)

count_field = MeasureLookMLViewField(
    field_name="count",
    parameters=[
        LookMLFieldParameter.type(LookMLFieldType.COUNT),
        LookMLFieldParameter.drill_fields([]),
    ],
)


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
        Constructs LookML fields based on entity attributes and relationships.
        TODO(#23292) Add support for per-entity custom fields
        """
        dimension_fields: List[LookMLViewField] = [
            lookml_view_field_for_schema_field(
                schema_field, self._get_custom_field_params(schema_field.name)
            )
            for schema_field in self.schema_fields
        ]
        measure_fields: List[LookMLViewField] = [count_field]

        return sorted(dimension_fields, key=lambda x: x.field_name) + measure_fields


PRIMARY_KEY_COL = "primary_key"


@attr.define
class AssociationTableLookMLFieldBuilder(LookMLFieldBuilder):
    """Builds LookML fields for an association table based on it's schema fields and associated entities' metadata."""

    metadata: AssociationTableMetadataHelper = attr.ib(
        validator=attr.validators.instance_of(AssociationTableMetadataHelper)
    )
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
        return DimensionLookMLViewField(
            field_name=PRIMARY_KEY_COL,
            parameters=[
                LookMLFieldParameter.type(LookMLFieldType.STRING),
                LookMLFieldParameter.primary_key(True),
                LookMLFieldParameter.sql(
                    f'CONCAT(${{TABLE}}.{associated_class_id_a}, "_", ${{TABLE}}.{associated_class_id_b})'
                ),
            ],
        )

    def build_view_fields(self) -> List[LookMLViewField]:
        """Build LookML fields for an association table.
        TODO(#23292) Add support for per-entity custom fields
        """

        dimension_fields: List[LookMLViewField] = [
            lookml_view_field_for_schema_field(schema_field)
            for schema_field in self.schema_fields
        ] + [self._build_primary_key_field()]
        measure_fields: List[LookMLViewField] = [count_field]

        return sorted(dimension_fields, key=lambda x: x.field_name) + measure_fields
