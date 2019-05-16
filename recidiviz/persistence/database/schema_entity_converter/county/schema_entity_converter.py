# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""
Defines a class for converting between county-specific Entity and schema Base
objects.
"""

from types import ModuleType

from recidiviz.persistence.database.schema_entity_converter. \
    base_schema_entity_converter import (
        BaseSchemaEntityConverter,
        FieldNameType,
    )

from recidiviz.persistence.entity.county import entities
from recidiviz.persistence.database.schema.county import schema


class CountySchemaEntityConverter(BaseSchemaEntityConverter):
    CLASS_RANK_LIST = [
        entities.Person.__name__,
        entities.Booking.__name__,
        # TODO(1625): Update with full ranking here and write tests
    ]

    def __init__(self):
        super().__init__(self.CLASS_RANK_LIST)

    def _get_schema_module(self) -> ModuleType:
        return schema

    def _get_entities_module(self) -> ModuleType:
        return entities

    def _should_skip_field(self, field: FieldNameType) -> bool:
        # TODO(1145): Correctly convert related_sentences once schema
        # for this field is finalized.
        return field == 'related_sentences'
