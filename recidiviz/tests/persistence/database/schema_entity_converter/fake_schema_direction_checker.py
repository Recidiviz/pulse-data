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
"""Defines a class hierarchy / SchemaEdgeDirectionChecker for the schema represented in
fake_entities.py / fake_schema.py
"""
from recidiviz.persistence.entity.schema_edge_direction_checker import (
    SchemaEdgeDirectionChecker,
)
from recidiviz.tests.persistence.database.schema_entity_converter import (
    fake_entities as entities,
)

CLASS_HIERARCHY = [
    entities.Root.__name__,
    entities.Parent.__name__,
    entities.Child.__name__,
    entities.Toy.__name__,
]

FAKE_SCHEMA_DIRECTION_CHECKER = SchemaEdgeDirectionChecker(CLASS_HIERARCHY)
