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
"""Tests for EntityCustomFieldRegistry."""
import unittest

from recidiviz.persistence.entity.entity_utils import (
    get_entity_class_in_module_with_table_id,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.tools.looker.entity.entity_custom_field_registry import (
    STATE_CUSTOM_FIELDS,
)


class TestEntityCustomFieldRegistry(unittest.TestCase):
    """Tests for EntityCustomFieldRegistry."""

    # TODO(#39355) Add tests for normalized entities

    def test_valid_state_tables(self) -> None:
        for table in STATE_CUSTOM_FIELDS:
            _ = get_entity_class_in_module_with_table_id(
                entities_module=state_entities, table_id=table
            )
