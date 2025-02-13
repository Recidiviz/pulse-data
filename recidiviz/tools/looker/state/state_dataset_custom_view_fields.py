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
"""Provides custom LookML fields for a state entity."""
from typing import List

import attr

from recidiviz.big_query.big_query_schema_validator import BigQuerySchemaValidator
from recidiviz.looker.lookml_field_factory import LookMLFieldFactory
from recidiviz.looker.lookml_field_registry import LookMLFieldRegistry
from recidiviz.looker.lookml_view_field import LookMLViewField


@attr.define
class StateEntityLookMLCustomFieldProvider:
    """
    Class to provide custom LookML fields for a state entity.
    TODO(#23292) just a stub for now, will be implemented in a follow-up PR.
    """

    field_validator: BigQuerySchemaValidator = attr.ib(
        validator=attr.validators.instance_of(BigQuerySchemaValidator)
    )
    _registry: LookMLFieldRegistry = attr.ib(factory=LookMLFieldRegistry)

    def get(self, _table_id: str) -> List[LookMLViewField]:
        return [LookMLFieldFactory.count_measure()]
