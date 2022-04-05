# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Defines a type that represents the (dataset_id, table_id) address of a BigQuery view
or table.
"""

import attr
from google.cloud import bigquery

from recidiviz.common import attr_validators


@attr.s(frozen=True, kw_only=True)
class BigQueryAddress:
    """Represents the (dataset_id, table_id) address of a BigQuery view or table."""

    dataset_id: str = attr.ib(validator=attr_validators.is_str)
    table_id: str = attr.ib(validator=attr_validators.is_str)

    @classmethod
    def from_list_item(cls, table: bigquery.table.TableListItem) -> "BigQueryAddress":
        return cls(dataset_id=table.dataset_id, table_id=table.table_id)
