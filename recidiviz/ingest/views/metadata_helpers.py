# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""A set of helper functions for computing metadata on ingested tables."""

from typing import List, Tuple, Type, Optional, Callable

from sqlalchemy.ext.declarative import DeclarativeMeta

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema_utils import get_non_history_state_database_entities

METADATA_EXCLUDED_PROPERTIES = [
    'external_id',
    'state_code',
]

METADATA_TABLES_WITH_CUSTOM_COUNTERS = [
    'state_person',
]


def get_enum_property_names(entity: DeclarativeMeta) -> List[str]:
    return [col.name for col in entity.__table__.columns if hasattr(col.type, 'enums')]


def get_non_enum_property_names(entity: DeclarativeMeta) -> List[str]:
    return [col.name for col in entity.__table__.columns if not hasattr(col.type, 'enums')]


def get_state_tables() -> List[Tuple[Type[DatabaseEntity], str]]:
    return [(e, e.get_entity_name()) for e in get_non_history_state_database_entities()]


class BigQueryTableColumnChecker:
    """Class that fetches the BQ schema for a given Table/View and exposes functionality for checking if a column
    exists in the table."""

    def __init__(self, dataset_id: str, table_id: str) -> None:
        self.dataset_id = dataset_id
        self.table_id = table_id
        self._columns: Optional[List[str]] = None

    @property
    def columns(self) -> List[str]:
        if self._columns is None:
            bq_client = BigQueryClientImpl()
            t = bq_client.get_table(bq_client.dataset_ref_for_id(self.dataset_id), self.table_id)
            self._columns = [col.name for col in t.schema]

        return self._columns

    def _table_has_column(self, col_name: str) -> bool:
        return col_name in self.columns

    def get_has_column_predicate(self, col: str) -> Callable[[], bool]:
        """Returns a predicate that can be called to check that this table has a given column. The predicate function,
        when called, will lazily load table columns from BigQuery if they have not been loaded already."""
        def has_column() -> bool:
            return self._table_has_column(col)
        return has_column
