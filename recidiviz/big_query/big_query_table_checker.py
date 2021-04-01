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
"""Class that fetches the BQ schema for a given Table/View and exposes functionality
for checking if a column exists in the table."""

from typing import List, Optional, Callable

from recidiviz.big_query.big_query_client import BigQueryClientImpl


class BigQueryTableChecker:
    """Class that fetches the BQ schema for a given Table/View and exposes
    functionality for checking if a column exists in the table."""

    def __init__(self, dataset_id: str, table_id: str) -> None:
        self.dataset_id = dataset_id
        self.table_id = table_id
        self._columns: Optional[List[str]] = None

    @property
    def columns(self) -> List[str]:
        if self._columns is None:
            bq_client = BigQueryClientImpl()
            t = bq_client.get_table(
                bq_client.dataset_ref_for_id(self.dataset_id), self.table_id
            )
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

    def _table_exists(self) -> bool:
        bq_client = BigQueryClientImpl()
        return bq_client.table_exists(
            bq_client.dataset_ref_for_id(self.dataset_id), self.table_id
        )

    def get_table_exists_predicate(self) -> Callable[[], bool]:
        def table_exists() -> bool:
            return self._table_exists()

        return table_exists
