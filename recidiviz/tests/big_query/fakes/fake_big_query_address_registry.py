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
"""Class that maintains a mapping of BigQueryAddress to unique string Postgres table
names.
"""

import logging
from typing import Dict, Iterable

from recidiviz.big_query.big_query_view import BigQueryAddress

MAX_POSTGRES_TABLE_NAME_LENGTH = 63


class FakeBigQueryAddressRegistry:
    """Class that maintains a mapping of BigQueryAddress to unique string Postgres table
    names.
    """

    def __init__(self) -> None:
        # Stores the list of mock tables that have been created as (dataset_id,
        # table_id) tuples.
        self._mock_bq_to_postgres_tables: Dict[BigQueryAddress, str] = {}

    def get_postgres_table(self, big_query_address: BigQueryAddress) -> str:
        if big_query_address not in self._mock_bq_to_postgres_tables:
            raise KeyError(
                f"BigQuery location [{big_query_address}] not properly registered - "
                f"must be created via register_bq_address."
            )

        return self._mock_bq_to_postgres_tables[big_query_address]

    def all_postgres_tables(self) -> Iterable[str]:
        return self._mock_bq_to_postgres_tables.values()

    def register_bq_address(self, address: BigQueryAddress) -> str:
        """Registers a BigQueryAddress in the map of address -> Postgres tables. Returns
        the corresponding Postgres table name.
        """
        # Postgres does not support '.' in table names, so we instead join them with an
        # underscore.
        postgres_table_name = "_".join([address.dataset_id, address.table_id])
        if len(postgres_table_name) > MAX_POSTGRES_TABLE_NAME_LENGTH:
            new_postgres_table_name = postgres_table_name[
                :MAX_POSTGRES_TABLE_NAME_LENGTH
            ]

            for (
                other_address,
                other_postgres_table_name,
            ) in self._mock_bq_to_postgres_tables.items():
                if (
                    other_postgres_table_name == new_postgres_table_name
                    and address != other_address
                ):
                    raise ValueError(
                        f"Truncated postgres table name [{new_postgres_table_name}] "
                        f"for address [{address}] collides with name for "
                        f"[{other_address}]."
                    )

            logging.warning(
                "Table name [%s] too long, truncating to [%s]",
                postgres_table_name,
                new_postgres_table_name,
            )
            postgres_table_name = new_postgres_table_name

        if address in self._mock_bq_to_postgres_tables:
            if self._mock_bq_to_postgres_tables[address] != postgres_table_name:
                raise ValueError(
                    f"Address [{address}] already has a different postgres table "
                    f"associated with it: {self._mock_bq_to_postgres_tables[address]}"
                )

        self._mock_bq_to_postgres_tables[address] = postgres_table_name
        return postgres_table_name
