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
"""Class that can be used for streaming rows into a given table."""
from typing import Any, Dict, List, Sequence

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient


class BigQueryRowStreamer:
    """Class that can be used for streaming rows into a given table."""

    def __init__(
        self,
        bq_client: BigQueryClient,
        table_address: BigQueryAddress,
        table_schema: List[bigquery.SchemaField],
    ) -> None:
        self.bq_client = bq_client
        self.table_address = table_address
        self.table_schema = table_schema

    def stream_rows(self, rows: Sequence[Dict[str, Any]]) -> None:
        """Streams the provided rows into the streamer's table, creating the table first
        if it does not exist.
        """
        self._create_or_update_table_if_necessary()
        self.bq_client.stream_into_table(self.table_address, rows)

    def _create_or_update_table_if_necessary(self) -> None:
        self.bq_client.create_dataset_if_necessary(self.table_address.dataset_id)
        if not self.bq_client.table_exists(self.table_address):
            self.bq_client.create_table_with_schema(
                self.table_address, schema_fields=self.table_schema
            )
        else:
            self.bq_client.update_schema(
                self.table_address,
                desired_schema_fields=self.table_schema,
                allow_field_deletions=False,
            )
