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
        self._create_table_if_necessary()
        self.bq_client.stream_into_table(
            self.bq_client.dataset_ref_for_id(self.table_address.dataset_id),
            self.table_address.table_id,
            rows,
        )

    def _create_table_if_necessary(self) -> None:
        dataset_ref = self.bq_client.dataset_ref_for_id(self.table_address.dataset_id)
        self.bq_client.create_dataset_if_necessary(dataset_ref)
        if not self.bq_client.table_exists(dataset_ref, self.table_address.table_id):
            self.bq_client.create_table_with_schema(
                dataset_ref.dataset_id,
                self.table_address.table_id,
                schema_fields=self.table_schema,
            )
