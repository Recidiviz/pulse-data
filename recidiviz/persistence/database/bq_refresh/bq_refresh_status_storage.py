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
"""Handles storing BQ refresh results in BigQuery"""
import datetime
from typing import Any, Dict, List, Optional, cast

import attr
import cattr

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.big_query_row_streamer import BigQueryRowStreamer
from recidiviz.big_query.constants import TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
from recidiviz.common import serialization
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.source_tables.yaml_managed.collect_yaml_managed_source_table_configs import (
    build_source_table_repository_for_yaml_managed_tables,
)
from recidiviz.source_tables.yaml_managed.datasets import (
    CLOUD_SQL_TO_BQ_REFRESH_DATASET_ID,
)
from recidiviz.utils import environment, metadata

CLOUD_SQL_TO_BQ_REFRESH_STATUS_ADDRESS = BigQueryAddress(
    dataset_id=CLOUD_SQL_TO_BQ_REFRESH_DATASET_ID, table_id="refresh_status"
)


@attr.s(frozen=True, kw_only=True)
class CloudSqlToBqRefreshStatus:
    """Holds information about last refresh date for a given schema to be persisted in BigQuery"""

    refresh_run_id: str = attr.ib()
    schema: SchemaType = attr.ib()
    last_refresh_datetime: datetime.datetime = attr.ib()
    region_code: Optional[str] = attr.ib()

    def to_serializable(self) -> Dict[str, Any]:
        converter = serialization.with_datetime_hooks(cattr.Converter())
        unstructured: Dict[str, Any] = converter.unstructure(self)

        # BigQuery doesn't store timezone information so we have to strip it off,
        # ensuring that we are passing it UTC.
        last_refresh_datetime = cast(str, unstructured["last_refresh_datetime"])
        if last_refresh_datetime.endswith("+00:00"):
            unstructured["last_refresh_datetime"] = last_refresh_datetime[
                : -len("+00:00")
            ]
        else:
            raise ValueError(f"Datetime {last_refresh_datetime=} is not UTC.")

        return unstructured


def store_bq_refresh_status_in_big_query(
    bq_client: BigQueryClient,
    bq_refresh_statuses: List[CloudSqlToBqRefreshStatus],
    dataset_override_prefix: Optional[str],
) -> None:
    """Stores BQ refresh status in a BQ table"""
    if not dataset_override_prefix and environment.in_test():
        # pylint: disable=import-outside-toplevel
        from unittest.mock import MagicMock

        if not isinstance(bq_client, MagicMock):
            raise ValueError(
                "Should not be calling store_bq_refresh_status_in_big_query with a "
                "real BigQueryClientImpl outside of GCP - did you fail to mock this"
                "properly?"
            )

    source_table_repository = build_source_table_repository_for_yaml_managed_tables(
        project_id=metadata.project_id()
    )
    source_table_config = source_table_repository.get_config(
        CLOUD_SQL_TO_BQ_REFRESH_STATUS_ADDRESS
    )

    if dataset_override_prefix:
        overrides = (
            BigQueryAddressOverrides.Builder(sandbox_prefix=dataset_override_prefix)
            .register_sandbox_override_for_address(
                CLOUD_SQL_TO_BQ_REFRESH_STATUS_ADDRESS
            )
            .build()
        )
        address = overrides.get_sandbox_address(CLOUD_SQL_TO_BQ_REFRESH_STATUS_ADDRESS)
        if address is None:
            raise ValueError(
                f"Expected sandbox address for "
                f"[{CLOUD_SQL_TO_BQ_REFRESH_STATUS_ADDRESS.to_str()}]"
            )
        # The standard source table update process only manages the unprefixed
        # table, so sandbox runs create their prefixed copy here.
        bq_client.create_dataset_if_necessary(
            address.dataset_id,
            default_table_expiration_ms=TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
        )
        if not bq_client.table_exists(address):
            bq_client.create_table_with_schema(
                address=address, schema_fields=source_table_config.schema_fields
            )
    else:
        address = CLOUD_SQL_TO_BQ_REFRESH_STATUS_ADDRESS

    row_streamer = BigQueryRowStreamer(
        bq_client=bq_client,
        table_address=address,
        expected_table_schema=source_table_config.schema_fields,
    )
    row_streamer.stream_rows(
        [result.to_serializable() for result in bq_refresh_statuses]
    )
