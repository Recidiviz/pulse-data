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
from typing import Any, Dict, List, Optional, Type, cast

import attr
import cattr
from google.cloud import bigquery

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.big_query_utils import schema_field_for_type
from recidiviz.common import serialization
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.utils import environment

CLOUD_SQL_TO_BQ_REFRESH_STATUS_ADDRESS = BigQueryAddress(
    dataset_id="cloud_sql_to_bq_refresh", table_id="refresh_status"
)


@attr.s(frozen=True, kw_only=True)
class CloudSqlToBqRefreshStatus:
    """Holds information about last refresh date for a given schema to be persisted in BigQuery"""

    refresh_run_id: str = attr.ib()
    schema: SchemaType = attr.ib()
    last_refresh_datetime: datetime.datetime = attr.ib()
    region_code: Optional[str] = attr.ib()

    @classmethod
    def table_fields(cls) -> Dict[str, Type]:
        return {
            "refresh_run_id": str,
            "schema": str,
            "last_refresh_datetime": datetime.datetime,
            "region_code": str,
        }

    @classmethod
    def bq_schema_for_table(cls) -> List[bigquery.SchemaField]:
        """Returns the necessary BigQuery schema for the table, which is a
        list of SchemaField objects containing the column name and value type for
        each field in |table_fields|."""
        return [
            schema_field_for_type(field_name=field_name, field_type=field_type)
            for field_name, field_type in cls.table_fields().items()
        ]

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
    if not dataset_override_prefix and not environment.in_gcp():
        # pylint: disable=import-outside-toplevel
        from unittest.mock import MagicMock

        if not isinstance(bq_client, MagicMock):
            raise ValueError(
                "Should not be calling store_bq_refresh_status_in_big_query with a "
                "real BigQueryClientImpl outside of GCP - did you fail to mock this"
                "properly?"
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
                "Expected sandbox address for {CLOUD_SQL_TO_BQ_REFRESH_STATUS_ADDRESS}"
            )
    else:
        address = CLOUD_SQL_TO_BQ_REFRESH_STATUS_ADDRESS

    update_bq_refresh_status_table(
        bq_client,
        address,
        CloudSqlToBqRefreshStatus.bq_schema_for_table(),
    )

    bq_client.stream_into_table(
        bq_client.dataset_ref_for_id(address.dataset_id),
        address.table_id,
        [result.to_serializable() for result in bq_refresh_statuses],
    )


def update_bq_refresh_status_table(
    bq_client: BigQueryClient,
    bq_refresh_status_address: BigQueryAddress,
    table_schema: List[bigquery.SchemaField],
) -> None:
    bq_refresh_status_dataset_ref = bq_client.dataset_ref_for_id(
        bq_refresh_status_address.dataset_id
    )

    bq_client.create_dataset_if_necessary(bq_refresh_status_dataset_ref)

    if bq_client.table_exists(
        bq_refresh_status_dataset_ref, bq_refresh_status_address.table_id
    ):
        # Compare schema derived from schema table to existing dataset and
        # update if necessary.
        bq_client.update_schema(
            bq_refresh_status_address.dataset_id,
            bq_refresh_status_address.table_id,
            table_schema,
        )
    else:
        bq_client.create_table_with_schema(
            bq_refresh_status_address.dataset_id,
            bq_refresh_status_address.table_id,
            table_schema,
        )
