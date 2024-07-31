#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Utilities to update raw data table schemas."""
import logging
from typing import List, Optional

from google.cloud import bigquery

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_schema_builder import (
    RawDataTableBigQuerySchemaBuilder,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import get_region_raw_file_config
from recidiviz.ingest.direct.types.direct_ingest_constants import FILE_ID_COL_NAME
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


def _update_raw_data_table_schema_to_new_schema(
    state_code: StateCode,
    instance: DirectIngestInstance,
    raw_file_tag: str,
    schema: List[bigquery.SchemaField],
    big_query_client: BigQueryClient,
    sandbox_dataset_prefix: Optional[str] = None,
) -> None:
    """Update the schema of a given raw data table to match the passed in `schema`."""
    raw_data_dataset_id = raw_tables_dataset_for_region(
        state_code=state_code,
        instance=instance,
        sandbox_dataset_prefix=sandbox_dataset_prefix,
    )

    try:
        if big_query_client.table_exists(raw_data_dataset_id, raw_file_tag):
            big_query_client.update_schema(
                raw_data_dataset_id, raw_file_tag, schema, allow_field_deletions=False
            )
        else:
            big_query_client.create_table_with_schema(
                raw_data_dataset_id,
                raw_file_tag,
                schema,
                clustering_fields=[FILE_ID_COL_NAME],
            )
    except Exception as e:
        logging.exception(
            "Failed to update schema for `%s.%s`", raw_data_dataset_id, raw_file_tag
        )
        raise ValueError(
            f"Failed to update schema for `{raw_data_dataset_id}.{raw_file_tag}`."
        ) from e


def _get_raw_data_table_schema(
    state_code: StateCode, raw_file_tag: str
) -> List[bigquery.SchemaField]:
    """Retrieve the raw data BQ schema for a given raw file tag."""
    region_config = get_region_raw_file_config(state_code.value.lower())
    return RawDataTableBigQuerySchemaBuilder.build_bq_schmea_for_config(
        raw_file_config=region_config.raw_file_configs[raw_file_tag]
    )


def update_raw_data_table_schema(
    state_code: StateCode,
    instance: DirectIngestInstance,
    raw_file_tag: str,
    big_query_client: BigQueryClient,
    sandbox_dataset_prefix: Optional[str] = None,
) -> None:
    """Update the raw data table for a given state, instance, and raw file tag by obtaining the appropriate raw file
    config updating the schema based on the columns defined in the YAML."""
    _update_raw_data_table_schema_to_new_schema(
        state_code=state_code,
        instance=instance,
        raw_file_tag=raw_file_tag,
        schema=_get_raw_data_table_schema(state_code, raw_file_tag),
        sandbox_dataset_prefix=sandbox_dataset_prefix,
        big_query_client=big_query_client,
    )
