#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2025 Recidiviz, Inc.
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
"""Utilities for raw data pruning operations involving BigQuery."""
import logging
from typing import Optional, Tuple

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.cloud_resources.platform_resource_labels import (
    RawDataImportStepResourceLabel,
)


def get_pruned_table_row_counts(
    big_query_client: BigQueryClient,
    project_id: str,
    temp_raw_data_diff_table_address: BigQueryAddress,
) -> Tuple[Optional[int], Optional[int]]:
    """Returns the net new or updated rows and deleted rows in the pruned table
    at |temp_raw_data_diff_table_address|.

    Returns:
        A tuple of (net_new_or_updated_rows, deleted_rows).
    """
    query = f"""
        SELECT
            SUM(CASE WHEN is_deleted THEN 1 ELSE 0 END) AS deleted_rows,
            SUM(CASE WHEN NOT is_deleted THEN 1 ELSE 0 END) AS net_new_or_updated_rows
        FROM `{project_id}.{temp_raw_data_diff_table_address.dataset_id}.{temp_raw_data_diff_table_address.table_id}`
    """
    query_job = big_query_client.run_query_async(
        query_str=query,
        use_query_cache=False,
        job_labels=[RawDataImportStepResourceLabel.RAW_DATA_PRUNING.value],
    )
    try:
        results = query_job.result()
    except Exception as e:
        logging.error(
            "Query job [%s] to get pruned table row counts failed with errors: [%s]",
            query_job.job_id,
            query_job.errors,
        )
        raise e

    if results.total_rows != 1:
        raise ValueError(
            f"Expected one row from pruned table row counts query, got {results.total_rows}"
        )

    row = list(results)[0]
    return row["net_new_or_updated_rows"] or 0, row["deleted_rows"] or 0
