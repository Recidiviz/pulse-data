# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Helpers for writing validation outputs to BigQuery."""
import datetime
import logging

import pytz

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.big_query_row_streamer import BigQueryRowStreamer
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.source_tables.externally_managed.collect_externally_managed_source_table_configs import (
    build_source_table_repository_for_externally_managed_tables,
)
from recidiviz.source_tables.externally_managed.datasets import (
    VALIDATION_RESULTS_DATASET_ID,
)
from recidiviz.utils import environment, metadata
from recidiviz.validation.validation_result_for_storage import (
    ValidationResultForStorage,
)

VALIDATION_RESULTS_BIGQUERY_ADDRESS = BigQueryAddress(
    dataset_id=VALIDATION_RESULTS_DATASET_ID, table_id="validation_results"
)
VALIDATIONS_COMPLETION_TRACKER_BIGQUERY_ADDRESS = BigQueryAddress(
    dataset_id=VALIDATION_RESULTS_DATASET_ID, table_id="validations_completion_tracker"
)


def store_validation_results_in_big_query(
    validation_results: list[ValidationResultForStorage],
) -> None:
    if not environment.in_gcp():
        logging.info(
            "Skipping storing [%d] validation results in BigQuery.",
            len(validation_results),
        )
        return

    bq_client = BigQueryClientImpl()
    bq_client.stream_into_table(
        VALIDATION_RESULTS_BIGQUERY_ADDRESS,
        [result.to_serializable() for result in validation_results],
    )


def store_validation_run_completion_in_big_query(
    state_code: StateCode,
    validation_run_id: str,
    num_validations_run: int,
    validations_runtime_sec: int,
    sandbox_dataset_prefix: str | None,
) -> None:
    """Persists a row to BQ that indicates that a particular validation run has
    completed without crashing. This row will be used by our Airflow DAG to determine
    whether we can continue.
    """

    if not environment.in_gcp():
        logging.info("Skipping storing validation run completion in BigQuery for task.")
        return

    source_table_repository = (
        build_source_table_repository_for_externally_managed_tables(
            metadata.project_id()
        )
    )

    bq_client = BigQueryClientImpl()
    source_table_config = source_table_repository.get_config(
        VALIDATIONS_COMPLETION_TRACKER_BIGQUERY_ADDRESS
    )
    success_row_streamer = BigQueryRowStreamer(
        bq_client=bq_client,
        table_address=source_table_config.address,
        expected_table_schema=source_table_config.schema_fields,
    )

    success_row_streamer.stream_rows(
        [
            {
                "region_code": state_code.value,
                "run_id": validation_run_id,
                "success_timestamp": datetime.datetime.now(tz=pytz.UTC).isoformat(),
                "num_validations_run": num_validations_run,
                "validations_runtime_sec": validations_runtime_sec,
                "ingest_instance": DirectIngestInstance.PRIMARY.value,
                "sandbox_dataset_prefix": sandbox_dataset_prefix,
            }
        ]
    )
