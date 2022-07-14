# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Handles storing validation results in BigQuery"""
import datetime
import json
import logging
from typing import Any, Dict, List, Optional, cast

import attr
import cattr
import pytz
from google.cloud import bigquery
from opencensus.trace import execution_context

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.big_query_row_streamer import BigQueryRowStreamer
from recidiviz.common import serialization
from recidiviz.utils import environment
from recidiviz.validation.validation_models import (
    DataValidationJob,
    DataValidationJobResult,
    DataValidationJobResultDetails,
    ValidationCategory,
    ValidationCheckType,
    ValidationResultStatus,
)


@attr.s(frozen=True, kw_only=True)
class ValidationResultForStorage:
    """The results for a single validation job run to be persisted in BigQuery"""

    run_id: str = attr.ib()
    run_date: datetime.date = attr.ib()
    run_datetime: datetime.datetime = attr.ib()

    system_version: str = attr.ib()

    check_type: ValidationCheckType = attr.ib()
    validation_name: str = attr.ib()
    region_code: str = attr.ib()

    did_run: bool = attr.ib()

    # The OpenCensus trace_id that can be used to track the request this job was performed in
    trace_id: str = attr.ib()

    validation_result_status: Optional[ValidationResultStatus] = attr.ib()
    failure_description: Optional[str] = attr.ib()
    result_details_type: Optional[str] = attr.ib()
    result_details: Optional[DataValidationJobResultDetails] = attr.ib()

    validation_category: Optional[ValidationCategory] = attr.ib()
    exception_log: Optional[Exception] = attr.ib()

    @trace_id.default
    def _trace_id_factory(self) -> str:
        return execution_context.get_opencensus_tracer().span_context.trace_id

    def __attrs_post_init__(self) -> None:
        if self.run_date != self.run_datetime.date():
            raise ValueError(
                f"run_date and run_datetime do not have matching dates: {self.run_date} vs. {self.run_datetime.date()}"
            )

    @classmethod
    def from_validation_result(
        cls,
        run_id: str,
        run_datetime: datetime.datetime,
        result: DataValidationJobResult,
    ) -> "ValidationResultForStorage":
        return cls(
            run_id=run_id,
            run_date=run_datetime.date(),
            run_datetime=run_datetime,
            system_version=environment.get_version(),
            check_type=result.validation_job.validation.validation_type,
            validation_name=result.validation_job.validation.validation_name,
            region_code=result.validation_job.region_code,
            did_run=True,
            validation_result_status=result.validation_result_status,
            failure_description=result.result_details.failure_description(),
            result_details_type=result.result_details.__class__.__name__,
            result_details=result.result_details,
            validation_category=result.validation_job.validation.validation_category,
            exception_log=None,
        )

    @classmethod
    def from_validation_job(
        cls,
        run_id: str,
        run_datetime: datetime.datetime,
        job: DataValidationJob,
        exception_log: Optional[Exception],
    ) -> "ValidationResultForStorage":
        return cls(
            run_id=run_id,
            run_date=run_datetime.date(),
            run_datetime=run_datetime,
            system_version=environment.get_version(),
            check_type=job.validation.validation_type,
            validation_name=job.validation.validation_name,
            region_code=job.region_code,
            did_run=False,
            validation_result_status=None,
            failure_description=None,
            result_details_type=None,
            result_details=None,
            validation_category=job.validation.validation_category,
            exception_log=exception_log,
        )

    def to_serializable(self) -> Dict[str, Any]:
        converter = serialization.with_datetime_hooks(cattr.Converter())
        unstructured: Dict[str, Any] = converter.unstructure(self)
        # The structure of result_details depends on the actual type of check. It is
        # stored as a JSON string so that all the fields can be stored and extracted in
        # the query.
        # TODO(#7544): Make result_details responsible for constraining its fields when
        # serializing so they don't scale linearly.
        if details := unstructured["result_details"]:
            unstructured["result_details"] = json.dumps(details)
        # BigQuery doesn't store timezone information so we have to strip it off,
        # ensuring that we are passing it UTC.
        run_datetime = cast(str, unstructured["run_datetime"])
        if run_datetime.endswith("+00:00"):
            unstructured["run_datetime"] = run_datetime[: -len("+00:00")]
        else:
            raise ValueError(f"Datetime {run_datetime=} is not UTC.")

        return unstructured


VALIDATION_RESULTS_DATASET_ID = "validation_results"

VALIDATION_RESULTS_BIGQUERY_ADDRESS = BigQueryAddress(
    dataset_id=VALIDATION_RESULTS_DATASET_ID, table_id="validation_results"
)


def store_validation_results_in_big_query(
    validation_results: List[ValidationResultForStorage],
) -> None:
    if not environment.in_gcp():
        logging.info(
            "Skipping storing [%d] validation results in BigQuery.",
            len(validation_results),
        )
        return

    bq_client = BigQueryClientImpl()
    bq_client.stream_into_table(
        bq_client.dataset_ref_for_id(VALIDATION_RESULTS_BIGQUERY_ADDRESS.dataset_id),
        VALIDATION_RESULTS_BIGQUERY_ADDRESS.table_id,
        [result.to_serializable() for result in validation_results],
    )


CLOUD_TASK_ID_COL = "cloud_task_id"
VALIDATION_RUN_ID_COL = "run_id"
SUCCESS_TIMESTAMP_COL = "success_timestamp"
NUM_VALIDATIONS_RUN_COL = "num_validations_run"
VALIDATIONS_RUNTIME_SEC_COL = "validations_runtime_sec"

VALIDATIONS_COMPLETION_TRACKER_BIGQUERY_ADDRESS = BigQueryAddress(
    dataset_id=VALIDATION_RESULTS_DATASET_ID, table_id="validations_completion_tracker"
)


def store_validation_run_completion_in_big_query(
    validation_run_id: str,
    num_validations_run: int,
    validations_runtime_sec: int,
    cloud_task_id: str,
) -> None:
    """Persists a row to BQ that indicates that a particular validation run has
    completed without crashing. This row will be used by our Airflow DAG to determine
    whether we can continue.
    """

    if not environment.in_gcp():
        logging.info(
            "Skipping storing validation run completion in BigQuery for task [%s].",
            cloud_task_id,
        )
        return

    bq_client = BigQueryClientImpl()
    success_row_streamer = BigQueryRowStreamer(
        bq_client=bq_client,
        table_address=VALIDATIONS_COMPLETION_TRACKER_BIGQUERY_ADDRESS,
        table_schema=[
            bigquery.SchemaField(
                name=CLOUD_TASK_ID_COL,
                field_type=bigquery.enums.SqlTypeNames.STRING.value,
                mode="REQUIRED",
            ),
            bigquery.SchemaField(
                name=VALIDATION_RUN_ID_COL,
                field_type=bigquery.enums.SqlTypeNames.STRING.value,
                mode="REQUIRED",
            ),
            bigquery.SchemaField(
                name=SUCCESS_TIMESTAMP_COL,
                field_type=bigquery.enums.SqlTypeNames.TIMESTAMP.value,
                mode="REQUIRED",
            ),
            bigquery.SchemaField(
                name=NUM_VALIDATIONS_RUN_COL,
                field_type=bigquery.enums.SqlTypeNames.INT64.value,
                mode="REQUIRED",
            ),
            bigquery.SchemaField(
                name=VALIDATIONS_RUNTIME_SEC_COL,
                field_type=bigquery.enums.SqlTypeNames.INT64.value,
                mode="REQUIRED",
            ),
        ],
    )

    success_row_streamer.stream_rows(
        [
            {
                CLOUD_TASK_ID_COL: cloud_task_id,
                VALIDATION_RUN_ID_COL: validation_run_id,
                SUCCESS_TIMESTAMP_COL: datetime.datetime.now(tz=pytz.UTC).isoformat(),
                NUM_VALIDATIONS_RUN_COL: num_validations_run,
                VALIDATIONS_RUNTIME_SEC_COL: validations_runtime_sec,
            }
        ]
    )
