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
from typing import Any, Dict, List, Optional

import attr
import cattr

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.big_query_view import BigQueryAddress
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
    validation_result_status: Optional[ValidationResultStatus] = attr.ib()
    failure_description: Optional[str] = attr.ib()
    result_details_type: Optional[str] = attr.ib()
    result_details: Optional[DataValidationJobResultDetails] = attr.ib()

    validation_category: Optional[ValidationCategory] = attr.ib()

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
        )

    @classmethod
    def from_validation_job(
        cls,
        run_id: str,
        run_datetime: datetime.datetime,
        job: DataValidationJob,
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
        return unstructured


VALIDATION_RESULTS_BIGQUERY_ADDRESS = BigQueryAddress(
    dataset_id="validation_results", table_id="validation_results"
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
