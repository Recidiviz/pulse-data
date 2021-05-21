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
from recidiviz.common import serialization
from recidiviz.validation.validation_models import (
    DataValidationJob,
    DataValidationJobResult,
    DataValidationJobResultDetails,
    ValidationCheckType,
)
from recidiviz.utils import environment


@attr.s(frozen=True, kw_only=True)
class ValidationResultForStorage:
    """The results for a single validation job run to be persisted in BigQuery"""

    run_id: str = attr.ib()
    run_date: datetime.date = attr.ib()

    system_version: str = attr.ib()

    check_type: ValidationCheckType = attr.ib()
    validation_name: str = attr.ib()
    region_code: str = attr.ib()

    did_run: bool = attr.ib()

    was_successful: Optional[bool] = attr.ib()
    failure_description: Optional[str] = attr.ib()
    result_details_type: Optional[str] = attr.ib()
    result_details: Optional[DataValidationJobResultDetails] = attr.ib()

    @classmethod
    def from_validation_result(
        cls, run_id: str, run_date: datetime.date, result: DataValidationJobResult
    ) -> "ValidationResultForStorage":
        return cls(
            run_id=run_id,
            run_date=run_date,
            system_version=environment.get_version(),
            check_type=result.validation_job.validation.validation_type,
            validation_name=result.validation_job.validation.validation_name,
            region_code=result.validation_job.region_code,
            did_run=True,
            was_successful=result.was_successful,
            failure_description=result.result_details.failure_description(),
            result_details_type=result.result_details.__class__.__name__,
            result_details=result.result_details,
        )

    @classmethod
    def from_validation_job(
        cls, run_id: str, run_date: datetime.date, job: DataValidationJob
    ) -> "ValidationResultForStorage":
        return cls(
            run_id=run_id,
            run_date=run_date,
            system_version=environment.get_version(),
            check_type=job.validation.validation_type,
            validation_name=job.validation.validation_name,
            region_code=job.region_code,
            did_run=False,
            was_successful=None,
            failure_description=None,
            result_details_type=None,
            result_details=None,
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


def store_validation_results(
    validation_results: List[ValidationResultForStorage],
) -> None:
    if not environment.in_gcp():
        logging.info(
            "Skipping storing [%d] validation results in BigQuery.",
            len(validation_results),
        )
        return

    bq_client = BigQueryClientImpl()
    bq_client.insert_into_table(
        bq_client.dataset_ref_for_id("validation_results"),
        "validation_results",
        [result.to_serializable() for result in validation_results],
    )
