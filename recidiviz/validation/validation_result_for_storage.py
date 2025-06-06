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
from ast import literal_eval
from typing import Any, Dict, Optional, cast

import attr
import cattr

from recidiviz.common import serialization
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.monitoring.context import get_current_trace_id
from recidiviz.utils import environment
from recidiviz.validation.checks.existence_check import ExistenceValidationResultDetails
from recidiviz.validation.checks.sameness_check import (
    SamenessPerRowValidationResultDetails,
    SamenessPerViewValidationResultDetails,
)
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

    runtime_seconds: Optional[float] = attr.ib()
    exception_log: Optional[Exception] = attr.ib()

    sandbox_dataset_prefix: Optional[str] = attr.ib()
    ingest_instance: Optional[DirectIngestInstance] = attr.ib()

    @trace_id.default
    def _trace_id_factory(self) -> str:
        return get_current_trace_id()

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
        runtime_seconds: float,
    ) -> "ValidationResultForStorage":
        return cls(
            run_id=run_id,
            run_date=run_datetime.date(),
            run_datetime=run_datetime,
            system_version=environment.get_data_platform_version(),
            check_type=result.validation_job.validation.validation_type,
            validation_name=result.validation_job.validation.validation_name,
            region_code=result.validation_job.region_code,
            did_run=True,
            validation_result_status=result.validation_result_status,
            failure_description=result.result_details.failure_description(),
            result_details_type=result.result_details.__class__.__name__,
            result_details=result.result_details,
            validation_category=result.validation_job.validation.validation_category,
            sandbox_dataset_prefix=(
                (result.validation_job.sandbox_context.output_sandbox_dataset_prefix)
                if result.validation_job.sandbox_context
                else None
            ),
            ingest_instance=DirectIngestInstance.PRIMARY,
            runtime_seconds=runtime_seconds,
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
            system_version=environment.get_data_platform_version(),
            check_type=job.validation.validation_type,
            validation_name=job.validation.validation_name,
            region_code=job.region_code,
            did_run=False,
            validation_result_status=None,
            failure_description=None,
            result_details_type=None,
            result_details=None,
            validation_category=job.validation.validation_category,
            sandbox_dataset_prefix=(
                job.sandbox_context.output_sandbox_dataset_prefix
                if job.sandbox_context
                else None
            ),
            ingest_instance=DirectIngestInstance.PRIMARY,
            runtime_seconds=None,
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
            unstructured["result_details"] = json.dumps(
                details, default=lambda o: o.__dict__
            )

        # BigQuery doesn't store timezone information so we have to strip it off,
        # ensuring that we are passing it UTC.
        run_datetime = cast(str, unstructured["run_datetime"])
        if run_datetime.endswith("+00:00"):
            unstructured["run_datetime"] = run_datetime[: -len("+00:00")]
        else:
            raise ValueError(f"Datetime {run_datetime=} is not UTC.")

        # Exceptions are not serializable.
        if exception_log := unstructured["exception_log"]:
            unstructured["exception_log"] = repr(exception_log)

        return unstructured

    @staticmethod
    def _load_from_json_str(json_str: str) -> DataValidationJobResultDetails:
        """Loads the result_details field from a JSON string."""
        attrs = json.loads(json_str)
        for cls in [
            ExistenceValidationResultDetails,
            SamenessPerRowValidationResultDetails,
            SamenessPerViewValidationResultDetails,
        ]:
            if set(attrs.keys()) == {
                attr.name for attr in list(cls.__dict__["__attrs_attrs__"])
            }:
                return cls(**attrs)
        raise ValueError(
            f"Could not deserialize DataValidationJobResultDetails from JSON string: {json_str}"
        )

    @classmethod
    def from_serializable(
        cls, serialized: Dict[str, Any]
    ) -> "ValidationResultForStorage":
        """Loads a ValidationResultForStorage from a Pandas Series."""
        converter = cattr.Converter()
        converter.register_structure_hook(datetime.date, lambda d, _: d)
        converter.register_structure_hook(datetime.datetime, lambda d, _: d)
        converter.register_structure_hook(
            Exception, lambda d, _: literal_eval(d) if d is not None else None
        )
        converter.register_structure_hook(
            DataValidationJobResultDetails, lambda d, _: cls._load_from_json_str(d)
        )
        structured = converter.structure(serialized, ValidationResultForStorage)
        return structured
