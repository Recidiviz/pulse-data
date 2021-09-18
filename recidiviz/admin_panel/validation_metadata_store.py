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
"""Stores recent validation results to serve to the admin panel"""

import datetime
import json
from enum import Enum
from typing import Any, Dict, Optional, Type, TypeVar, cast

import attr
import cattr
from google.cloud.bigquery.table import Row
from werkzeug.exceptions import ServiceUnavailable

from recidiviz.admin_panel.admin_panel_store import AdminPanelStore
from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.big_query.big_query_view import BigQueryAddress
from recidiviz.common import serialization
from recidiviz.validation.checks.existence_check import ExistenceValidationResultDetails
from recidiviz.validation.checks.sameness_check import (
    SamenessPerRowValidationResultDetails,
    SamenessPerViewValidationResultDetails,
)
from recidiviz.validation.validation_models import (
    DataValidationJobResultDetails,
    ValidationResultStatus,
)
from recidiviz.validation.validation_result_storage import (
    VALIDATION_RESULTS_BIGQUERY_ADDRESS,
)

# TODO(#7816): These are camel case because they are used in JS in the admin panel, we
# should probably move to protos.
# TODO(#8687): Remove the extra case statements with the old result detail class names.


class ResultDetailsTypes(Enum):
    EXISTENCE_VALIDATION_RESULT_DETAILS = "ExistenceValidationResultDetails"
    SAMENESS_PER_VIEW_VALIDATION_RESULT_DETAILS = (
        "SamenessPerViewValidationResultDetails"
    )
    SAMENESS_PER_ROW_VALIDATION_RESULT_DETAILS = "SamenessPerRowValidationResultDetails"

    @classmethod
    def get(cls, value: str) -> "ResultDetailsTypes":
        # This adds legacy support for depreciated result types
        if value == "SamenessStringsValidationResultDetails":
            return cls.SAMENESS_PER_VIEW_VALIDATION_RESULT_DETAILS
        if value == "SamenessNumbersValidationResultDetails":
            return cls.SAMENESS_PER_ROW_VALIDATION_RESULT_DETAILS
        return cls(value)

    def get_cls(self) -> Type[DataValidationJobResultDetails]:
        if self == ResultDetailsTypes.EXISTENCE_VALIDATION_RESULT_DETAILS:
            return ExistenceValidationResultDetails
        if self == ResultDetailsTypes.SAMENESS_PER_ROW_VALIDATION_RESULT_DETAILS:
            return SamenessPerRowValidationResultDetails
        if self == ResultDetailsTypes.SAMENESS_PER_VIEW_VALIDATION_RESULT_DETAILS:
            return SamenessPerViewValidationResultDetails
        raise ValueError(f"type {self} not mapped to class")


@attr.s
class ValidationStatusRecord:
    didRun: bool = attr.ib()
    hardFailureAmount: Optional[float] = attr.ib()
    softFailureAmount: Optional[float] = attr.ib()
    isPercentage: Optional[bool] = attr.ib()
    validationResultStatus: Optional[ValidationResultStatus] = attr.ib()
    failureDescription: Optional[str] = attr.ib()
    hasData: Optional[bool] = attr.ib()
    errorAmount: Optional[float] = attr.ib()
    resultDetailsType: Optional[ResultDetailsTypes] = attr.ib()


@attr.s
class ValidationStatusResult:
    validationCategory: str = attr.ib()
    # stateCode -> ValidationStatusRecord
    resultsByState: Dict[str, ValidationStatusRecord] = attr.ib()


@attr.s
class ValidationStatusResults:
    runId: str = attr.ib()
    runDatetime: datetime.datetime = attr.ib()
    systemVersion: str = attr.ib()
    # validationName -> ValidationStatusResult
    results: Dict[str, ValidationStatusResult] = attr.ib()

    def to_serializable(self) -> Dict[str, Any]:
        converter = serialization.with_datetime_hooks(cattr.Converter())
        return converter.unstructure(self)


def result_query(project_id: str, validation_result_address: BigQueryAddress) -> str:
    return f"""
SELECT
    run_id,
    run_datetime,
    system_version,
    validation_name,
    validation_category,
    region_code,
    did_run,
    validation_result_status,
    was_successful,
    failure_description,
    result_details_type,
    result_details
FROM `{project_id}.{validation_result_address.dataset_id}.{validation_result_address.table_id}`
WHERE run_id = (
    SELECT run_id
    FROM `{project_id}.{validation_result_address.dataset_id}.{validation_result_address.table_id}`
    ORDER BY run_datetime desc LIMIT 1
)
"""


T = TypeVar("T")


def _set_if_new(old_value: Optional[T], new_value: T) -> T:
    if old_value is not None and old_value != new_value:
        raise ValueError(
            f"Expected single value from query but got '{old_value}' and '{new_value}'."
        )
    return new_value


def _is_percentaqe_type(result_details_type: ResultDetailsTypes) -> bool:
    return result_details_type in [
        ResultDetailsTypes.SAMENESS_PER_VIEW_VALIDATION_RESULT_DETAILS,
        ResultDetailsTypes.SAMENESS_PER_ROW_VALIDATION_RESULT_DETAILS,
    ]


def _cast_result_details_json_to_object(
    result_details_type: ResultDetailsTypes, json_data: str
) -> DataValidationJobResultDetails:
    """
    Creates validation result details objects and contained variables using json data from the validation status
    store
    """
    unstructured = json.loads(json_data)
    converter = serialization.with_datetime_hooks(cattr.Converter())
    result_details = converter.structure(unstructured, result_details_type.get_cls())
    return result_details


class ValidationStatusStore(AdminPanelStore):
    """Stores the status of validations to serve to the admin panel"""

    def __init__(self, override_project_id: Optional[str] = None) -> None:
        super().__init__(override_project_id)
        self.bq_client: BigQueryClient = BigQueryClientImpl(project_id=self.project_id)

        self.results: Optional[ValidationStatusResults] = None

    def recalculate_store(self) -> None:
        """Recalculates validation data by querying the validation data store"""
        query_job = self.bq_client.run_query_async(
            result_query(self.project_id, VALIDATION_RESULTS_BIGQUERY_ADDRESS), []
        )

        # Build up new results
        results: Dict[str, ValidationStatusResult] = {}

        run_id: Optional[str] = None
        run_datetime: Optional[datetime.datetime] = None
        system_version: Optional[str] = None

        row: Row
        for row in query_job:
            run_id = _set_if_new(run_id, row.get("run_id"))
            run_datetime = _set_if_new(run_datetime, row.get("run_datetime"))
            system_version = _set_if_new(system_version, row.get("system_version"))
            result_details_type = (
                ResultDetailsTypes.get(row.get("result_details_type"))
                if row.get("result_details_type")
                else None
            )
            result_details = row.get("result_details")
            job_result_details = (
                _cast_result_details_json_to_object(result_details_type, result_details)
                if result_details and result_details_type
                else None
            )
            validation_result_status: Optional[ValidationResultStatus] = None
            hard_failure_amount: Optional[float] = None
            soft_failure_amount: Optional[float] = None
            error_amount: Optional[float] = None
            has_data: Optional[bool] = None

            if job_result_details:
                validation_result_status = job_result_details.validation_result_status()
                hard_failure_amount = job_result_details.hard_failure_amount
                soft_failure_amount = job_result_details.soft_failure_amount
                error_amount = job_result_details.error_amount
                has_data = job_result_details.has_data

            # legacy support for rows before validation_result_status was introduced
            if validation_result_status is None:
                was_successful = row.get("was_successful")
                if was_successful is not None:
                    validation_result_status = (
                        ValidationResultStatus.SUCCESS
                        if was_successful
                        else ValidationResultStatus.FAIL_HARD
                    )

            if row.get("validation_name") not in results:
                validation_status_result = ValidationStatusResult(
                    validationCategory=row.get("validation_category"), resultsByState={}
                )
                results[row.get("validation_name")] = validation_status_result

            results[row.get("validation_name")].resultsByState[
                row.get("region_code")
            ] = ValidationStatusRecord(
                didRun=row.get("did_run"),
                hardFailureAmount=hard_failure_amount,
                softFailureAmount=soft_failure_amount,
                isPercentage=_is_percentaqe_type(result_details_type)
                if result_details_type
                else None,
                validationResultStatus=validation_result_status,
                failureDescription=row.get("failure_description"),
                hasData=has_data,
                errorAmount=error_amount,
                resultDetailsType=result_details_type,
            )

        if run_id is None:
            # No validation results exist.
            return

        # Swap results
        self.results = ValidationStatusResults(
            runId=cast(str, run_id),
            runDatetime=cast(datetime.datetime, run_datetime),
            systemVersion=cast(str, system_version),
            results=dict(results),
        )

    def get_most_recent_validation_results(self) -> ValidationStatusResults:
        if self.results is None:
            raise ServiceUnavailable("Validation results not yet loaded")
        return self.results
