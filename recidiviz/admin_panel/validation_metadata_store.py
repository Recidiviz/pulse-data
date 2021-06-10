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
from collections import defaultdict
from typing import Any, DefaultDict, Dict, Optional, TypeVar, cast

import attr
import cattr
from google.cloud.bigquery.table import Row
from werkzeug.exceptions import ServiceUnavailable

from recidiviz.admin_panel.admin_panel_store import AdminPanelStore
from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.big_query.big_query_view import BigQueryAddress
from recidiviz.common import serialization
from recidiviz.validation.validation_result_storage import (
    VALIDATION_RESULTS_BIGQUERY_ADDRESS,
)

# TODO(#7816): These are camel case because they are used in JS in the admin panel, we
# should probably move to protos.


@attr.s
class ValidationStatusRecord:
    didRun: bool = attr.ib()
    wasSuccessful: Optional[bool] = attr.ib()

    hasData: Optional[bool] = attr.ib()
    errorAmount: Optional[str] = attr.ib()


@attr.s
class ValidationStatusResults:
    runId: str = attr.ib()
    runDate: datetime.date = attr.ib()
    systemVersion: str = attr.ib()

    results: Dict[str, Dict[str, ValidationStatusRecord]] = attr.ib()

    def to_serializable(self) -> Dict[str, Any]:
        converter = serialization.with_datetime_hooks(cattr.Converter())
        return converter.unstructure(self)


def result_query(project_id: str, validation_result_address: BigQueryAddress) -> str:
    return f"""
SELECT
    run_id,
    run_date,
    system_version,
    validation_name,
    region_code,
    did_run,
    was_successful,
    result_details_type,
    -- TODO(#7810): Remove this logic and pull straight from BQ.
    case result_details_type
        WHEN "SamenessStringsValidationResultDetails" THEN if(cast(JSON_QUERY(result_details, "$.total_num_rows") as int64) > 0, true, false)
    END as has_data,
    CASE result_details_type
        WHEN "ExistenceValidationResultDetails" THEN cast(JSON_QUERY(result_details, "$.num_invalid_rows") as int64)
        WHEN "SamenessNumbersValidationResultDetails" THEN ifnull(ARRAY_LENGTH(JSON_QUERY_ARRAY(result_details, "$.failed_rows")), 0)
        WHEN "SamenessStringsValidationResultDetails" THEN safe_divide(cast(JSON_QUERY(result_details, "$.num_error_rows") as int64), cast(JSON_QUERY(result_details, "$.total_num_rows") as int64))
    END as error_amount,
FROM `{project_id}.{validation_result_address.dataset_id}.{validation_result_address.table_id}`
WHERE run_id = (
    SELECT run_id
    FROM `{project_id}.{validation_result_address.dataset_id}.{validation_result_address.table_id}`
    ORDER BY run_date desc LIMIT 1
)
"""


T = TypeVar("T")


def _set_if_new(old_value: Optional[T], new_value: T) -> T:
    if old_value is not None and old_value != new_value:
        raise ValueError(
            f"Expected single value from query but got '{old_value}' and '{new_value}'."
        )
    return new_value


def _format_error_amount(
    error_amount: Optional[float], result_details_type: str
) -> Optional[str]:
    if error_amount is None:
        return None
    if result_details_type == "SamenessStringsValidationResultDetails":
        return f"{(error_amount * 100):.1f}%"
    return f"{int(error_amount)}"


class ValidationStatusStore(AdminPanelStore):
    """Stores the status of validations to serve to the admin panel"""

    def __init__(self, override_project_id: Optional[str] = None) -> None:
        super().__init__(override_project_id)
        self.bq_client: BigQueryClient = BigQueryClientImpl(project_id=self.project_id)

        self.results: Optional[ValidationStatusResults] = None

    def recalculate_store(self) -> None:
        query_job = self.bq_client.run_query_async(
            result_query(self.project_id, VALIDATION_RESULTS_BIGQUERY_ADDRESS), []
        )

        # Build up new results
        results: DefaultDict[str, Dict[str, ValidationStatusRecord]] = defaultdict(dict)

        run_id: Optional[str] = None
        run_date: Optional[datetime.date] = None
        system_version: Optional[str] = None

        row: Row
        for row in query_job:
            run_id = _set_if_new(run_id, row.get("run_id"))
            run_date = _set_if_new(run_date, row.get("run_date"))
            system_version = _set_if_new(system_version, row.get("system_version"))

            results[row.get("validation_name")][
                row.get("region_code")
            ] = ValidationStatusRecord(
                didRun=row.get("did_run"),
                wasSuccessful=row.get("was_successful"),
                hasData=row.get("has_data"),
                errorAmount=_format_error_amount(
                    row.get("error_amount"), row.get("result_details_type")
                ),
            )

        if run_id is None:
            # No validation results exist.
            return

        # Swap results
        self.results = ValidationStatusResults(
            runId=cast(str, run_id),
            runDate=cast(datetime.date, run_date),
            systemVersion=cast(str, system_version),
            results=dict(results),
        )

    def get_most_recent_validation_results(self) -> ValidationStatusResults:
        if self.results is None:
            raise ServiceUnavailable("Validation results not yet loaded")
        return self.results
