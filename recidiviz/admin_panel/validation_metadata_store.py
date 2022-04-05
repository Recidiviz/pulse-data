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

import json
from enum import Enum
from typing import List, Optional, Type, TypeVar, Union, cast

import attr
import cattr
from google.cloud.bigquery.table import Row
from google.protobuf.timestamp_pb2 import Timestamp  # pylint: disable=no-name-in-module
from werkzeug.exceptions import ServiceUnavailable

from recidiviz.admin_panel.admin_panel_store import AdminPanelStore
from recidiviz.admin_panel.models.validation_pb2 import (
    ExistenceValidationResultDetails as ExistenceValidationResultDetails_pb2,
)
from recidiviz.admin_panel.models.validation_pb2 import ResultRow as ResultRow_pb2
from recidiviz.admin_panel.models.validation_pb2 import (
    SamenessPerRowValidationResultDetails as SamenessPerRowValidationResultDetails_pb2,
)
from recidiviz.admin_panel.models.validation_pb2 import (
    SamenessPerViewValidationResultDetails as SamenessPerViewValidationResultDetails_pb2,
)
from recidiviz.admin_panel.models.validation_pb2 import (
    ValidationStatusRecord as ValidationStatusRecord_pb2,
)
from recidiviz.admin_panel.models.validation_pb2 import (
    ValidationStatusRecords as ValidationStatusRecords_pb2,
)
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.common import serialization
from recidiviz.common.constants.states import StateCode
from recidiviz.utils import metadata
from recidiviz.validation.checks.existence_check import ExistenceValidationResultDetails
from recidiviz.validation.checks.sameness_check import (
    SamenessPerRowValidationResultDetails,
    SamenessPerViewValidationResultDetails,
)
from recidiviz.validation.configured_validations import get_all_validations
from recidiviz.validation.validation_models import DataValidationJobResultDetails
from recidiviz.validation.validation_result_storage import (
    VALIDATION_RESULTS_BIGQUERY_ADDRESS,
)

# TODO(#8687): Remove the extra case statements with the old result detail class names.
from recidiviz.validation.views.dataset_config import VIEWS_DATASET


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


def results_query(project_id: str, validation_result_address: BigQueryAddress) -> str:
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
    result_details,
    last_better_status_run_id,
    last_better_status_run_datetime,
    last_better_status_run_result_status,
FROM (
    SELECT
    result.*,
    last_better_status_run.run_id as last_better_status_run_id,
    last_better_status_run.run_datetime as last_better_status_run_datetime,
    last_better_status_run.validation_result_status as last_better_status_run_result_status,
    ROW_NUMBER() OVER (
        PARTITION BY result.run_id, result.validation_name, result.region_code
        -- Orders by recency of the compared data
        ORDER BY last_better_status_run.run_datetime DESC) as ordinal
    FROM `{project_id}.{validation_result_address.dataset_id}.{validation_result_address.table_id}` result
    -- Explodes to all prior better runs
    LEFT JOIN `{project_id}.{validation_result_address.dataset_id}.{validation_result_address.table_id}` last_better_status_run
    ON (result.validation_name = last_better_status_run.validation_name
        AND result.region_code = last_better_status_run.region_code
        AND result.run_datetime >= last_better_status_run.run_datetime
        AND CASE result.validation_result_status 
                WHEN "FAIL_HARD" THEN last_better_status_run.validation_result_status IN ("FAIL_SOFT", "SUCCESS")
                ELSE last_better_status_run.validation_result_status = "SUCCESS"
            END
    )
)
-- Get the row with the most recent better run
WHERE ordinal = 1
"""


def recent_run_results_query(
    project_id: str, validation_result_address: BigQueryAddress
) -> str:
    return f"""
{results_query(project_id, validation_result_address)}
AND run_id = (
    SELECT run_id
    FROM `{project_id}.{validation_result_address.dataset_id}.{validation_result_address.table_id}`
    ORDER BY run_datetime desc LIMIT 1
)
"""


def validation_history_results_query(
    project_id: str,
    validation_result_address: BigQueryAddress,
    validation_name: str,
    region_code: str,
    days_to_include: int = 14,
) -> str:
    return f"""
{results_query(project_id, validation_result_address)}
AND validation_name = "{validation_name}"
AND region_code = "{region_code}"
AND run_datetime >= DATETIME_SUB(CURRENT_DATE('US/Eastern'), INTERVAL {days_to_include} DAY)
ORDER BY run_datetime desc 
"""


def validation_error_table_query(
    project_id: str,
    validation_address: BigQueryAddress,
    region_code: str,
    limit: int,
) -> str:
    return f"""
SELECT * FROM {project_id}.{validation_address.dataset_id}.{validation_address.table_id}
WHERE region_code = "{region_code}"
LIMIT {limit}
"""


def validation_error_table_count_query(
    project_id: str,
    validation_address: BigQueryAddress,
    region_code: str,
) -> str:
    return f"""
    SELECT COUNT(*) as count FROM {project_id}.{validation_address.dataset_id}.{validation_address.table_id}
    WHERE region_code = "{region_code}"
    """


T = TypeVar("T")


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


def _convert_result_details(
    result_details: Optional[DataValidationJobResultDetails],
) -> Optional[
    Union[
        ExistenceValidationResultDetails_pb2,
        SamenessPerRowValidationResultDetails_pb2,
        SamenessPerViewValidationResultDetails_pb2,
    ]
]:
    """Converts the result details python object into the appropriate protobuf"""
    if result_details is None:
        return None

    result_details_type = ResultDetailsTypes(result_details.__class__.__name__)
    if result_details_type is ResultDetailsTypes.EXISTENCE_VALIDATION_RESULT_DETAILS:
        # The existence details doesn't contain any extra information besides what is
        # already available in the details interface.
        return ExistenceValidationResultDetails_pb2()
    if (
        result_details_type
        is ResultDetailsTypes.SAMENESS_PER_ROW_VALIDATION_RESULT_DETAILS
    ):
        result_details = cast(SamenessPerRowValidationResultDetails, result_details)
        return SamenessPerRowValidationResultDetails_pb2(
            failed_rows=[
                SamenessPerRowValidationResultDetails_pb2.RowWithError(
                    row=ResultRow_pb2(
                        label_values=row[0].label_values,
                        comparison_values=row[0].comparison_values,
                    ),
                    error=row[1],
                )
                for row in result_details.failed_rows
            ]
        )
    if (
        result_details_type
        is ResultDetailsTypes.SAMENESS_PER_VIEW_VALIDATION_RESULT_DETAILS
    ):
        result_details = cast(SamenessPerViewValidationResultDetails, result_details)
        return SamenessPerViewValidationResultDetails_pb2(
            num_error_rows=result_details.num_error_rows,
            total_num_rows=result_details.total_num_rows,
            non_null_counts_per_column_per_partition=[
                SamenessPerViewValidationResultDetails_pb2.PartitionCounts(
                    partition_labels=entry[0], column_counts=entry[1]
                )
                for entry in result_details.non_null_counts_per_column_per_partition
            ],
        )

    raise ValueError(f"Unexpected result details type '{result_details_type}'")


@attr.s
class ResultDetailsOneof:

    details: Optional[
        Union[
            ExistenceValidationResultDetails_pb2,
            SamenessPerRowValidationResultDetails_pb2,
            SamenessPerViewValidationResultDetails_pb2,
        ]
    ] = attr.ib(converter=_convert_result_details)

    @property
    def existence(self) -> Optional[ExistenceValidationResultDetails_pb2]:
        if isinstance(self.details, ExistenceValidationResultDetails_pb2):
            return self.details
        return None

    @property
    def sameness_per_row(self) -> Optional[SamenessPerRowValidationResultDetails_pb2]:
        if isinstance(self.details, SamenessPerRowValidationResultDetails_pb2):
            return self.details
        return None

    @property
    def sameness_per_view(self) -> Optional[SamenessPerViewValidationResultDetails_pb2]:
        if isinstance(self.details, SamenessPerViewValidationResultDetails_pb2):
            return self.details
        return None


def _validation_status_record_from_row(row: Row) -> ValidationStatusRecord_pb2:
    """Takes a BigQuery row from the query template and converts it to a protobuf record"""
    result_details = _result_details_from_row(row)
    result_details_oneof = ResultDetailsOneof(result_details)

    result_status: Optional[ValidationStatusRecord_pb2.ValidationResultStatus.V] = None
    hard_failure_amount: Optional[float] = None
    soft_failure_amount: Optional[float] = None
    error_amount: Optional[float] = None
    has_data: Optional[bool] = None
    dev_mode: Optional[bool] = None

    if result_details:
        result_status = ValidationStatusRecord_pb2.ValidationResultStatus.Value(
            result_details.validation_result_status().value
        )
        hard_failure_amount = result_details.hard_failure_amount
        soft_failure_amount = result_details.soft_failure_amount
        error_amount = result_details.error_amount
        has_data = result_details.has_data
        dev_mode = result_details.is_dev_mode

    # legacy support for rows before result_status was introduced
    if result_status is None:
        was_successful = row.get("was_successful")
        if was_successful is not None:
            result_status = (
                ValidationStatusRecord_pb2.ValidationResultStatus.SUCCESS
                if was_successful
                else ValidationStatusRecord_pb2.ValidationResultStatus.FAIL_HARD
            )

    run_datetime = Timestamp()
    run_datetime.FromDatetime(row.get("run_datetime"))

    last_better_status_run_id = None
    last_better_status_run_datetime = None
    last_better_status_run_result_status = None
    if row.get("last_better_status_run_id") is not None:
        last_better_status_run_id = row.get("last_better_status_run_id")
        last_better_status_run_datetime = Timestamp()
        last_better_status_run_datetime.FromDatetime(
            row.get("last_better_status_run_datetime")
        )
        last_better_status_run_result_status = (
            ValidationStatusRecord_pb2.ValidationResultStatus.Value(
                row.get("last_better_status_run_result_status")
            )
        )

    return ValidationStatusRecord_pb2(
        run_id=row.get("run_id"),
        run_datetime=run_datetime,
        system_version=row.get("system_version"),
        name=row.get("validation_name"),
        category=ValidationStatusRecord_pb2.ValidationCategory.Value(
            row.get("validation_category")
        ),
        is_percentage=result_details.error_is_percentage if result_details else None,
        state_code=row.get("region_code"),
        did_run=row.get("did_run"),
        has_data=has_data,
        dev_mode=dev_mode,
        hard_failure_amount=hard_failure_amount,
        soft_failure_amount=soft_failure_amount,
        result_status=result_status,
        error_amount=error_amount,
        failure_description=row.get("failure_description"),
        existence=result_details_oneof.existence,
        sameness_per_row=result_details_oneof.sameness_per_row,
        sameness_per_view=result_details_oneof.sameness_per_view,
        last_better_status_run_id=last_better_status_run_id,
        last_better_status_run_datetime=last_better_status_run_datetime,
        last_better_status_run_result_status=last_better_status_run_result_status,
    )


def _result_details_from_row(row: Row) -> Optional[DataValidationJobResultDetails]:
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
    return job_result_details


class ValidationStatusStore(AdminPanelStore):
    """Stores the status of validations to serve to the admin panel"""

    def __init__(self) -> None:
        self.bq_client: BigQueryClient = BigQueryClientImpl()

        self.records: Optional[ValidationStatusRecords_pb2] = None

    @property
    def state_codes(self) -> List[StateCode]:
        if self.records is None:
            raise ServiceUnavailable("Validation results not yet loaded")
        return [
            StateCode(state_code)
            for state_code in set(record.state_code for record in self.records.records)
        ]

    def recalculate_store(self) -> None:
        """Recalculates validation data by querying the validation data store"""
        query_job = self.bq_client.run_query_async(
            recent_run_results_query(
                metadata.project_id(), VALIDATION_RESULTS_BIGQUERY_ADDRESS
            ),
            [],
        )

        # Build up new results
        records: List[ValidationStatusRecord_pb2] = []
        run_id: Optional[str] = None

        row: Row
        for row in query_job:
            if run_id is not None and run_id != row.get("run_id"):
                raise ValueError(
                    f"Expected single run id but got '{run_id}' and '{row.get('run_id')}'."
                )
            run_id = row.get("run_id")
            records.append(_validation_status_record_from_row(row))

        if run_id is None:
            # No validation results exist.
            return

        # Swap results
        self.records = ValidationStatusRecords_pb2(records=records)

    def get_most_recent_validation_results(self) -> ValidationStatusRecords_pb2:
        if self.records is None:
            raise ServiceUnavailable("Validation results not yet loaded")
        return self.records

    def get_results_for_validation(
        self, validation_name: str, state_code: str
    ) -> ValidationStatusRecords_pb2:
        query_job = self.bq_client.run_query_async(
            validation_history_results_query(
                metadata.project_id(),
                VALIDATION_RESULTS_BIGQUERY_ADDRESS,
                validation_name,
                state_code,
            ),
            [],
        )

        # Build up new results
        records: List[ValidationStatusRecord_pb2] = []

        row: Row
        for row in query_job:
            records.append(_validation_status_record_from_row(row))

        return ValidationStatusRecords_pb2(records=records)

    def get_error_table_for_validation(
        self, validation_name: str, state_code: str
    ) -> Optional[str]:
        """Returns rows in the error view of a given validation"""
        validations = get_all_validations()
        limit = 500

        for validation in validations:
            if validation.validation_name == validation_name:
                query_str = validation_error_table_query(
                    metadata.project_id(),
                    BigQueryAddress(
                        dataset_id=VIEWS_DATASET,
                        table_id=validation.error_view_builder.view_id,
                    ),
                    state_code,
                    limit,
                )
                count_query_str = validation_error_table_count_query(
                    metadata.project_id(),
                    BigQueryAddress(
                        dataset_id=VIEWS_DATASET,
                        table_id=validation.error_view_builder.view_id,
                    ),
                    state_code,
                )

                query_job = self.bq_client.run_query_async(
                    query_str,
                    [],
                )
                count_query_job = self.bq_client.run_query_async(
                    count_query_str,
                    [],
                )

                records = [dict(row) for row in query_job]
                total_rows = list(count_query_job)[0]["count"]
                data = {
                    "metadata": {
                        "query": query_str,
                        "limitedRowsShown": total_rows > limit,
                        "totalRows": total_rows,
                    },
                    "rows": records[0 : min(limit, total_rows)],
                }

                return json.dumps(data, default=str)
        return None
