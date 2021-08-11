# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

"""Models a sameness check, which identifies a validation issue by observing that values in a configured set of
columns are not the same."""
import datetime
from enum import Enum
from typing import Dict, Generic, List, Optional, Set, Tuple, Type, TypeVar

import attr
from google.cloud.bigquery import QueryJob
from google.cloud.bigquery.table import Row

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.validation.checks.validation_checker import ValidationChecker
from recidiviz.validation.validation_config import ValidationRegionConfig
from recidiviz.validation.validation_models import (
    DataValidationCheck,
    DataValidationJob,
    DataValidationJobResult,
    DataValidationJobResultDetails,
    ValidationCheckType,
)

EMPTY_STRING_VALUE = "EMPTY_STRING_VALUE"
EMPTY_DATE_VALUE = datetime.date.min

PerViewRowType = TypeVar("PerViewRowType")


class SamenessDataValidationCheckType(Enum):
    # Used for comparing integer and/or float columns. The validation fails if the two
    # numbers differ by a percentage larger than the threshold for any single row.
    NUMBERS = "NUMBERS"

    # Used for comparing string columns. The validation fails if the percentage of rows
    # for which the string columns are not equal is more than the threshold.
    STRINGS = "STRINGS"

    # Used for comparing date columns. The validation fails if the percentage of rows
    # for which the date columns are not equal to the YYYY-MM-DD granularity is more than
    # the threshold
    DATES = "DATES"


@attr.s(frozen=True)
class SamenessDataValidationCheck(DataValidationCheck):
    """A type of validation check which identifies validation issues by observing that values in a configured set
    of columns are not the same."""

    # The list of columns whose values should be compared
    comparison_columns: List[str] = attr.ib(factory=list)

    @comparison_columns.validator
    def _check_comparison_columns(
        self, _attribute: attr.Attribute, value: List
    ) -> None:
        if len(value) < 2:
            raise ValueError(
                f"Found only [{len(value)}] comparison columns, expected at least 2."
            )

    # Columns included in the join but not compared
    partition_columns: Optional[List[str]] = attr.ib(default=None)

    def get_partition_columns(self) -> List[str]:
        if self.partition_columns is None:
            raise ValueError(
                f"Partition columns must be set for check [{self.validation_name}]"
            )
        return self.partition_columns

    # The type of sameness check this is
    sameness_check_type: SamenessDataValidationCheckType = attr.ib(
        default=SamenessDataValidationCheckType.NUMBERS
    )

    # The acceptable margin of error across the range of compared values. Defaults to 0.0 (no difference allowed)
    max_allowed_error: float = attr.ib(default=0.0)

    @max_allowed_error.validator
    def _check_max_allowed_error(
        self, _attribute: attr.Attribute, value: float
    ) -> None:
        if not isinstance(value, float):
            raise ValueError(
                f"Unexpected type [{type(value)}] for error value [{value}]"
            )

        if not 0.0 <= value <= 1.0:
            raise ValueError(
                f"Allowed error value must be between 0.0 and 1.0. Found instead: [{value}]"
            )

    validation_type: ValidationCheckType = attr.ib(default=ValidationCheckType.SAMENESS)

    def updated_for_region(
        self, region_config: ValidationRegionConfig
    ) -> "SamenessDataValidationCheck":
        max_allowed_error_config = region_config.max_allowed_error_overrides.get(
            self.validation_name, None
        )
        max_allowed_error = (
            max_allowed_error_config.max_allowed_error_override
            if max_allowed_error_config
            else self.max_allowed_error
        )
        return attr.evolve(self, max_allowed_error=max_allowed_error)


RowValueType = TypeVar("RowValueType", float, str)


@attr.s(frozen=True, kw_only=True)
class ResultRow(Generic[RowValueType]):
    # Values from the non-comparison columns
    label_values: Tuple[str, ...] = attr.ib()

    # Values from the comparison columns
    comparison_values: Tuple[RowValueType, ...] = attr.ib()


@attr.s(frozen=True, kw_only=True)
class SamenessPerViewValidationResultDetails(DataValidationJobResultDetails):
    """Stores result details for a sameness validation check in which the margin
    of error is calculated at the view level (encompassing all rows)."""

    num_error_rows: int = attr.ib()
    total_num_rows: int = attr.ib()
    max_allowed_error: float = attr.ib()

    # This field is not used directly in checking for failures but provides additional
    # context for analyzing results.
    #
    # For each unique set of label column values in the results, this contains the
    # number of non-null values for each comparison column. For most checks this will
    # have some logical meaning, such as the internal and external populations for a
    # single day. E.g. [
    #     (("US_XX", "2021-01-31"), {"internal_id": 3, "external_id": 3}),
    #     (("US_XX", "2020-12-31"), {"internal_id": 2, "external_id": 3})
    # ]
    non_null_counts_per_column_per_partition: List[
        Tuple[Tuple[str, ...], Dict[str, int]]
    ] = attr.ib()

    @property
    def error_rate(self) -> float:
        return (
            (self.num_error_rows / self.total_num_rows)
            if self.total_num_rows > 0
            else 0.0
        )

    def was_successful(self) -> bool:
        return self.error_rate <= self.max_allowed_error

    def failure_description(self) -> Optional[str]:
        if self.was_successful():
            return None
        return (
            f"{self.num_error_rows} out of {self.total_num_rows} row(s) did not contain matching "
            f"strings. The acceptable margin of error is only {self.max_allowed_error}, "
            f"but the validation returned an error rate of {round(self.error_rate, 4)}."
        )


@attr.s(frozen=True, kw_only=True)
class SamenessPerRowValidationResultDetails(DataValidationJobResultDetails):
    """Stores result details for a sameness validation check in which the margin of error
    is calculated at the row level."""

    # List of failed rows, where each entry is a tuple containing the row and the amount
    # of error for that row.
    failed_rows: List[Tuple[ResultRow[float], float]] = attr.ib()
    max_allowed_error: float = attr.ib()

    @property
    def highest_error(self) -> Optional[float]:
        return (
            round(max(row[1] for row in self.failed_rows), 4)
            if self.failed_rows
            else None
        )

    def was_successful(self) -> bool:
        return not self.failed_rows

    def failure_description(self) -> Optional[str]:
        if self.was_successful():
            return None
        return (
            f"{len(self.failed_rows)} row(s) had unacceptable margins of error. The "
            f"acceptable margin of error is only {self.max_allowed_error}, but the "
            f"validation returned rows with errors as high as {self.highest_error}."
        )


class SamenessValidationChecker(ValidationChecker[SamenessDataValidationCheck]):
    """Performs the validation check for sameness check types."""

    @classmethod
    def run_check(
        cls, validation_job: DataValidationJob[SamenessDataValidationCheck]
    ) -> DataValidationJobResult:
        comparison_columns = validation_job.validation.comparison_columns
        max_allowed_error = validation_job.validation.max_allowed_error

        query_job = BigQueryClientImpl().run_query_async(validation_job.query_str(), [])

        if (
            validation_job.validation.sameness_check_type
            == SamenessDataValidationCheckType.NUMBERS
        ):
            return SamenessValidationChecker.run_check_per_row(
                validation_job, comparison_columns, max_allowed_error, query_job
            )
        if (
            validation_job.validation.sameness_check_type
            == SamenessDataValidationCheckType.STRINGS
        ):
            return SamenessValidationChecker.run_check_per_view(
                validation_job,
                comparison_columns,
                max_allowed_error,
                query_job,
                str,
                EMPTY_STRING_VALUE,
            )
        if (
            validation_job.validation.sameness_check_type
            == SamenessDataValidationCheckType.DATES
        ):
            return SamenessValidationChecker.run_check_per_view(
                validation_job,
                comparison_columns,
                max_allowed_error,
                query_job,
                datetime.date,
                EMPTY_DATE_VALUE,
            )

        raise ValueError(
            f"Unexpected sameness_check_type of {validation_job.validation.sameness_check_type}."
        )

    @staticmethod
    def run_check_per_row(
        validation_job: DataValidationJob[SamenessDataValidationCheck],
        comparison_columns: List[str],
        max_allowed_error: float,
        query_job: QueryJob,
    ) -> DataValidationJobResult:
        """Performs the validation check for sameness check types, where the values being compares are numbers (either
        ints or floats)."""
        failed_rows: List[Tuple[ResultRow[float], float]] = []

        row: Row
        for row in query_job:
            label_values: List[str] = []
            comparison_values: List[float] = []
            for column, value in row.items():
                if column in comparison_columns:
                    if value is None:
                        raise ValueError(
                            f"Unexpected None value for column [{column}] in validation "
                            f"[{validation_job.validation.validation_name}]."
                        )
                    try:
                        float_value = float(value)
                    except ValueError as e:
                        raise ValueError(
                            f"Could not cast value [{value}] in column [{column}] to a float in validation "
                            f"[{validation_job.validation.validation_name}]."
                        ) from e
                    comparison_values.append(float_value)
                else:
                    label_values.append(str(value))

            max_value = max(comparison_values)
            min_value = min(comparison_values)

            # If max and min are 0, then there's no issue
            if max_value == 0 and min_value == 0:
                break

            # If comparing negative values to 0, swap min and max
            if max_value == 0 and min_value < 0:
                max_value, min_value = min_value, max_value

            error = (max_value - min_value) / max_value
            if error > max_allowed_error:
                failed_rows.append(
                    (
                        ResultRow(
                            label_values=tuple(label_values),
                            comparison_values=tuple(comparison_values),
                        ),
                        error,
                    )
                )

        return DataValidationJobResult(
            validation_job=validation_job,
            result_details=SamenessPerRowValidationResultDetails(
                failed_rows=failed_rows, max_allowed_error=max_allowed_error
            ),
        )

    @staticmethod
    def run_check_per_view(
        validation_job: DataValidationJob[SamenessDataValidationCheck],
        comparison_columns: List[str],
        max_allowed_error: float,
        query_job: QueryJob,
        type_to_check: Type[PerViewRowType],
        empty_value: PerViewRowType,
    ) -> DataValidationJobResult:
        """Performs the validation check for sameness check types, where the values being compared are strings."""
        num_errors = 0
        num_rows = 0
        non_null_counts_per_column_per_partition: Dict[
            Tuple[str, ...], Dict[str, int]
        ] = {}

        row: Row
        for row in query_job:
            num_rows += 1
            unique_values: Set[PerViewRowType] = set()

            partition_key = tuple(
                str(row.get(column))
                for column in validation_job.validation.get_partition_columns()
            )
            if partition_key not in non_null_counts_per_column_per_partition:
                non_null_counts_per_column_per_partition[partition_key] = {
                    column: 0 for column in comparison_columns
                }
            non_null_counts_per_column = non_null_counts_per_column_per_partition[
                partition_key
            ]

            for column in comparison_columns:
                value = row[column]
                if value is None:
                    unique_values.add(empty_value)
                elif isinstance(value, type_to_check):
                    non_null_counts_per_column[column] += 1
                    unique_values.add(value)
                else:
                    raise ValueError(
                        f"Unexpected type [{type(value)}] for value [{value}] in {validation_job.validation.sameness_check_type.value} validation "
                        f"[{validation_job.validation.validation_name}]."
                    )

            # If there is more than one unique value in the row, then there's an issue
            if len(unique_values) > 1:
                # Increment the number of errors
                num_errors += 1

        return DataValidationJobResult(
            validation_job=validation_job,
            result_details=SamenessPerViewValidationResultDetails(
                num_error_rows=num_errors,
                total_num_rows=num_rows,
                max_allowed_error=max_allowed_error,
                non_null_counts_per_column_per_partition=list(
                    non_null_counts_per_column_per_partition.items()
                ),
            ),
        )
