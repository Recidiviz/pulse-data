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

from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple

import attr
from google.cloud.bigquery.table import Row

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.validation.validation_config import ValidationRegionConfig
from recidiviz.validation.validation_models import (
    DataValidationCheck,
    DataValidationJob,
    DataValidationJobResult,
    DataValidationJobResultDetails,
    ValidationChecker,
    ValidationCheckType,
    ValidationResultStatus,
    validate_result_status,
)


class SamenessDataValidationCheckType(Enum):
    # Used for comparing integer and/or float columns. The validation fails if the two
    # numbers differ by a percentage larger than the threshold for any single row.
    PER_ROW = "PER_ROW"

    # Used for comparing categorical columns. The validation fails if the percentage of rows
    # for which the categorical columns are not equal is more than the threshold.
    PER_VIEW = "PER_VIEW"


ERROR_ROWS_VIEW_BUILDER_TEMPLATE: str = """
    /*{description}*/
    WITH validation as (
        {validation_view}
    )
    {validation_check}
    """


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

    # The type of sameness check this is
    sameness_check_type: SamenessDataValidationCheckType = attr.ib(
        default=SamenessDataValidationCheckType.PER_ROW
    )

    # The acceptable margin of error across the range of compared values. Defaults to 0.02 (small difference allowed)
    hard_max_allowed_error: float = attr.ib(default=0.02)
    soft_max_allowed_error: float = attr.ib(default=0.02)

    @hard_max_allowed_error.validator
    def _check_hard_max_allowed_error(
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

        if value < self.soft_max_allowed_error:
            raise ValueError(
                f"Value cannot be less than soft_max_allowed_error. "
                f"Found instead: {value} vs. {self.soft_max_allowed_error}. "
                f"Make sure you are setting both errors."
            )

    @soft_max_allowed_error.validator
    def _check_soft_max_allowed_error(
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

        if value > self.hard_max_allowed_error:
            raise ValueError(
                f"Value cannot be greater than hard_max_allowed_error. "
                f"Found instead: {value} vs. {self.hard_max_allowed_error}"
            )

    # Used to override PER_ROW max_allowed_errors for each region when creating error validation query
    region_configs: Optional[Dict[str, ValidationRegionConfig]] = attr.ib(default=None)

    @region_configs.validator
    def _check_region_configs(
        self,
        _attribute: attr.Attribute,
        value: Optional[Dict[str, ValidationRegionConfig]],
    ) -> None:
        if (
            self.sameness_check_type == SamenessDataValidationCheckType.PER_ROW
            and value is None
        ):
            raise ValueError(
                "Region configs has to be set for sameness validations where the sameness check type is PER_ROW"
            )

    validation_type: ValidationCheckType = attr.ib(default=ValidationCheckType.SAMENESS)

    @property
    def managed_view_builders(self) -> List[SimpleBigQueryViewBuilder]:
        return [self.view_builder, self.error_view_builder]

    @property
    def error_view_builder(self) -> SimpleBigQueryViewBuilder:
        # TODO(#8646): Build a way for view builders to depend on other view *builders*
        #  so that we don't have to build the view query for the parent view while we
        #  are constructing the builder for the error rows view here.
        validation_view_query = self.view_builder.build().view_query

        view_id = self.view_builder.view_id
        if self.validation_name_suffix is not None:
            view_id += f"_{self.validation_name_suffix}"

        return SimpleBigQueryViewBuilder(
            dataset_id=self.view_builder.dataset_id,
            view_id=f"{view_id}_errors",
            view_query_template=ERROR_ROWS_VIEW_BUILDER_TEMPLATE,
            description=self.view_builder.description,
            validation_view=validation_view_query,
            should_materialize=self.view_builder.should_materialize,
            validation_check=self.get_checker().get_validation_query_str(self),
        )

    def updated_for_region(
        self, region_config: ValidationRegionConfig
    ) -> "SamenessDataValidationCheck":
        max_allowed_error_config = region_config.max_allowed_error_overrides.get(
            self.validation_name, None
        )

        if region_config.num_allowed_rows_overrides.get(self.validation_name, None):
            raise ValueError(
                f"{self.validation_name} region config incorrectly set "
                f"num_allowed_rows_overrides for region {region_config.region_code}"
            )

        hard_max_allowed_error = self.hard_max_allowed_error
        soft_max_allowed_error = self.soft_max_allowed_error
        if max_allowed_error_config:
            hard_max_allowed_error = (
                max_allowed_error_config.hard_max_allowed_error_override
                or self.hard_max_allowed_error
            )
            soft_max_allowed_error = (
                max_allowed_error_config.soft_max_allowed_error_override
                or self.soft_max_allowed_error
            )

        return attr.evolve(
            self,
            dev_mode=region_config.dev_mode,
            hard_max_allowed_error=hard_max_allowed_error,
            soft_max_allowed_error=soft_max_allowed_error,
        )

    def get_checker(self) -> ValidationChecker:
        if self.sameness_check_type == SamenessDataValidationCheckType.PER_ROW:
            return SamenessPerRowValidationChecker()
        if self.sameness_check_type == SamenessDataValidationCheckType.PER_VIEW:
            return SamenessPerViewValidationChecker()
        raise ValueError(
            f"Unexpected sameness_check_type of {self.sameness_check_type}."
        )


@attr.s(frozen=True, kw_only=True)
class ResultRow:
    # Values from the non-comparison columns
    label_values: Tuple[str, ...] = attr.ib()

    # Values from the comparison columns
    comparison_values: Tuple[float, ...] = attr.ib()


@attr.s(frozen=True, kw_only=True)
class SamenessPerViewValidationResultDetails(DataValidationJobResultDetails):
    """Stores result details for a sameness validation check in which the margin
    of error is calculated at the view level (encompassing all rows)."""

    num_error_rows: int = attr.ib()
    total_num_rows: int = attr.ib()
    hard_max_allowed_error: float = attr.ib()
    soft_max_allowed_error: float = attr.ib()

    dev_mode: bool = attr.ib(default=False)

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
    def has_data(self) -> bool:
        return self.total_num_rows > 0

    @property
    def is_dev_mode(self) -> bool:
        return self.dev_mode

    @property
    def error_amount(self) -> float:
        return self.error_rate

    @property
    def error_rate(self) -> float:
        return (
            (self.num_error_rows / self.total_num_rows)
            if self.total_num_rows > 0
            else 0.0
        )

    @property
    def hard_failure_amount(self) -> float:
        return self.hard_max_allowed_error

    @property
    def soft_failure_amount(self) -> float:
        return self.soft_max_allowed_error

    @property
    def error_is_percentage(self) -> bool:
        return True

    def validation_result_status(self) -> ValidationResultStatus:
        return validate_result_status(
            self.error_rate,
            self.soft_max_allowed_error,
            self.hard_max_allowed_error,
        )

    def failure_description(self) -> Optional[str]:
        validation_result_status = self.validation_result_status()
        if validation_result_status == ValidationResultStatus.SUCCESS:
            return None
        if validation_result_status in (
            ValidationResultStatus.FAIL_SOFT,
            ValidationResultStatus.FAIL_HARD,
        ):
            error_type_text = {
                ValidationResultStatus.FAIL_SOFT: "soft",
                ValidationResultStatus.FAIL_HARD: "hard",
            }
            return (
                f"{self.num_error_rows} out of {self.total_num_rows} row(s) did not contain matching strings. "
                f"The acceptable margin of error is {self.soft_max_allowed_error} ({error_type_text[validation_result_status]}), "
                f"but the validation returned an error rate of {round(self.error_rate, 4)}."
            )
        raise AttributeError(
            f"failure_description for validation_result_status {validation_result_status} not set"
        )


@attr.s(frozen=True, kw_only=True)
class SamenessPerRowValidationResultDetails(DataValidationJobResultDetails):
    """Stores result details for a sameness validation check in which the margin of error
    is calculated at the row level."""

    # List of failed rows, where each entry is a tuple containing the row and the amount
    # of error for that row.
    failed_rows: List[Tuple[ResultRow, float]] = attr.ib()
    hard_max_allowed_error: float = attr.ib()
    soft_max_allowed_error: float = attr.ib()

    dev_mode: bool = attr.ib(default=False)

    @property
    def has_data(self) -> bool:
        return True

    @property
    def is_dev_mode(self) -> bool:
        return self.dev_mode

    @property
    def error_amount(self) -> float:
        return self.highest_error

    @property
    def hard_failure_amount(self) -> float:
        return self.hard_max_allowed_error

    @property
    def soft_failure_amount(self) -> float:
        return self.soft_max_allowed_error

    @property
    def error_is_percentage(self) -> bool:
        return True

    @property
    def highest_error(self) -> float:
        return (
            round(max(row[1] for row in self.failed_rows), 4) if self.failed_rows else 0
        )

    @property
    def rows_soft_failure(self) -> Optional[List[Tuple[ResultRow, float]]]:
        return self._filter_row_of_result_status(ValidationResultStatus.FAIL_SOFT)

    @property
    def rows_hard_failure(self) -> Optional[List[Tuple[ResultRow, float]]]:
        return self._filter_row_of_result_status(ValidationResultStatus.FAIL_HARD)

    def _filter_row_of_result_status(
        self, result_status_filter: ValidationResultStatus
    ) -> Optional[List[Tuple[ResultRow, float]]]:
        return (
            [
                row
                for row in self.failed_rows
                if (
                    result_status_filter
                    == validate_result_status(
                        row[1], self.soft_max_allowed_error, self.hard_max_allowed_error
                    )
                )
            ]
            if self.failed_rows
            else None
        )

    def validation_result_status(self) -> ValidationResultStatus:
        if not self.failed_rows:
            return ValidationResultStatus.SUCCESS
        if not self.highest_error:
            raise AttributeError(
                f"highest_error should not be null since failed_rows is not null. failed_rows: {self.failed_rows}"
            )
        return validate_result_status(
            self.highest_error,
            self.soft_max_allowed_error,
            self.hard_max_allowed_error,
        )

    def failure_description(self) -> Optional[str]:
        validation_result_status = self.validation_result_status()
        if validation_result_status == ValidationResultStatus.SUCCESS:
            return None
        if validation_result_status == ValidationResultStatus.FAIL_SOFT:
            return (
                f"{len(self.failed_rows)} row(s) exceeded the soft_max_allowed_error threshold. The "
                f"acceptable margin of error is {self.soft_max_allowed_error} (soft), but the "
                f"validation returned rows with errors as high as {self.highest_error}."
            )
        if validation_result_status == ValidationResultStatus.FAIL_HARD:
            return (
                f"{len(self.failed_rows)} row(s) had unacceptable margins of error. Of those rows, "
                f"{len(self.rows_hard_failure or [])} row(s) exceeded the hard threshold and "
                f"{len(self.rows_soft_failure or [])} row(s) exceeded the soft threshold. The "
                f"acceptable margin of error is only {self.hard_max_allowed_error} (hard) "
                f"and {self.soft_max_allowed_error} (soft), but the "
                f"validation returned rows with errors as high as {self.highest_error}."
            )
        raise AttributeError(
            f"failure_description for validation_result_status {validation_result_status} not set"
        )


class SamenessPerRowValidationChecker(ValidationChecker[SamenessDataValidationCheck]):
    """
    Performs the validation check for sameness check type PerRow.
    This is done by checking each row of data and calculating the error rate for that row.
    Each row should be a number.
    """

    @classmethod
    def run_check(
        cls, validation_job: DataValidationJob[SamenessDataValidationCheck]
    ) -> DataValidationJobResult:
        comparison_columns = validation_job.validation.comparison_columns
        validation = validation_job.validation

        error_query_job = BigQueryClientImpl().run_query_async(
            validation_job.error_builder_query_str(), []
        )

        failed_rows: List[Tuple[ResultRow, float]] = []

        row: Row
        for row in error_query_job:
            label_values: List[str] = []
            comparison_values: List[float] = []

            for column, value in row.items():
                if column in ["error_rate", "error_type"]:
                    continue
                if column in comparison_columns:
                    if value is None:
                        raise ValueError(
                            f"Unexpected None value for column [{column}] in validation "
                            f"[{validation.validation_name}]."
                        )
                    try:
                        float_value = float(value)
                    except ValueError as e:
                        raise ValueError(
                            f"Could not cast value [{value}] in column [{column}] to a float in validation "
                            f"[{validation.validation_name}]."
                        ) from e
                    comparison_values.append(float_value)
                else:
                    label_values.append(str(value))

            error = float(row.get("error_rate"))
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
                failed_rows=failed_rows,
                dev_mode=validation.dev_mode,
                hard_max_allowed_error=validation.hard_max_allowed_error,
                soft_max_allowed_error=validation.soft_max_allowed_error,
            ),
        )

    @classmethod
    def get_validation_query_str(
        cls, validation_check: SamenessDataValidationCheck
    ) -> str:
        """Creates a query that adds the error rate between the comparison columns for each row of data.
        It takes into account region code soft max allowed error overrides and only returns rows that exceed the max."""

        comparison_str = ", ".join(validation_check.comparison_columns)
        error_rate_filters: List[str] = []
        set_config_error_types: List[str] = []
        region_code_with_filter_overrides: List[str] = []

        if validation_check.region_configs is None:
            raise ValueError("Expected nonnull region_configs at this point.")

        for region_code, region_config in validation_check.region_configs.items():
            soft_max_allowed_error = (
                _get_soft_max_allowed_error_override_for_validation_name(
                    region_config, validation_check.validation_name
                )
            )
            hard_max_allowed_error = (
                _get_hard_max_allowed_error_override_for_validation_name(
                    region_config, validation_check.validation_name
                )
            )
            if soft_max_allowed_error is not None or hard_max_allowed_error is not None:
                region_code_with_filter_overrides.append(f"{region_code}")
                error_rate_filters.append(
                    f"(region_code = '{region_code}' AND error_rate > {soft_max_allowed_error or validation_check.soft_max_allowed_error})"
                )
                set_config_error_types.append(
                    f"WHEN '{region_code}' THEN IF(error_rate <= {soft_max_allowed_error or validation_check.soft_max_allowed_error}, null, IF(error_rate <= {hard_max_allowed_error or validation_check.hard_max_allowed_error}, CAST('soft' AS STRING), CAST('hard' AS STRING)))"
                )

        basic_error_type = f"IF(error_rate <= {validation_check.soft_max_allowed_error}, null, IF(error_rate <= {validation_check.hard_max_allowed_error}, CAST('soft' AS STRING), CAST('hard' AS STRING)))"
        set_error_type_str = f" {basic_error_type} as error_type"
        error_rate_filters_str = f"error_rate > {validation_check.soft_max_allowed_error} OR error_rate IS NULL"
        # use region code filters instead if at least one region code filter added to error_rate_filters
        if len(error_rate_filters) > 0:
            joined_config_error_types = "\n\t\t".join(set_config_error_types)
            set_error_type_str = f"CASE region_code \n\t\t{joined_config_error_types} \n\t\tELSE {basic_error_type} \n\tEND as error_type"
            error_rate_filters.extend(
                [
                    f"(region_code NOT IN ('{', '.join(region_code_with_filter_overrides)}') AND error_rate > {validation_check.soft_max_allowed_error})",
                    "error_rate IS NULL",
                ]
            )
            error_rate_filters_str = " OR ".join(error_rate_filters)

        return f""",
    validations_potential_min_max as (
        SELECT *, ABS(GREATEST({comparison_str})) as potential_max_value,
        ABS(LEAST({comparison_str})) as potential_min_value,
        FROM validation
    ),
    validations_min_max as (
        -- Get true min and max values accounting for negative numbers
        SELECT * EXCEPT (potential_max_value, potential_min_value),
        IF(potential_max_value > potential_min_value, potential_max_value, potential_min_value) as max_value,
        IF(potential_max_value > potential_min_value, potential_min_value, potential_max_value) as min_value,
        FROM validations_potential_min_max
    ),
    validations_error_rate as (
        SELECT * EXCEPT(max_value, min_value),
        IF(max_value = 0 AND min_value = 0, CAST(0 as FLOAT64), (max_value - min_value) / CAST(max_value AS FLOAT64)) as error_rate,
        FROM validations_min_max
    )
    SELECT *, {set_error_type_str}
    FROM validations_error_rate
    WHERE {error_rate_filters_str}
    """


class SamenessPerViewValidationChecker(ValidationChecker[SamenessDataValidationCheck]):
    """
    Performs the validation check for sameness check type PerView.
    This is done by dividing the number of rows that have mismatching columns with the total number of rows in the view.
    """

    @classmethod
    def run_check(
        cls, validation_job: DataValidationJob[SamenessDataValidationCheck]
    ) -> DataValidationJobResult:
        comparison_columns = validation_job.validation.comparison_columns
        validation = validation_job.validation

        error_query_job = BigQueryClientImpl().run_query_async(
            validation_job.error_builder_query_str(), []
        )
        original_query_job = BigQueryClientImpl().run_query_async(
            validation_job.original_builder_query_str(), []
        )

        num_errors = len(list(error_query_job))
        num_rows = 0
        non_null_counts_per_column_per_partition: Dict[
            Tuple[str, ...], Dict[str, int]
        ] = {}

        row: Row
        for row in original_query_job:
            num_rows += 1
            unique_values: Set[Any] = set()
            partition_key = (
                tuple(str(row.get(column)) for column in validation.partition_columns)
                if validation.partition_columns
                else tuple()
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
                    unique_values.add(None)
                else:
                    non_null_counts_per_column[column] += 1
                    unique_values.add(value)

        return DataValidationJobResult(
            validation_job=validation_job,
            result_details=SamenessPerViewValidationResultDetails(
                num_error_rows=num_errors,
                total_num_rows=num_rows,
                dev_mode=validation.dev_mode,
                hard_max_allowed_error=validation.hard_max_allowed_error,
                soft_max_allowed_error=validation.soft_max_allowed_error,
                non_null_counts_per_column_per_partition=list(
                    non_null_counts_per_column_per_partition.items()
                ),
            ),
        )

    @classmethod
    def get_validation_query_str(
        cls, validation_check: SamenessDataValidationCheck
    ) -> str:
        comparison_str = ", ".join(validation_check.comparison_columns)

        return f""",
    validation_unique_columns as (
        SELECT *,
        ARRAY_LENGTH( ARRAY(SELECT DISTINCT * FROM UNNEST(ARRAY[{comparison_str}]))) as unique_count
        FROM validation
    )
    SELECT * EXCEPT(unique_count) FROM validation_unique_columns
    WHERE unique_count > 1
    """


def _get_soft_max_allowed_error_override_for_validation_name(
    region_config: ValidationRegionConfig, validation_name: str
) -> Optional[float]:
    soft_max_allowed_error_override: Optional[float] = None
    if (
        validation_name not in region_config.exclusions
        and validation_name in region_config.max_allowed_error_overrides
    ):
        soft_max_allowed_error_override = region_config.max_allowed_error_overrides[
            validation_name
        ].soft_max_allowed_error_override
    return soft_max_allowed_error_override


def _get_hard_max_allowed_error_override_for_validation_name(
    region_config: ValidationRegionConfig, validation_name: str
) -> Optional[float]:
    hard_max_allowed_error_override: Optional[float] = None
    if (
        validation_name not in region_config.exclusions
        and validation_name in region_config.max_allowed_error_overrides
    ):
        hard_max_allowed_error_override = region_config.max_allowed_error_overrides[
            validation_name
        ].hard_max_allowed_error_override
    return hard_max_allowed_error_override
