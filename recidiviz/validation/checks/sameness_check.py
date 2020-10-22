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
from typing import List, Set

import attr
from google.cloud.bigquery import QueryJob

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.validation.checks.validation_checker import ValidationChecker
from recidiviz.validation.validation_models import ValidationCheckType, \
    DataValidationCheck, DataValidationJob, DataValidationJobResult
from recidiviz.validation.validation_config import ValidationRegionConfig

EMPTY_STRING_VALUE = 'EMPTY_STRING_VALUE'


class SamenessDataValidationCheckType(Enum):
    # For comparing integers and/or floats
    NUMBERS = 'NUMBERS'

    # For comparing strings
    STRINGS = 'STRINGS'


@attr.s(frozen=True)
class SamenessDataValidationCheck(DataValidationCheck):
    """A type of validation check which identifies validation issues by observing that values in a configured set
    of columns are not the same."""

    # The list of columns whose values should be compared
    comparison_columns: List[str] = attr.ib(factory=list)
    @comparison_columns.validator
    def _check_comparison_columns(self, _attribute: attr.Attribute, value: List) -> None:
        if len(value) < 2:
            raise ValueError(f'Found only [{len(value)}] comparison columns, expected at least 2.')

    # The type of sameness check this is
    sameness_check_type: SamenessDataValidationCheckType = attr.ib(default=SamenessDataValidationCheckType.NUMBERS)

    # The acceptable margin of error across the range of compared values. Defaults to 0.0 (no difference allowed)
    max_allowed_error: float = attr.ib(default=0.0)
    @max_allowed_error.validator
    def _check_max_allowed_error(self, _attribute: attr.Attribute, value: float) -> None:
        if not isinstance(value, float):
            raise ValueError(f'Unexpected type [{type(value)}] for error value [{value}]')

        if not 0.0 <= value <= 1.0:
            raise ValueError(f'Allowed error value must be between 0.0 and 1.0. Found instead: [{value}]')

    validation_type: ValidationCheckType = attr.ib(default=ValidationCheckType.SAMENESS)

    def updated_for_region(self, region_config: ValidationRegionConfig) -> 'SamenessDataValidationCheck':
        max_allowed_error_config = region_config.max_allowed_error_overrides.get(self.validation_name, None)
        max_allowed_error = max_allowed_error_config.max_allowed_error_override \
            if max_allowed_error_config else self.max_allowed_error
        return attr.evolve(self, max_allowed_error=max_allowed_error)


class SamenessValidationChecker(ValidationChecker[SamenessDataValidationCheck]):
    """Performs the validation check for sameness check types."""

    @classmethod
    def run_check(cls, validation_job: DataValidationJob[SamenessDataValidationCheck]) -> DataValidationJobResult:
        comparison_columns = validation_job.validation.comparison_columns
        max_allowed_error = validation_job.validation.max_allowed_error

        query_job = BigQueryClientImpl().run_query_async(validation_job.query_str(), [])

        if validation_job.validation.sameness_check_type == SamenessDataValidationCheckType.NUMBERS:
            return SamenessValidationChecker.run_check_for_numbers(
                validation_job, comparison_columns, max_allowed_error, query_job)
        if validation_job.validation.sameness_check_type == SamenessDataValidationCheckType.STRINGS:
            return SamenessValidationChecker.run_check_for_strings(
                validation_job, comparison_columns, max_allowed_error, query_job)

        raise ValueError(f"Unexpected sameness_check_type of {validation_job.validation.sameness_check_type}.")

    @staticmethod
    def run_check_for_numbers(validation_job: DataValidationJob[SamenessDataValidationCheck],
                              comparison_columns: List[str],
                              max_allowed_error: float,
                              query_job: QueryJob) -> DataValidationJobResult:
        """Performs the validation check for sameness check types, where the values being compares are numbers (either
        ints or floats)."""
        was_successful = True
        failed_rows = []

        for row in query_job:
            comparison_values: List[float] = []
            for column in comparison_columns:
                if row[column] is None:
                    raise ValueError(
                        f'Unexpected None value for column [{column}] in validation '
                        f'[{validation_job.validation.validation_name}].')
                try:
                    float_value = float(row[column])
                except ValueError as e:
                    raise ValueError(
                        f'Could not cast value [{row[column]}] in column [{column}] to a float in validation '
                        f'[{validation_job.validation.validation_name}].') from e
                comparison_values.append(float_value)

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
                was_successful = False
                failed_rows.append(error)

        highest_error = round(max(failed_rows), 4) if failed_rows else None

        description = f'{len(failed_rows)} row(s) had unacceptable margins of error. The acceptable ' \
                      f'margin of error is only {max_allowed_error}, but the validation returned rows with errors ' \
                      f'as high as {highest_error}.' \
            if not was_successful else None
        return DataValidationJobResult(validation_job=validation_job,
                                       was_successful=was_successful,
                                       failure_description=description)

    @staticmethod
    def run_check_for_strings(validation_job: DataValidationJob[SamenessDataValidationCheck],
                              comparison_columns: List[str],
                              max_allowed_error: float,
                              query_job: QueryJob) -> DataValidationJobResult:
        """Performs the validation check for sameness check types, where the values being compared are strings."""
        num_errors = 0
        num_rows = 0

        for row in query_job:
            num_rows += 1
            unique_string_values: Set[str] = set()

            for column in comparison_columns:
                value = row[column]
                if value is None:
                    unique_string_values.add(EMPTY_STRING_VALUE)
                elif isinstance(value, str):
                    unique_string_values.add(value)
                else:
                    raise ValueError(
                        f'Unexpected type [{type(value)}] for value [{value}] in STRING validation '
                        f'[{validation_job.validation.validation_name}].')

            # If there is more than one unique string value in the row, then there's an issue
            if len(unique_string_values) > 1:
                # Increment the number of errors
                num_errors += 1

        error_rate = (num_errors / num_rows) if num_rows > 0 else 0.0
        was_successful = error_rate <= max_allowed_error

        description = f'{num_errors} out of {num_rows} row(s) did not contain matching strings. The acceptable ' \
                      f'margin of error is only {max_allowed_error}, but the validation returned an error rate of ' \
                      f'{error_rate}.' \
            if not was_successful else None
        return DataValidationJobResult(validation_job=validation_job,
                                       was_successful=was_successful,
                                       failure_description=description)
