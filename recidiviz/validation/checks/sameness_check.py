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
from typing import List

import attr

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.validation.checks.validation_checker import ValidationChecker
from recidiviz.validation.validation_models import ValidationCheckType, \
    DataValidationCheck, DataValidationJob, DataValidationJobResult


@attr.s(frozen=True)
class SamenessDataValidationCheck(DataValidationCheck):
    """A type of validation check which identifies validation issues by observing that values in a configured set
    of columns are not the same."""

    # The list of columns whose values should be compared
    comparison_columns: List[str] = attr.ib()

    # The acceptable margin of error across the range of compared values. Defaults to 0.0 (no difference allowed)
    max_allowed_error: float = attr.ib(default=0.0)

    validation_type: ValidationCheckType = attr.ib(default=ValidationCheckType.SAMENESS)


class SamenessValidationChecker(ValidationChecker[SamenessDataValidationCheck]):
    """Performs the validation check for sameness check types."""

    @classmethod
    def run_check(cls, validation_job: DataValidationJob[SamenessDataValidationCheck]) -> DataValidationJobResult:
        was_successful = True
        failed_rows = []

        comparison_columns = validation_job.validation.comparison_columns
        max_allowed_error = validation_job.validation.max_allowed_error

        query_job = BigQueryClientImpl().run_query_async(validation_job.query_str())

        for row in query_job:
            comparison_values = [row[column] for column in comparison_columns]
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

        highest_error = round(max(failed_rows), 2) if failed_rows else None

        description = f'{len(failed_rows)} row(s) had unacceptable margins of error. The acceptable ' \
                      f'margin of error is only {max_allowed_error}, but the validation returned rows with errors ' \
                      f'as high as {highest_error}.' \
            if not was_successful else None
        return DataValidationJobResult(validation_job=validation_job,
                                       was_successful=was_successful,
                                       failure_description=description)
