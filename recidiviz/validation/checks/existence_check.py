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

"""Models an existence check, which identifies a validation issue by observing that there is any row returned
in a given validation result set."""

import attr

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.validation.checks.validation_checker import ValidationChecker
from recidiviz.validation.validation_models import ValidationCheckType, \
    DataValidationCheck, DataValidationJob, DataValidationJobResult


@attr.s(frozen=True)
class ExistenceDataValidationCheck(DataValidationCheck):
    """A type of validation check which identifies validation issues through the existence of any row returned by the
    validation query."""

    validation_type: ValidationCheckType = attr.ib(default=ValidationCheckType.EXISTENCE)


class ExistenceValidationChecker(ValidationChecker[ExistenceDataValidationCheck]):
    """Performs the validation check for existence check types."""

    @classmethod
    def run_check(cls, validation_job: DataValidationJob[ExistenceDataValidationCheck]) -> DataValidationJobResult:
        was_successful = True
        invalid_rows = 0
        query_job = BigQueryClientImpl().run_query_async(validation_job.query_str())

        # We need to iterate over the collection to initialize the query result set
        for _ in query_job:
            was_successful = False
            invalid_rows += 1

        description = f'Found {invalid_rows} invalid rows, though 0 were expected' if not was_successful else None
        return DataValidationJobResult(validation_job=validation_job,
                                       was_successful=was_successful,
                                       failure_description=description)
