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
from recidiviz.validation.validation_config import ValidationRegionConfig


@attr.s(frozen=True)
class ExistenceDataValidationCheck(DataValidationCheck):
    """A type of validation check which identifies validation issues through the existence of any row returned by the
    validation query."""

    validation_type: ValidationCheckType = attr.ib(default=ValidationCheckType.EXISTENCE)

    num_allowed_rows: int = attr.ib(default=0)

    def updated_for_region(self, region_config: ValidationRegionConfig) -> 'ExistenceDataValidationCheck':
        num_allowed_rows_config = region_config.num_allowed_rows_overrides.get(self.validation_name, None)
        num_allowed_rows = num_allowed_rows_config.num_allowed_rows_override \
            if num_allowed_rows_config else self.num_allowed_rows

        return attr.evolve(self, num_allowed_rows=num_allowed_rows)


class ExistenceValidationChecker(ValidationChecker[ExistenceDataValidationCheck]):
    """Performs the validation check for existence check types."""

    @classmethod
    def run_check(cls, validation_job: DataValidationJob[ExistenceDataValidationCheck]) -> DataValidationJobResult:
        was_successful = True
        invalid_rows = 0
        query_job = BigQueryClientImpl().run_query_async(validation_job.query_str(), [])

        # We need to iterate over the collection to initialize the query result set
        for _ in query_job:
            invalid_rows += 1
            if validation_job.validation.num_allowed_rows < invalid_rows:
                was_successful = False

        description = None
        if not was_successful:
            num_allowed_rows = validation_job.validation.num_allowed_rows
            description = f'Found [{invalid_rows}] invalid rows, though [{num_allowed_rows}] were expected'

        return DataValidationJobResult(validation_job=validation_job,
                                       was_successful=was_successful,
                                       failure_description=description)
