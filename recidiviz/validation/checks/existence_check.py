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

from typing import Optional

import attr
import more_itertools

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.validation.checks.validation_checker import ValidationChecker
from recidiviz.validation.validation_config import ValidationRegionConfig
from recidiviz.validation.validation_models import (
    DataValidationCheck,
    DataValidationJob,
    DataValidationJobResult,
    DataValidationJobResultDetails,
    ValidationCheckType,
    ValidationResultStatus,
    validate_result_status,
)


@attr.s(frozen=True)
class ExistenceDataValidationCheck(DataValidationCheck):
    """A type of validation check which identifies validation issues through the existence of any row returned by the
    validation query."""

    validation_type: ValidationCheckType = attr.ib(
        default=ValidationCheckType.EXISTENCE
    )

    hard_num_allowed_rows: int = attr.ib(default=0)
    soft_num_allowed_rows: int = attr.ib(default=0)

    def __attrs_post_init__(self) -> None:
        if self.hard_num_allowed_rows < self.soft_num_allowed_rows:
            raise ValueError(
                f"soft_num_allowed_rows cannot be greater than hard_num_allowed_rows. "
                f"Found instead: {self.soft_num_allowed_rows} vs. {self.hard_num_allowed_rows}"
            )

    def updated_for_region(
        self, region_config: ValidationRegionConfig
    ) -> "ExistenceDataValidationCheck":
        num_allowed_rows_config = region_config.num_allowed_rows_overrides.get(
            self.validation_name, None
        )

        if region_config.max_allowed_error_overrides.get(self.validation_name, None):
            raise ValueError(
                f"{self.validation_name} region config incorrectly set "
                f"max_allowed_error_overrides for region {region_config.region_code}"
            )

        hard_num_allowed_rows = self.hard_num_allowed_rows
        soft_num_allowed_rows = self.soft_num_allowed_rows
        if num_allowed_rows_config:
            hard_num_allowed_rows = (
                num_allowed_rows_config.hard_num_allowed_rows_override
                or self.hard_num_allowed_rows
            )
            soft_num_allowed_rows = (
                num_allowed_rows_config.soft_num_allowed_rows_override
                or self.soft_num_allowed_rows
            )

        return attr.evolve(
            self,
            dev_mode=region_config.dev_mode,
            hard_num_allowed_rows=hard_num_allowed_rows,
            soft_num_allowed_rows=soft_num_allowed_rows,
        )


@attr.s(frozen=True, kw_only=True)
class ExistenceValidationResultDetails(DataValidationJobResultDetails):
    """A type of validation check which identifies validation issues through the number of invalid rows returned by the
    validation query."""

    num_invalid_rows: int = attr.ib()
    hard_num_allowed_rows: int = attr.ib()
    soft_num_allowed_rows: int = attr.ib()

    dev_mode: bool = attr.ib(default=False)

    @property
    def has_data(self) -> bool:
        return True

    @property
    def is_dev_mode(self) -> bool:
        return self.dev_mode

    @property
    def error_amount(self) -> float:
        return self.num_invalid_rows

    @property
    def hard_failure_amount(self) -> float:
        return self.hard_num_allowed_rows

    @property
    def soft_failure_amount(self) -> float:
        return self.soft_num_allowed_rows

    @property
    def error_is_percentage(self) -> bool:
        return False

    def validation_result_status(self) -> ValidationResultStatus:
        return validate_result_status(
            self.num_invalid_rows,
            self.soft_num_allowed_rows,
            self.hard_num_allowed_rows,
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
                f"Found [{self.num_invalid_rows}] invalid rows, more than the allowed "
                f"[{self.soft_num_allowed_rows}] ({error_type_text[validation_result_status]})"
            )
        raise AttributeError(
            f"failure_description for validation_result_status {validation_result_status} not set"
        )


class ExistenceValidationChecker(ValidationChecker[ExistenceDataValidationCheck]):
    """Performs the validation check for existence check types."""

    @classmethod
    def run_check(
        cls, validation_job: DataValidationJob[ExistenceDataValidationCheck]
    ) -> DataValidationJobResult:
        query_job = BigQueryClientImpl().run_query_async(validation_job.query_str(), [])

        return DataValidationJobResult(
            validation_job=validation_job,
            result_details=ExistenceValidationResultDetails(
                num_invalid_rows=more_itertools.ilen(query_job),
                dev_mode=validation_job.validation.dev_mode,
                hard_num_allowed_rows=validation_job.validation.hard_num_allowed_rows,
                soft_num_allowed_rows=validation_job.validation.soft_num_allowed_rows,
            ),
        )
