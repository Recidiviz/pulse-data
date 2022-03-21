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

"""Models representing data validation."""
import abc
from enum import Enum
from typing import Dict, Generic, List, Optional, TypeVar

import attr

from recidiviz.big_query.big_query_view import BigQueryView, SimpleBigQueryViewBuilder
from recidiviz.common.attr_mixins import BuildableAttr
from recidiviz.validation.validation_config import ValidationRegionConfig


class ValidationCheckType(Enum):
    EXISTENCE = "EXISTENCE"
    SAMENESS = "SAMENESS"


class ValidationCategory(Enum):
    EXTERNAL_AGGREGATE = "EXTERNAL_AGGREGATE"
    EXTERNAL_INDIVIDUAL = "EXTERNAL_INDIVIDUAL"
    CONSISTENCY = "CONSISTENCY"
    INVARIANT = "INVARIANT"
    FRESHNESS = "FRESHNESS"


class ValidationResultStatus(Enum):
    # Validation was successful and is within the desired error threshold (less than or equal to the soft threshold)
    SUCCESS = "SUCCESS"
    # Validation exceeded the soft threshold but not the hard threshold. These are known failures.
    FAIL_SOFT = "FAIL_SOFT"
    # Validation exceeded the hard threshold. These should be investigated.
    FAIL_HARD = "FAIL_HARD"


@attr.s(frozen=True)
class DataValidationCheck(BuildableAttr):
    """Models a type of validation check that can be performed."""

    # The BigQuery builder for this type of check
    view_builder: SimpleBigQueryViewBuilder = attr.ib()

    # The type of validation to be performed for this type of check
    validation_type: ValidationCheckType = attr.ib()

    # The category that the validation check falls into
    validation_category: ValidationCategory = attr.ib()

    # A suffix to add to the end of the view name to generate the validation_name.
    validation_name_suffix: Optional[str] = attr.ib(default=None)

    # If this run should be declared a success regardless of the thresholds.
    dev_mode: bool = attr.ib(default=False)

    @property
    def validation_name(self) -> str:
        return self.view_builder.view_id + (
            f"_{self.validation_name_suffix}" if self.validation_name_suffix else ""
        )

    @property
    @abc.abstractmethod
    def error_view_builder(self) -> SimpleBigQueryViewBuilder:
        """Returns generated view builder for a view where all rows represent validation
        errors. The error may be simply that the row exists at all, or there may be some
        columns that are being compared in the row which show a discrepancy.
        """

    @property
    @abc.abstractmethod
    def managed_view_builders(self) -> List[SimpleBigQueryViewBuilder]:
        """Returns the list of builders for views that should be managed via the deployment."""

    @abc.abstractmethod
    def updated_for_region(
        self, region_config: ValidationRegionConfig
    ) -> "DataValidationCheck":
        """Returns a copy of this DataValidationCheck that has been modified
        appropriately based on the region config."""

    @abc.abstractmethod
    def get_checker(self) -> "ValidationChecker":
        """Returns the validation checker to use for this validation"""


DataValidationType = TypeVar("DataValidationType", bound=DataValidationCheck)


def _query_str_for_region_code(view: BigQueryView, region_code: str) -> str:
    return f"{view.select_query} WHERE region_code = '{region_code}';"


@attr.s(frozen=True)
class DataValidationJob(Generic[DataValidationType], BuildableAttr):
    """Models a specific data validation that is to be performed for a specific region."""

    # The config for the validation to run (what we're going to check for)
    validation: DataValidationType = attr.ib()

    # The region we're going to validate (who we're going to check)
    region_code: str = attr.ib()

    # Optional dataset overrides to change which datasets will be used for query
    dataset_overrides: Optional[Dict[str, str]] = attr.ib(default=None)

    def original_builder_query_str(self) -> str:
        view = self.validation.view_builder.build(
            dataset_overrides=self.dataset_overrides
        )
        return _query_str_for_region_code(view=view, region_code=self.region_code)

    def error_builder_query_str(self) -> str:
        view = self.validation.error_view_builder.build(
            dataset_overrides=self.dataset_overrides
        )
        return _query_str_for_region_code(view=view, region_code=self.region_code)


def validate_result_status(
    error_rate: float, soft_threshold: float, hard_threshold: float
) -> ValidationResultStatus:
    if error_rate <= soft_threshold:
        return ValidationResultStatus.SUCCESS
    if error_rate <= hard_threshold:
        return ValidationResultStatus.FAIL_SOFT
    return ValidationResultStatus.FAIL_HARD


class DataValidationJobResultDetails(abc.ABC):
    """Interface for the details about a particular data validation job result."""

    @property
    @abc.abstractmethod
    def has_data(self) -> bool:
        """Whether there is data to show the result details for"""

    @property
    @abc.abstractmethod
    def is_dev_mode(self) -> bool:
        """Whether or not this was a dry run of the validation, where the threshold is not applied"""

    @property
    @abc.abstractmethod
    def error_amount(self) -> float:
        """Returns the amount of error"""

    @property
    @abc.abstractmethod
    def hard_failure_amount(self) -> float:
        """Returns the error amount that would be considered a hard failure if exceeded"""

    @property
    @abc.abstractmethod
    def soft_failure_amount(self) -> float:
        """Returns the error amount that would be considered either a soft or hard failure if exceeded.
        If not exceeded,it should be considered a success"""

    @property
    @abc.abstractmethod
    def error_is_percentage(self) -> bool:
        """Returns whether or not the error amount should be displayed as a percentage."""

    @abc.abstractmethod
    def validation_result_status(self) -> ValidationResultStatus:
        """Describes if the validation error was acceptable or unacceptable (and to what degree)"""

    @abc.abstractmethod
    def failure_description(self) -> Optional[str]:
        """Description of failure, if there was a failure"""


@attr.s(frozen=True, kw_only=True)
class DataValidationJobResult:
    """Models a data validation result that is to be reviewed."""

    # The validation which was evaluated
    validation_job: DataValidationJob = attr.ib()

    # The result of running that validation
    result_details: DataValidationJobResultDetails = attr.ib()

    @property
    def validation_result_status(self) -> ValidationResultStatus:
        """Whether or not the validation was successful"""
        return self.result_details.validation_result_status()

    def __str__(self) -> str:
        return (
            f"DataValidationJobResult["
            f"\n\tvalidation_result_status: {self.validation_result_status},"
            f"\n\tdev_mode: {self.result_details.is_dev_mode},"
            f"\n\tfailure_description: {self.result_details.failure_description()},"
            f"\n\tvalidation["
            f"\n\t\tregion_code: {self.validation_job.region_code},"
            f"\n\t\tcheck_type: {self.validation_job.validation.validation_type},"
            f"\n\t\tvalidation_category: {self.validation_job.validation.validation_category},"
            f"\n\t\tvalidation_name: {self.validation_job.validation.validation_name},"
            f"\n\t\tview_id: {self.validation_job.validation.view_builder.view_id},"
            f"\n\t]"
            f"\n]"
        )


# pylint: disable=unused-argument
class ValidationChecker(Generic[DataValidationType]):
    """Defines the interface for performing a particular kind of check."""

    @abc.abstractmethod
    def run_check(
        self, validation_job: DataValidationJob[DataValidationType]
    ) -> DataValidationJobResult:
        pass

    @classmethod
    def get_validation_query_str(cls, validation_check: DataValidationType) -> str:
        return ""
