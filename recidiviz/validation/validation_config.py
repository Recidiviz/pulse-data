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
"""Classes containing both region-specific and global information for our validation flows."""

from enum import Enum
from typing import Dict, Optional

import attr
import yaml

from recidiviz.utils.yaml_dict import YAMLDict


class ValidationExclusionType(Enum):
    """Category for the reason why a particular validation is excluded for a region."""

    # This validation is not relevant for a given region. For example, an external accuracy check against data we've
    # never received from a state.
    DOES_NOT_APPLY = "DOES_NOT_APPLY"

    # This validation does not pass / has never passed for a particular region and work will be required to fix it. This
    # should be used sparingly and largely only during the initial development process for a region and before we ship
    # to production. Reason strings for BROKEN validations should reference tasks for fixing the validation for that
    # region.
    BROKEN = "BROKEN"


@attr.s(frozen=True)
class ValidationExclusion:
    """Configuration indicating we should exclude a specific validation for a particular region."""

    region_code: str = attr.ib()

    # Name of the validation that should be excluded for this region
    validation_name: str = attr.ib()

    # Category of exclusion
    exclusion_type: ValidationExclusionType = attr.ib()

    # A string description reason for why the validation is being excluded.
    exclusion_reason: str = attr.ib()


@attr.s(frozen=True)
class ValidationMaxAllowedErrorOverride:
    """For SamenessDataValidationChecks, provides a custom override of the maximum allowed error. """

    region_code: str = attr.ib()

    # Name of the SamenessDataValidationCheck whose error threshold should be overridden
    validation_name: str = attr.ib()

    # A string description reason for why the error threshold is being overridden. Should contain a TO-DO with a task
    # number if we plan to improve this threshold.
    override_reason: str = attr.ib()

    # The new hard_max_allowed_error value. This is the alerting threshold, so any error that exceeds this
    # should be investigated.
    hard_max_allowed_error_override: Optional[float] = attr.ib()

    # The new soft_max_allowed_error value. This sets the max desired threshold, so all errors less than or equal
    # to this would be considered successful and errors greater would either be just okay, or require investigation
    # depending on whether they they exceed the hard_max_allowed_error_override
    soft_max_allowed_error_override: Optional[float] = attr.ib()

    @hard_max_allowed_error_override.validator
    def _check_hard_max_allowed_error(
        self, _attribute: attr.Attribute, value: float
    ) -> None:
        if not isinstance(value, float) and value:
            raise ValueError(
                f"Unexpected type [{type(value)}] for error value [{value}]"
            )

        if not value:
            return

        if not 0.0 <= value <= 1.0:
            raise ValueError(
                f"Allowed error value must be between 0.0 and 1.0. Found instead: [{value}]"
            )
        if (
            self.soft_max_allowed_error_override
            and value < self.soft_max_allowed_error_override
        ):
            raise ValueError(
                f"Value cannot be less than soft_max_allowed_error_override. "
                f"Found instead: {value} vs. {self.soft_max_allowed_error_override}"
            )

    @soft_max_allowed_error_override.validator
    def _check_soft_max_allowed_error(
        self, _attribute: attr.Attribute, value: float
    ) -> None:
        if not isinstance(value, float) and value:
            raise ValueError(
                f"Unexpected type [{type(value)}] for error value [{value}]"
            )

        if not value:
            return

        if not 0.0 <= value <= 1.0:
            raise ValueError(
                f"Allowed error value must be between 0.0 and 1.0. Found instead: [{value}]"
            )
        if (
            self.hard_max_allowed_error_override
            and value > self.hard_max_allowed_error_override
        ):
            raise ValueError(
                f"Value cannot be greater than hard_max_allowed_error_override. "
                f"Found instead: {value} vs. {self.hard_max_allowed_error_override}"
            )

    def __attrs_post_init__(self) -> None:
        if (
            not self.soft_max_allowed_error_override
            and not self.hard_max_allowed_error_override
        ):
            raise ValueError(
                "soft_max_allowed_error_override and hard_max_allowed_error_override not set. "
                "One of the these variable should be set."
            )


@attr.s(frozen=True)
class ValidationNumAllowedRowsOverride:
    """For ExistenceDataValidationChecks, provides a custom override of the number of allowed rows (default is usually
    0).
    """

    region_code: str = attr.ib()

    # Name of the ExistenceDataValidationCheck validation whose error threshold should be overridden
    validation_name: str = attr.ib()

    # A string description reason for why the error threshold is being overridden. Should contain a TO-DO with a task
    # number if we plan to improve this threshold.
    override_reason: str = attr.ib()

    # The new hard_num_allowed_rows value. This is the alerting threshold, so any error that exceeds this
    # should be investigated.
    hard_num_allowed_rows_override: Optional[int] = attr.ib()

    # The new soft_num_allowed_rows value. This sets the max desired threshold, so all errors less than or equal
    # to this would be considered successful and errors greater would either be just okay, or require investigation
    # depending on whether they they exceed the hard_max_allowed_error_override
    soft_num_allowed_rows_override: Optional[int] = attr.ib()

    @hard_num_allowed_rows_override.validator
    def _check_hard_num_allowed_rows_override(
        self, _attribute: attr.Attribute, value: int
    ) -> None:
        if not value:
            return

        if value < 0:
            raise ValueError(
                f"Allowed error value must be between greater or equal to 0: [{value}]"
            )

        if (
            self.soft_num_allowed_rows_override
            and value < self.soft_num_allowed_rows_override
        ):
            raise ValueError(
                f"Value cannot be less than soft_num_allowed_rows_override. "
                f"Found instead: {value} vs. {self.soft_num_allowed_rows_override}"
            )

    @soft_num_allowed_rows_override.validator
    def _check_soft_num_allowed_rows_override(
        self, _attribute: attr.Attribute, value: int
    ) -> None:
        if not value:
            return

        if value < 0:
            raise ValueError(
                f"Allowed error value must be between greater or equal to 0: [{value}]"
            )
        if (
            self.hard_num_allowed_rows_override
            and value > self.hard_num_allowed_rows_override
        ):
            raise ValueError(
                f"Value cannot be greater than hard_max_allowed_error_override. "
                f"Found instead: {value} vs. {self.hard_num_allowed_rows_override}"
            )

    def __attrs_post_init__(self) -> None:
        if (
            not self.soft_num_allowed_rows_override
            and not self.hard_num_allowed_rows_override
        ):
            raise ValueError(
                "soft_num_allowed_rows_override and hard_num_allowed_rows_override not set. "
                "One of the these variable should be set."
            )


@attr.s(frozen=True)
class ValidationRegionConfig:
    """Region-specific validation configuration information."""

    region_code: str = attr.ib()

    # If a region is in dev_mode then the validations will run but failures will not be
    # raised as errors (e.g. in opencensus alerts or the admin panel summary).
    dev_mode: bool = attr.ib()

    # Information about validations that should not run for this region, indexed by validation_name.
    exclusions: Dict[str, ValidationExclusion] = attr.ib()

    # Information about max_allowed_error overrides, indexed by validation_name.
    max_allowed_error_overrides: Dict[
        str, ValidationMaxAllowedErrorOverride
    ] = attr.ib()

    num_allowed_rows_overrides: Dict[str, ValidationNumAllowedRowsOverride] = attr.ib()

    @classmethod
    def from_yaml(cls, yaml_path: str) -> "ValidationRegionConfig":
        """Parses a region validation config file at the given path into a ValidationRegionConfig object."""

        file_contents = YAMLDict.from_path(yaml_path)
        region_code = file_contents.pop("region_code", str)
        dev_mode = file_contents.pop_optional("dev_mode", bool) or False
        exclusions = {}
        for exclusion_dict in file_contents.pop_dicts("exclusions"):
            validation_name = exclusion_dict.pop("validation_name", str)

            if validation_name in exclusions:
                raise ValueError(
                    f"Found multiple exclusions defined for the same validation: [{validation_name}]"
                )

            exclusions[validation_name] = ValidationExclusion(
                region_code=region_code,
                validation_name=validation_name,
                exclusion_type=ValidationExclusionType(
                    exclusion_dict.pop("exclusion_type", str)
                ),
                exclusion_reason=exclusion_dict.pop("exclusion_reason", str),
            )
        max_allowed_error_overrides = {}
        for max_allowed_error_override_dict in file_contents.pop_dicts(
            "max_allowed_error_overrides"
        ):
            validation_name = max_allowed_error_override_dict.pop(
                "validation_name", str
            )

            if validation_name in max_allowed_error_overrides:
                raise ValueError(
                    f"Found multiple error overrides defined for the same validation: [{validation_name}]"
                )

            max_allowed_error_overrides[
                validation_name
            ] = ValidationMaxAllowedErrorOverride(
                region_code=region_code,
                validation_name=validation_name,
                hard_max_allowed_error_override=max_allowed_error_override_dict.pop_optional(
                    "hard_max_allowed_error_override", float
                ),
                soft_max_allowed_error_override=max_allowed_error_override_dict.pop_optional(
                    "soft_max_allowed_error_override", float
                ),
                override_reason=max_allowed_error_override_dict.pop(
                    "override_reason", str
                ),
            )

        num_allowed_rows_overrides = {}
        for num_allowed_rows_override_dict in file_contents.pop_dicts(
            "num_allowed_rows_overrides"
        ):
            validation_name = num_allowed_rows_override_dict.pop("validation_name", str)

            if validation_name in num_allowed_rows_overrides:
                raise ValueError(
                    f"Found multiple num row overrides defined for the same validation: [{validation_name}]"
                )

            num_allowed_rows_overrides[
                validation_name
            ] = ValidationNumAllowedRowsOverride(
                region_code=region_code,
                validation_name=validation_name,
                hard_num_allowed_rows_override=num_allowed_rows_override_dict.pop_optional(
                    "hard_num_allowed_rows_override", int
                ),
                soft_num_allowed_rows_override=num_allowed_rows_override_dict.pop_optional(
                    "soft_num_allowed_rows_override", int
                ),
                override_reason=num_allowed_rows_override_dict.pop(
                    "override_reason", str
                ),
            )

        return ValidationRegionConfig(
            region_code=region_code,
            dev_mode=dev_mode,
            exclusions=exclusions,
            max_allowed_error_overrides=max_allowed_error_overrides,
            num_allowed_rows_overrides=num_allowed_rows_overrides,
        )


@attr.s(frozen=True)
class ValidationGlobalDisable:
    """Configuration indicating we should disable a specific validation entirely for all regions."""

    # Name of the validation that should be disabled globally
    validation_name: str = attr.ib()

    # A string description reason for why the validation is being disabled. Should include a linked issue.
    disable_reason: str = attr.ib()


@attr.s(frozen=True)
class ValidationGlobalConfig:
    """Global validation configuration information."""

    disabled: Dict[str, ValidationGlobalDisable] = attr.ib()

    @classmethod
    def from_yaml(cls, yaml_path: str) -> "ValidationGlobalConfig":
        with open(yaml_path, "r", encoding="utf-8") as f:
            file_contents = yaml.full_load(f)
            disable_list = file_contents["disabled"]

            disabled_configs = {}
            for disable_dict in disable_list:
                validation_name = disable_dict["validation_name"]
                disabled_configs[validation_name] = ValidationGlobalDisable(
                    validation_name=validation_name,
                    disable_reason=disable_dict["disable_reason"],
                )
            return ValidationGlobalConfig(disabled=disabled_configs)
