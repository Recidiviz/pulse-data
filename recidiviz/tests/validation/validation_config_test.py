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
"""Tests for classes in validation_config.py."""
import unittest

from recidiviz.tests.ingest import fixtures
from recidiviz.validation.configured_validations import get_validation_global_config
from recidiviz.validation.validation_config import (
    ValidationExclusion,
    ValidationExclusionType,
    ValidationMaxAllowedErrorOverride,
    ValidationNumAllowedRowsOverride,
    ValidationRegionConfig,
)


class ValidationConfigTest(unittest.TestCase):
    """Tests for classes in validation_config.py."""

    def test_parse_empty_config(self) -> None:
        # Arrange
        yaml_path = fixtures.as_filepath("us_xx_validation_config_empty.yaml")

        # Act
        config = ValidationRegionConfig.from_yaml(yaml_path)

        # Assert
        self.assertEqual("US_XX", config.region_code)
        self.assertEqual({}, config.exclusions)
        self.assertEqual({}, config.max_allowed_error_overrides)

    def test_parse_complex_config(self) -> None:
        # Arrange
        yaml_path = fixtures.as_filepath("us_xx_validation_config_complex.yaml")

        # Act
        config = ValidationRegionConfig.from_yaml(yaml_path)

        # Assert
        expected_exclusions = {
            "my_view": ValidationExclusion(
                region_code="US_XX",
                validation_name="my_view",
                exclusion_type=ValidationExclusionType.BROKEN,
                exclusion_reason="This needs fixing",
            ),
            "my_other_view": ValidationExclusion(
                region_code="US_XX",
                validation_name="my_other_view",
                exclusion_type=ValidationExclusionType.DOES_NOT_APPLY,
                exclusion_reason="We do not have this data",
            ),
        }

        expected_max_allowed_error_overrides = {
            "sameness_view": ValidationMaxAllowedErrorOverride(
                region_code="US_XX",
                validation_name="sameness_view",
                hard_max_allowed_error_override=0.3,
                soft_max_allowed_error_override=0.3,
                override_reason="This is hard to get right",
            )
        }

        expected_num_allowed_rows_overrides = {
            "existence_view": ValidationNumAllowedRowsOverride(
                region_code="US_XX",
                validation_name="existence_view",
                hard_num_allowed_rows_override=10,
                soft_num_allowed_rows_override=10,
                override_reason="These should not exist. TODO(#0000) - fix it.",
            )
        }
        expected_config = ValidationRegionConfig(
            region_code="US_XX",
            dev_mode=False,
            exclusions=expected_exclusions,
            max_allowed_error_overrides=expected_max_allowed_error_overrides,
            num_allowed_rows_overrides=expected_num_allowed_rows_overrides,
        )

        self.assertEqual(expected_config, config)

    def test_parse_view_name_reused_exclusion(self) -> None:
        # Arrange
        yaml_path = fixtures.as_filepath(
            "us_xx_validation_config_reused_exclusion.yaml"
        )

        # Act
        with self.assertRaisesRegex(
            ValueError,
            r"^Found multiple exclusions defined for the same validation: \[my_view\]$",
        ):
            _ = ValidationRegionConfig.from_yaml(yaml_path)

    def test_parse_view_name_reused_overrides(self) -> None:
        # Arrange
        yaml_path = fixtures.as_filepath(
            "us_xx_validation_config_reused_overrides.yaml"
        )

        # Act
        with self.assertRaisesRegex(
            ValueError,
            r"^Found multiple error overrides defined for the same validation: \[my_view\]$",
        ):
            _ = ValidationRegionConfig.from_yaml(yaml_path)

    def test_parse_bad_exclusion_type(self) -> None:
        # Arrange
        yaml_path = fixtures.as_filepath(
            "us_xx_validation_config_bad_exclusion_type.yaml"
        )

        # Act
        with self.assertRaisesRegex(
            ValueError, r"^'NOT_A_VALID_TYPE' is not a valid ValidationExclusionType$"
        ):
            _ = ValidationRegionConfig.from_yaml(yaml_path)

    def test_parse_override_is_not_float(self) -> None:
        # Arrange
        yaml_path = fixtures.as_filepath(
            "us_xx_validation_config_override_is_not_float.yaml"
        )

        # Act
        with self.assertRaisesRegex(
            ValueError,
            r"^The field \[hard_max_allowed_error_override\] must be of type \[<class 'float'>\]. "
            r"Invalid \[hard_max_allowed_error_override\] value, expected type "
            r"\[<class 'float'>\] but received: <class 'str'>$",
        ):
            _ = ValidationRegionConfig.from_yaml(yaml_path)

    def test_parse_global_config_parses(self) -> None:
        # Test passes if this parses
        self.assertIsNotNone(get_validation_global_config())
