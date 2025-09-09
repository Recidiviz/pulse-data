# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for the stable_historical_raw_data_counts_validation_config.py."""
import datetime
import json
import unittest

import yaml
from jsonschema.validators import validate

from recidiviz.common.constants.states import StateCode
from recidiviz.common.local_file_paths import filepath_relative_to_caller
from recidiviz.ingest.direct.raw_data.validations.stable_historical_raw_data_counts_validation_config import (
    StableHistoricalCountsValidationConfigLoader,
    StableHistoricalRawDataCountsValidationConfig,
)


class TestStableHistoricalRawDataCountsValidationConfig(unittest.TestCase):
    """Tests for the StableHistoricalRawDataCountsValidationConfig class."""

    def setUp(self) -> None:
        self.config_path = filepath_relative_to_caller(
            "stable_counts_validation_test_config.yaml", "configs"
        )

        self.validation_config = StableHistoricalRawDataCountsValidationConfig(
            config_loader=StableHistoricalCountsValidationConfigLoader(
                config_path=self.config_path
            )
        )

    def test_get_custom_percent_change_tolerance(self) -> None:
        # Test custom tolerance
        tolerance = self.validation_config.get_custom_percent_change_tolerance(
            StateCode.US_XX, "ft_custom_config"
        )
        self.assertEqual(tolerance, 0.2)

        # Test default tolerance if no custom tolerance exists
        tolerance_default = self.validation_config.get_custom_percent_change_tolerance(
            StateCode.US_YY, "unknown_tag"
        )
        self.assertEqual(tolerance_default, 0.1)

    def test_get_date_range_exclusions(self) -> None:
        exclusions = self.validation_config.get_date_range_exclusions(
            StateCode.US_XX, "ft_custom_config"
        )
        self.assertEqual(len(exclusions), 1)
        self.assertEqual(
            exclusions[0].datetime_start_inclusive,
            datetime.datetime(2024, 8, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
        )
        self.assertEqual(
            exclusions[0].datetime_end_exclusive,
            datetime.datetime(2024, 8, 15, 0, 0, 0, tzinfo=datetime.timezone.utc),
        )

    def test_get_time_window_lookback_days(self) -> None:
        lookback_days = self.validation_config.get_time_window_lookback_days()
        self.assertEqual(lookback_days, 10000)

    def test_yaml_matches_schema(self) -> None:
        prod_config_path = (
            StableHistoricalRawDataCountsValidationConfig().config_loader.config_path
        )

        spec_path = filepath_relative_to_caller(
            "stable_counts_validation_config_schema.json", "configs"
        )
        with open(spec_path, "r", encoding="utf-8") as f:
            loaded_spec = json.load(f)

        with open(self.config_path, "r", encoding="utf-8") as f:
            test_validation_config = yaml.safe_load(f)

        with open(prod_config_path, "r", encoding="utf-8") as f:
            prod_validation_config = yaml.safe_load(f)

        validate(test_validation_config, loaded_spec)
        validate(prod_validation_config, loaded_spec)
