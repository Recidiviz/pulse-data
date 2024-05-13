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
"""Tests for stable_historical_raw_data_counts_validation.py"""
from typing import Dict
from unittest import TestCase
from unittest.mock import MagicMock, patch

from jsonschema.exceptions import ValidationError
from parameterized import parameterized

from recidiviz.validation.views.state.raw_data import (
    stable_historical_raw_data_counts_validation,
)
from recidiviz.validation.views.state.raw_data.stable_historical_raw_data_counts_validation import (
    _load_stable_historical_raw_data_counts_validation_config,
)

STABLE_RAW_DATA_PACKAGE = stable_historical_raw_data_counts_validation.__name__


class StableHistoricalRawDataCountsValidationExclusion(TestCase):
    """Tests for the validation exclusion pattern in
    stable_historical_raw_data_counts_validation.py
    """

    def test_current_config_is_valid(self) -> None:
        _ = _load_stable_historical_raw_data_counts_validation_config()

    @parameterized.expand(
        [
            (
                {"PERMANENT": {"US_OZ": [{"file_tag": "123"}]}},
                r"'exclusion_reason' is a required property",
            ),
            (
                {
                    "PERMANENT": {
                        "US_oZ": [{"file_tag": "123", "exclusion_reason": "456"}]
                    }
                },
                r"'US_oZ' does not match any of the regexes: '\^US_\[A-Z\]\{2\}\$'",
            ),
            (
                {
                    "PERMANENT": {
                        "US_OZ": [{"file_tag": 123, "exclusion_reason": "456"}]
                    }
                },
                r"123 is not of type 'string'",
            ),
            (
                {
                    "DATE_RANGE": {
                        "US_OZ": [
                            {
                                "file_tag": "123",
                                "exclusion_reason": "456",
                                "datetime_start_inclusive": "2022-11-17T00:00:00Z",
                                "datetime_end_exclusive": "2022-11-17T00:00:000Z",
                            }
                        ]
                    }
                },
                r"'2022-11-17T00:00:000Z' does not match '.*'",
            ),
            (
                {
                    "DATE_RANGE": {
                        "US_OZ": [
                            {
                                "file_tag": "123",
                                "exclusion_reason": "456",
                                "datetime_start_inclusive": "2022-11-17T00:00:00ZZ",
                                "datetime_end_exclusive": "2022-11-17T00:00:00Z",
                            }
                        ]
                    }
                },
                r"'2022-11-17T00:00:00ZZ' does not match '.*'",
            ),
        ]
    )
    @patch(f"{STABLE_RAW_DATA_PACKAGE}.yaml")
    def test_spec_is_not_what_we_want_it_to_be(
        self,
        mock_return: Dict,
        validation_regex: str,
        yaml_mock: MagicMock,
    ) -> None:
        _load_stable_historical_raw_data_counts_validation_config.cache_clear()
        yaml_mock.safe_load.return_value = mock_return
        with self.assertRaisesRegex(ValidationError, validation_regex):
            _load_stable_historical_raw_data_counts_validation_config()
