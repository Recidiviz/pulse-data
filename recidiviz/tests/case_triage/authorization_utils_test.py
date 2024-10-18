# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Implements tests for authorization utils."""

from unittest import TestCase

import freezegun

from recidiviz.case_triage.authorization_utils import get_active_feature_variants

TEST_PSEUDONYMIZED_ID: str = "testestestbsapr749bwrb893b4389test"

INPUT_FVS = {
    "fvOne": {},
    "fvTwo": {"activeDate": "2022-01-01T01:01:01.000Z"},
    "fvThree": {"activeDate": "2022-01-01T01:01:01.000"},
    "fvFour": {"activeDate": "2022-01-01"},
    "fvFive": True,
}
EXPECTED_FVS = {
    "fvOne": {},
    "fvTwo": {"activeDate": "2022-01-01T01:01:01.000Z"},
    "fvThree": {"activeDate": "2022-01-01T01:01:01.000"},
    "fvFour": {"activeDate": "2022-01-01"},
    "fvFive": {},
}


class TestAuthorizationUtils(TestCase):
    """_summary_
    Test authorization utils for case triage.
    """

    @freezegun.freeze_time("2022-12-30")
    def test_feature_variant_parsing_included(self) -> None:

        self.assertDictEqual(
            get_active_feature_variants(INPUT_FVS, TEST_PSEUDONYMIZED_ID), EXPECTED_FVS
        )

    @freezegun.freeze_time("2022-12-30")
    def test_feature_variant_with_future_active_date(self) -> None:
        input_fvs = {**INPUT_FVS, "fvSix": {"activeDate": "2023-01-01T01:01:01.000Z"}}

        self.assertDictEqual(
            get_active_feature_variants(input_fvs, TEST_PSEUDONYMIZED_ID), EXPECTED_FVS
        )

    @freezegun.freeze_time("2022-12-30")
    def test_feature_variant_with_bad_value(self) -> None:
        # Define the additional feature variants as a separate dict
        new_fvs = {"fvZero": 3, "fvSix": "dog"}

        # Merge new_fvs into a copy of INPUT_FVS
        input_fvs = {**INPUT_FVS, **new_fvs}

        # Assert active feature variants match expected ones
        self.assertDictEqual(
            get_active_feature_variants(input_fvs, TEST_PSEUDONYMIZED_ID), EXPECTED_FVS
        )

        # Capture logs and verify content
        with self.assertLogs(level="ERROR") as log:
            get_active_feature_variants(input_fvs, TEST_PSEUDONYMIZED_ID)

        # Loop through new_fvs to check if each one is logged as expected
        for key, value in new_fvs.items():
            self.assertTrue(
                any(
                    all(
                        term in message
                        for term in [TEST_PSEUDONYMIZED_ID, key, str(value)]
                    )
                    for message in log.output
                ),
                f"Expected {key} with value {value} in the log message, but it was not found.",
            )
