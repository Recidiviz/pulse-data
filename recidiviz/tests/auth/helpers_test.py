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
"""Tests for auth/helpers.py"""


from typing import Any, Dict, List
from unittest import TestCase

from recidiviz.auth.helpers import generate_pseudonymized_id, merge_permissions
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import schema_field_for_type
from recidiviz.calculator.query.bq_utils import get_pseudonymized_id_query_str
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)


class BQHelpersTest(BigQueryEmulatorTestCase):
    """BQ tests for auth/helpers.py"""

    pseudonymized_id_test_data = [
        {"state_code": "US_XX", "external_id": "12345"},
        {"state_code": "US_XX", "external_id": "abcdef"},
        {"state_code": "US_YY", "external_id": "id6789"},
        {"state_code": "US_YY", "external_id": "a*b-c+1_2,3 4"},
        {"state_code": "US_YY", "external_id": "0"},
        {"state_code": "US_ID", "external_id": "XXXXXXXX"},
        {"state_code": "US_MI", "external_id": "1372ajk23"},
        {"state_code": "US_MI", "external_id": "XYZ"},
        {"state_code": "US_TN", "external_id": "new"},
    ]

    def test_generate_pseudonymized_id(self) -> None:
        # Generated in BQ using:
        # WITH data AS (
        #   SELECT "US_XX" AS state_code, "12345" AS external_id
        #   UNION ALL SELECT "US_XX", "abcdef"
        #   UNION ALL SELECT "US_YY", "id6789"
        #   UNION ALL SELECT "US_YY", "a*b-c+1_2,3 4"
        #   UNION ALL SELECT "US_YY", "0"
        #   UNION ALL SELECT "US_ID", "XXXXXXXX" -- roster fixture
        #   UNION ALL SELECT "US_MI", "1372ajk23" -- roster fixture
        #   UNION ALL SELECT "US_MI", "XYZ" -- override fixture
        #   UNION ALL SELECT "US_TN", "new" --override fixture
        # )
        # SELECT state_code, external_id,
        # SUBSTRING(
        #         # hashing external ID to base64url
        #             REPLACE(
        #                 REPLACE(
        #                     TO_BASE64(SHA256(state_code || external_id)),
        #                     '+',
        #                     '-'
        #                 ),
        #                 '/',
        #                 '_'
        #             ),
        #             1,
        #             16
        #         )
        #         FROM data
        expected = [
            "royo1D-59S9vRQzZ",
            "Shk9sb8xFnKdMpDc",
            "8F-w8kS69g1_3-B7",
            "WoQ4DaiBMSJDS7XS",
            "HjSvWEfPfkeGQGWs",
            "SpIZHrcyUqhpqgFN",
            "j2r_8fkvyBAELcPm",
            "VO8NzIUbaCBJFIPZ",
            "lxUZNDl-N8VEXNQF",
        ]
        actual = [
            generate_pseudonymized_id(data["state_code"], data["external_id"])
            for data in self.pseudonymized_id_test_data
        ]

        self.assertEqual(expected, actual)

    def test_generate_pseudonymized_id_matches_bq_emulator(self) -> None:
        generated = [
            {
                "pseudo_id": generate_pseudonymized_id(
                    data["state_code"], data["external_id"]
                )
            }
            for data in self.pseudonymized_id_test_data
        ]

        address = BigQueryAddress(dataset_id="foo", table_id="foo")
        self.create_mock_table(
            address=address,
            schema=[
                schema_field_for_type("state_code", str),
                schema_field_for_type("external_id", str),
            ],
        )

        self.load_rows_into_table(address, self.pseudonymized_id_test_data)

        query = f"SELECT {get_pseudonymized_id_query_str('state_code || external_id')} AS pseudo_id FROM `{address.to_str()}`"
        self.run_query_test(query_str=query, expected_result=generated)


class HelpersTest(TestCase):
    """Tests for auth/helpers.py"""

    def setUp(self) -> None:
        self.active_date1 = "2024-04-30T14:45:09.865Z"
        self.active_date2 = "2024-05-03T15:37:07.119Z"

    def test_merge_permissions_permissions_single_role(self) -> None:
        permissions: Dict[str, List[Dict[str, Any]]] = {
            "routes": [{"A": True, "B": False}],
            "feature_variants": [
                {"feature1": {}, "feature2": {"activeDate": f"{self.active_date1}"}}
            ],
        }

        processed_permissions = {}
        processed_permissions["routes"] = merge_permissions(permissions["routes"])
        processed_permissions["feature_variants"] = merge_permissions(
            permissions["feature_variants"]
        )

        expected = {
            "routes": {"A": True, "B": False},
            "feature_variants": {
                "feature1": {},
                "feature2": {"activeDate": f"{self.active_date1}"},
            },
        }

        self.assertEqual(expected, processed_permissions)

    def test_merge_permissions_multiple_roles_no_conflict(self) -> None:
        permissions: Dict[str, List[Dict[str, Any]]] = {
            "routes": [{"A": True, "B": False}, {"C": True}],
            "feature_variants": [{"feature1": False}, {"feature2": {}}],
        }

        processed_permissions = {}
        processed_permissions["routes"] = merge_permissions(permissions["routes"])
        processed_permissions["feature_variants"] = merge_permissions(
            permissions["feature_variants"]
        )

        expected = {
            "routes": {"A": True, "B": False, "C": True},
            "feature_variants": {"feature1": False, "feature2": {}},
        }

        self.assertEqual(expected, processed_permissions)

    def test_merge_permissions_multiple_roles_with_conflicts(self) -> None:

        permissions: Dict[str, List[Dict[str, Any]]] = {
            "routes": [{"A": True, "B": False}, {"A": False, "C": True}],
            "feature_variants": [
                {
                    "feature1": False,
                    "feature2": {"activeDate": f"{self.active_date1}"},
                    "feature3": False,
                    "feature4": {"activeDate": f"{self.active_date2}"},
                    "feature5": {},
                    "feature6": {"activeDate": f"{self.active_date1}"},
                },
                {
                    "feature1": {},
                    "feature2": {},
                    "feature3": {"activeDate": f"{self.active_date2}"},
                    "feature4": {"activeDate": f"{self.active_date1}"},
                    "feature5": {"activeDate": f"{self.active_date2}"},
                    "feature6": False,
                },
            ],
        }

        processed_permissions = {}
        processed_permissions["routes"] = merge_permissions(permissions["routes"])
        processed_permissions["feature_variants"] = merge_permissions(
            permissions["feature_variants"]
        )

        expected = {
            "routes": {"A": True, "B": False, "C": True},
            "feature_variants": {
                "feature1": {},
                "feature2": {},
                "feature3": {"activeDate": f"{self.active_date2}"},
                "feature4": {"activeDate": f"{self.active_date1}"},
                "feature5": {},
                "feature6": {"activeDate": f"{self.active_date1}"},
            },
        }

        self.assertEqual(expected, processed_permissions)
