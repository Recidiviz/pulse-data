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


from typing import Any, cast

from recidiviz.auth.helpers import generate_pseudonymized_id, merge_permissions
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import schema_field_for_type
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    get_pseudonymized_id_query_str,
    merge_permissions_query_str,
)
from recidiviz.tests.auth.helpers import convert_list_values_to_json
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)


class BQHelpersTest(BigQueryEmulatorTestCase):
    """BQ tests for auth/helpers.py"""

    dataset_id = "foo"
    address = BigQueryAddress(dataset_id=dataset_id, table_id="foo")

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

    active_date1 = "2024-04-30T14:45:09.865Z"
    active_date2 = "2024-05-03T15:37:07.119Z"

    permissions_single_role_test_data = [
        {
            "email_address": "leadership@testdomain.com",
            "routes": [{"A": True, "B": False}],
            "feature_variants": [
                {"feature1": {}, "feature2": {"activeDate": f"{active_date1}"}}
            ],
        },
    ]

    permissions_multiple_roles_no_conflict_test_data = [
        {
            "email_address": "leadership@testdomain.com",
            "routes": [{"A": True, "B": False}, {"C": True}],
            "feature_variants": [{"feature1": False}, {"feature2": {}}],
        }
    ]

    permissions_multiple_roles_with_conflicts_test_data = [
        {
            "email_address": "leadership@testdomain.com",
            "routes": [{"A": True, "B": False}, {"A": False, "C": True}],
            "feature_variants": [
                {
                    "feature1": False,
                    "feature2": {"activeDate": f"{active_date1}"},
                    "feature3": False,
                    "feature4": {"activeDate": f"{active_date2}"},
                    "feature5": {},
                    "feature6": {"activeDate": f"{active_date1}"},
                },
                {
                    "feature1": {},
                    "feature2": {},
                    "feature3": {"activeDate": f"{active_date2}"},
                    "feature4": {"activeDate": f"{active_date1}"},
                    "feature5": {"activeDate": f"{active_date2}"},
                    "feature6": False,
                },
            ],
        }
    ]

    permissions_bq_query = f"""
                WITH 
                    {merge_permissions_query_str("routes", address.to_str())},
                    {merge_permissions_query_str("feature_variants", address.to_str())}
                SELECT
                    merged_routes.routes AS default_routes,
                    merged_feature_variants.feature_variants AS default_feature_variants
                FROM
                    `{address.to_str()}`
                FULL OUTER JOIN
                    merged_routes
                USING(email_address)
                FULL OUTER JOIN
                    merged_feature_variants
                USING(email_address)
            """

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

        self.create_mock_table(
            address=self.address,
            schema=[
                schema_field_for_type("state_code", str),
                schema_field_for_type("external_id", str),
            ],
        )

        self.load_rows_into_table(self.address, self.pseudonymized_id_test_data)

        query = f"SELECT {get_pseudonymized_id_query_str('state_code || external_id')} AS pseudo_id FROM `{self.address.to_str()}`"
        self.run_query_test(query_str=query, expected_result=generated)

    def test_merge_permissions_single_role(self) -> None:
        processed_permissions = [
            {
                "routes": merge_permissions(
                    cast(list[dict[str, Any]], data["routes"])
                ),  # cast to prevent mypy error
                "feature_variants": merge_permissions(
                    cast(list[dict[str, Any]], data["feature_variants"])
                ),
            }
            for data in self.permissions_single_role_test_data
        ]

        expected = [
            {
                "routes": {"A": True, "B": False},
                "feature_variants": {
                    "feature1": {},
                    "feature2": {"activeDate": f"{self.active_date1}"},
                },
            }
        ]

        self.assertEqual(expected, processed_permissions)

    def test_merge_permissions_single_role_matches_bq_emulator(self) -> None:
        generated = convert_list_values_to_json(
            [
                {
                    "default_routes": merge_permissions(
                        cast(list[dict[str, Any]], data["routes"])
                    ),  # cast to prevent mypy error
                    "default_feature_variants": merge_permissions(
                        cast(list[dict[str, Any]], data["feature_variants"])
                    ),
                }
                for data in self.permissions_single_role_test_data
            ]
        )

        self.create_mock_table(
            address=self.address,
            schema=[
                schema_field_for_type("email_address", str),
                schema_field_for_type("routes", list),
                schema_field_for_type("feature_variants", list),
            ],
        )

        self.load_rows_into_table(
            self.address,
            convert_list_values_to_json(self.permissions_single_role_test_data),
        )

        query_builder = SimpleBigQueryViewBuilder(
            dataset_id=self.dataset_id,
            view_id="test_merge_permissions_single_role",
            description="View to test permissions merging for a user with a single role",
            view_query_template=self.permissions_bq_query,
        )
        self.run_query_test(
            query_str=query_builder.build().view_query, expected_result=generated
        )

    def test_merge_permissions_multiple_roles_no_conflict(self) -> None:
        processed_permissions = [
            {
                "routes": merge_permissions(
                    cast(list[dict[str, Any]], data["routes"])
                ),  # cast to prevent mypy error
                "feature_variants": merge_permissions(
                    cast(list[dict[str, Any]], data["feature_variants"])
                ),
            }
            for data in self.permissions_multiple_roles_no_conflict_test_data
        ]

        expected = [
            {
                "routes": {"A": True, "B": False, "C": True},
                "feature_variants": {"feature1": False, "feature2": {}},
            }
        ]

        self.assertEqual(expected, processed_permissions)

    def test_merge_permissions_multiple_roles_no_conflict_matches_bq_emulator(
        self,
    ) -> None:
        generated = convert_list_values_to_json(
            [
                {
                    "default_routes": merge_permissions(
                        cast(list[dict[str, Any]], data["routes"])
                    ),  # cast to prevent mypy error
                    "default_feature_variants": merge_permissions(
                        cast(list[dict[str, Any]], data["feature_variants"])
                    ),
                }
                for data in self.permissions_multiple_roles_no_conflict_test_data
            ]
        )

        self.create_mock_table(
            address=self.address,
            schema=[
                schema_field_for_type("email_address", str),
                schema_field_for_type("routes", list),
                schema_field_for_type("feature_variants", list),
            ],
        )

        self.load_rows_into_table(
            self.address,
            convert_list_values_to_json(
                self.permissions_multiple_roles_no_conflict_test_data
            ),
        )

        query_builder = SimpleBigQueryViewBuilder(
            dataset_id=self.dataset_id,
            view_id="test_merge_permissions_multiple_roles_no_conflict",
            description="View to test permissions merging for a user with multiple roles that don't conflict",
            view_query_template=self.permissions_bq_query,
        )
        self.run_query_test(
            query_str=query_builder.build().view_query, expected_result=generated
        )

    def test_merge_permissions_multiple_roles_with_conflicts(self) -> None:
        processed_permissions = [
            {
                "routes": merge_permissions(
                    cast(list[dict[str, Any]], data["routes"])
                ),  # cast to prevent mypy error
                "feature_variants": merge_permissions(
                    cast(list[dict[str, Any]], data["feature_variants"])
                ),
            }
            for data in self.permissions_multiple_roles_with_conflicts_test_data
        ]

        expected = [
            {
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
        ]

        self.assertEqual(expected, processed_permissions)

    def test_merge_permissions_multiple_roles_with_conflicts_matches_bq_emulator(
        self,
    ) -> None:
        generated = convert_list_values_to_json(
            [
                {
                    "default_routes": merge_permissions(
                        cast(list[dict[str, Any]], data["routes"])
                    ),  # cast to prevent mypy error
                    "default_feature_variants": merge_permissions(
                        cast(list[dict[str, Any]], data["feature_variants"])
                    ),
                }
                for data in self.permissions_multiple_roles_with_conflicts_test_data
            ]
        )

        self.create_mock_table(
            address=self.address,
            schema=[
                schema_field_for_type("email_address", str),
                schema_field_for_type("routes", list),
                schema_field_for_type("feature_variants", list),
            ],
        )

        self.load_rows_into_table(
            self.address,
            convert_list_values_to_json(
                self.permissions_multiple_roles_with_conflicts_test_data
            ),
        )

        query_builder = SimpleBigQueryViewBuilder(
            dataset_id=self.dataset_id,
            view_id="test_merge_permissions_multiple_roles_with_conflicts",
            description="View to test permissions merging for a user with multiple roles that do conflict",
            view_query_template=self.permissions_bq_query,
        )
        self.run_query_test(
            query_str=query_builder.build().view_query, expected_result=generated
        )
