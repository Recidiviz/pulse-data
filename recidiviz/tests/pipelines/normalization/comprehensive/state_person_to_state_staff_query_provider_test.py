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
"""Tests for state_person_to_state_staff_query-provider.py"""
import unittest

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.pipelines.normalization.comprehensive.state_person_to_state_staff_query_provider import (
    get_state_person_to_state_staff_query_provider,
)


class TestStatePersonToStateStaffQueryProvider(unittest.TestCase):
    """Tests for state_person_to_state_staff_query-provider.py"""

    def test_get_state_person_to_state_staff_query_provider(self) -> None:
        query_provider = get_state_person_to_state_staff_query_provider(
            project_id="recidiviz-456",
            state_code=StateCode.US_XX,
            address_overrides=None,
        )

        expected_parent_tables = {
            BigQueryAddress(dataset_id="us_xx_state", table_id="state_assessment"),
            BigQueryAddress(
                dataset_id="us_xx_state", table_id="state_program_assignment"
            ),
            BigQueryAddress(
                dataset_id="us_xx_state", table_id="state_staff_external_id"
            ),
            BigQueryAddress(
                dataset_id="us_xx_state", table_id="state_supervision_contact"
            ),
            BigQueryAddress(
                dataset_id="us_xx_state", table_id="state_supervision_period"
            ),
            BigQueryAddress(
                dataset_id="us_xx_state",
                table_id="state_supervision_violation_response",
            ),
        }
        self.assertEqual(expected_parent_tables, query_provider.parent_tables)

    def test_get_state_person_to_state_staff_query_provider_dataset_overrides(
        self,
    ) -> None:
        address_overrides = (
            BigQueryAddressOverrides.Builder(sandbox_prefix="my_prefix")
            .register_sandbox_override_for_entire_dataset("us_xx_state")
            .build()
        )
        query_provider = get_state_person_to_state_staff_query_provider(
            project_id="recidiviz-456",
            state_code=StateCode.US_XX,
            address_overrides=address_overrides,
        )

        expected_parent_tables = {
            BigQueryAddress(
                dataset_id="my_prefix_us_xx_state", table_id="state_assessment"
            ),
            BigQueryAddress(
                dataset_id="my_prefix_us_xx_state", table_id="state_program_assignment"
            ),
            BigQueryAddress(
                dataset_id="my_prefix_us_xx_state", table_id="state_staff_external_id"
            ),
            BigQueryAddress(
                dataset_id="my_prefix_us_xx_state", table_id="state_supervision_contact"
            ),
            BigQueryAddress(
                dataset_id="my_prefix_us_xx_state", table_id="state_supervision_period"
            ),
            BigQueryAddress(
                dataset_id="my_prefix_us_xx_state",
                table_id="state_supervision_violation_response",
            ),
        }
        self.assertEqual(expected_parent_tables, query_provider.parent_tables)

    def test_get_us_ix_case_update_info_query_provider_specific_address_overrides(
        self,
    ) -> None:
        address_overrides = (
            BigQueryAddressOverrides.Builder(sandbox_prefix="my_prefix")
            .register_sandbox_override_for_address(
                BigQueryAddress(
                    dataset_id="us_xx_state",
                    table_id="state_supervision_violation_response",
                )
            )
            .build()
        )
        query_provider = get_state_person_to_state_staff_query_provider(
            project_id="recidiviz-456",
            state_code=StateCode.US_XX,
            address_overrides=address_overrides,
        )

        expected_parent_tables = {
            BigQueryAddress(dataset_id="us_xx_state", table_id="state_assessment"),
            BigQueryAddress(
                dataset_id="us_xx_state", table_id="state_program_assignment"
            ),
            BigQueryAddress(
                dataset_id="us_xx_state", table_id="state_staff_external_id"
            ),
            BigQueryAddress(
                dataset_id="us_xx_state", table_id="state_supervision_contact"
            ),
            BigQueryAddress(
                dataset_id="us_xx_state", table_id="state_supervision_period"
            ),
            # Only this one has an override applied
            BigQueryAddress(
                dataset_id="my_prefix_us_xx_state",
                table_id="state_supervision_violation_response",
            ),
        }
        self.assertEqual(expected_parent_tables, query_provider.parent_tables)
