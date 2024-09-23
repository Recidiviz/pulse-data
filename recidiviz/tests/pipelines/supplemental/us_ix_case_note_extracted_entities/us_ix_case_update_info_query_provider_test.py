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
"""Tests for us_ix_case_update_info_query_provider.py"""
import unittest

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.pipelines.supplemental.us_ix_case_note_extracted_entities.us_ix_case_update_info_query_provider import (
    get_us_ix_case_update_info_query_provider,
)


class TestUsIxCaseUpdateInfoQueryProvider(unittest.TestCase):
    """Tests for us_ix_case_update_info_query_provider.py"""

    def test_get_us_ix_case_update_info_query_provider(self) -> None:
        query_provider = get_us_ix_case_update_info_query_provider(
            project_id="recidiviz-456", address_overrides=None
        )

        expected_parent_tables = {
            BigQueryAddress(
                dataset_id="us_ix_normalized_state",
                table_id="state_person_external_id",
            ),
            BigQueryAddress(
                dataset_id="us_ix_raw_data_up_to_date_views",
                table_id="ind_OffenderNoteInfo_latest",
            ),
            BigQueryAddress(
                dataset_id="us_ix_raw_data_up_to_date_views",
                table_id="ind_OffenderNote_latest",
            ),
            BigQueryAddress(
                dataset_id="us_ix_raw_data_up_to_date_views",
                table_id="ref_Employee_latest",
            ),
        }
        self.assertEqual(expected_parent_tables, query_provider.parent_tables)

    def test_get_us_ix_case_update_info_query_provider_dataset_overrides(self) -> None:
        address_overrides = (
            BigQueryAddressOverrides.Builder(sandbox_prefix="my_prefix")
            .register_sandbox_override_for_entire_dataset(
                "us_ix_raw_data_up_to_date_views"
            )
            .register_sandbox_override_for_entire_dataset("us_ix_normalized_state")
            .build()
        )
        query_provider = get_us_ix_case_update_info_query_provider(
            project_id="recidiviz-456", address_overrides=address_overrides
        )

        expected_parent_tables = {
            BigQueryAddress(
                dataset_id="my_prefix_us_ix_normalized_state",
                table_id="state_person_external_id",
            ),
            BigQueryAddress(
                dataset_id="my_prefix_us_ix_raw_data_up_to_date_views",
                table_id="ind_OffenderNoteInfo_latest",
            ),
            BigQueryAddress(
                dataset_id="my_prefix_us_ix_raw_data_up_to_date_views",
                table_id="ind_OffenderNote_latest",
            ),
            BigQueryAddress(
                dataset_id="my_prefix_us_ix_raw_data_up_to_date_views",
                table_id="ref_Employee_latest",
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
                    dataset_id="us_ix_raw_data_up_to_date_views",
                    table_id="ind_OffenderNoteInfo_latest",
                )
            )
            .build()
        )
        query_provider = get_us_ix_case_update_info_query_provider(
            project_id="recidiviz-456", address_overrides=address_overrides
        )

        expected_parent_tables = {
            BigQueryAddress(
                dataset_id="us_ix_normalized_state",
                table_id="state_person_external_id",
            ),
            # This is the only address that should be impacted
            BigQueryAddress(
                dataset_id="my_prefix_us_ix_raw_data_up_to_date_views",
                table_id="ind_OffenderNoteInfo_latest",
            ),
            BigQueryAddress(
                dataset_id="us_ix_raw_data_up_to_date_views",
                table_id="ind_OffenderNote_latest",
            ),
            BigQueryAddress(
                dataset_id="us_ix_raw_data_up_to_date_views",
                table_id="ref_Employee_latest",
            ),
        }
        self.assertEqual(expected_parent_tables, query_provider.parent_tables)
