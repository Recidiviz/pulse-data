# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests the PA external ids query functionality"""
from mock import Mock, patch

from recidiviz.common.constants.states import StateCode
from recidiviz.tests.big_query.view_test_util import BaseViewTest


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="t"))
class ViewPersonExternalIdsTest(BaseViewTest):
    """Tests the PA external ids query functionality"""

    def setUp(self) -> None:
        super().setUp()
        self.region_code = StateCode.US_PA.value
        self.view_builder = self.view_builder_for_tag(
            self.region_code, "person_external_ids"
        )

    def test_view_person_external_ids_parses(self) -> None:
        self.run_ingest_view_test(fixtures_files_name="person_external_ids_parses.csv")

    def test_view_person_external_ids_simple(self) -> None:
        self.run_ingest_view_test(fixtures_files_name="person_external_ids_simple.csv")

    def test_view_person_external_ids_multiple_inmate(self) -> None:
        self.run_ingest_view_test(
            fixtures_files_name="person_external_ids_multiple_inmate.csv"
        )

    def test_view_person_external_ids_missing_parole(self) -> None:
        self.run_ingest_view_test(
            fixtures_files_name="person_external_ids_missing_parole.csv"
        )

    def test_view_person_external_ids_missing_control(self) -> None:
        self.run_ingest_view_test(
            fixtures_files_name="person_external_ids_missing_control.csv"
        )

    def test_view_person_external_ids_null_inmate_numbers(self) -> None:
        self.run_ingest_view_test(
            fixtures_files_name="person_external_ids_null_inmate_numbers.csv"
        )

    def test_view_person_external_ids_clean_bad_inmate_numbers(self) -> None:
        self.run_ingest_view_test(
            fixtures_files_name="person_external_ids_clean_bad_inmate_numbers.csv"
        )

    def test_view_person_external_ids_complex(self) -> None:
        self.run_ingest_view_test(fixtures_files_name="person_external_ids_complex.csv")

    def test_view_person_external_ids_complex2(self) -> None:
        self.run_ingest_view_test(
            fixtures_files_name="person_external_ids_complex2.csv"
        )

    def test_view_person_external_ids_join_on_parole(self) -> None:
        self.run_ingest_view_test(
            fixtures_files_name="person_external_ids_join_on_parole.csv"
        )

    def test_view_person_external_ids_join_on_parole_2(self) -> None:
        self.run_ingest_view_test(
            fixtures_files_name="person_external_ids_join_on_parole_2.csv"
        )

    def test_view_person_external_ids_join_on_parole_mismatch_casing(self) -> None:
        self.run_ingest_view_test(
            fixtures_files_name="person_external_ids_join_on_parole_mismatch_casing.csv"
        )

    def test_view_person_external_ids_multiple_control_linked_via_parole(
        self,
    ) -> None:
        self.run_ingest_view_test(
            fixtures_files_name="person_external_ids_multiple_control_linked_via_parole.csv"
        )

    def test_view_person_external_ids_link_via_pseudo_id(self) -> None:
        self.run_ingest_view_test(
            fixtures_files_name="person_external_ids_link_via_pseudo_id.csv"
        )

    def test_view_person_external_ids_link_via_pseudo_id_one_parole_link(
        self,
    ) -> None:
        self.run_ingest_view_test(
            fixtures_files_name="person_external_ids_link_via_pseudo_id_one_parole_link.csv"
        )
