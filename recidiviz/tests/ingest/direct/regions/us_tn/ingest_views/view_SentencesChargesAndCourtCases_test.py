#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2021 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Tests the TN `SentencesChargesAndCourtCases` view logic."""

from mock import Mock, patch

from recidiviz.common.constants.states import StateCode
from recidiviz.tests.big_query.view_test_util import BaseViewTest


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="t"))
class SentencesChargesAndCourtCasesTest(BaseViewTest):
    """Tests the TN `SentencesChargesAndCourtCase` query functionality."""

    def setUp(self) -> None:
        super().setUp()
        self.region_code = StateCode.US_TN.value
        self.view_builder = self.view_builder_for_tag(
            self.region_code, "SentencesChargesAndCourtCases"
        )
        self.data_types = str

    def test_sentences_charges_courtcases_simple(self) -> None:
        self.run_ingest_view_test(
            fixtures_files_name="sentences_charges_courtcases_simple.csv"
        )

    # TODO(#11145): Investigate differences in MostRecentSentenceAction within .0001 seconds of each other

    def test_sentences_charges_courtcases_with_quotes_in_conditions(self) -> None:
        self.run_ingest_view_test(
            fixtures_files_name="sentences_charges_courtcases_with_quotes_in_conditions.csv"
        )

    def test_sentences_charges_courtcases_with_conditions_over_multiple_dates(
        self,
    ) -> None:
        self.run_ingest_view_test(
            fixtures_files_name="sentences_charges_courtcases_with_conditions_over_multiple_dates.csv"
        )
