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
"""Tests the US_ME `CLIENT` view logic."""
from typing import Optional, Tuple
from unittest.mock import Mock, patch

import pandas as pd
from more_itertools import one
from pandas._testing import assert_frame_equal

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.controllers.direct_ingest_view_collector import (
    DirectIngestPreProcessedIngestViewCollector,
)
from recidiviz.ingest.direct.query_utils import get_region_raw_file_config
from recidiviz.tests.big_query.view_test_util import BaseViewTest
from recidiviz.utils.regions import get_region

STATE_CODE = StateCode.US_ME.value


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="t"))
class ClientTest(BaseViewTest):
    """Tests the US_ME `CLIENT` view query functionality."""

    def setUp(self) -> None:
        super().setUp()
        view_builders = DirectIngestPreProcessedIngestViewCollector(
            get_region(STATE_CODE, is_direct_ingest=True), []
        ).collect_view_builders()
        self.view_builder = one(
            view for view in view_builders if view.file_tag == "CLIENT"
        )
        # All columns need to be in lowercase.
        self.expected_result_columns = [
            "client_id",
            "first_name",
            "middle_name",
            "last_name",
            "birth_date",
            "gender",
            "ethnicity",
            "race",
        ]

        self.clients = [
            self._build_client(
                first_name="FIRST",
                middle_name="MIDDLE",
                last_name="LAST",
                birth_date="6/18/1980 12:00:00 AM",
                race_code="1",
                ethnicity_code="187",
                gender_code="1",
                client_id="12345678",
            ),
            # Test that invalid Middle_Name is ignored
            self._build_client(
                first_name="FIRST",
                middle_name="(cd-01-03)",
                last_name="LAST",
                birth_date="6/18/1990 12:00:00 AM",
                race_code="1",
                ethnicity_code="188",
                gender_code="1",
                client_id="987654321",
            ),
            # Test that Middle_Name is formatted
            self._build_client(
                first_name="FIRST",
                middle_name="(Juliette)",
                last_name="LAST",
                birth_date="6/18/1990 12:00:00 AM",
                race_code="1",
                ethnicity_code="186",
                gender_code="1",
                client_id="5555",
            ),
            # Test that duplicates are filtered out
            self._build_client(
                first_name="duplicate ID: 4123",
                middle_name="(Juliette)",
                last_name="LAST",
                birth_date="6/18/1990 12:00:00 AM",
                race_code="1",
                ethnicity_code="188",
                gender_code="1",
                client_id="1111",
            ),
            # Test that test accounts are filtered out
            self._build_client(
                first_name="FIRST",
                middle_name="**testing**",
                last_name="LAST",
                birth_date="6/18/1990 12:00:00 AM",
                race_code="1",
                ethnicity_code="186",
                gender_code="1",
                client_id="2222",
            ),
            # Test that duplicates are filtered out
            self._build_client(
                first_name="FIRST",
                middle_name="MIDDLE",
                last_name="^^",
                birth_date="6/18/1990 12:00:00 AM",
                race_code="1",
                ethnicity_code="186",
                gender_code="1",
                client_id="3333",
            ),
        ]

    def test_client_query(self) -> None:
        raw_file_configs = get_region_raw_file_config(STATE_CODE).raw_file_configs

        self.create_mock_raw_file(
            region_code=STATE_CODE,
            file_config=raw_file_configs["CIS_100_CLIENT"],
            mock_data=self.clients,
        )

        expected_output = [
            [
                "12345678",
                "FIRST",
                "MIDDLE",
                "LAST",
                "6/18/1980 12:00:00 AM",
                "1",
                "187",
                "1",
            ],
            # Formats Middle_Name
            [
                "5555",
                "FIRST",
                "Juliette",
                "LAST",
                "6/18/1990 12:00:00 AM",
                "1",
                "186",
                "1",
            ],
            # Removes invalid Middle_Name
            [
                "987654321",
                "FIRST",
                "",
                "LAST",
                "6/18/1990 12:00:00 AM",
                "1",
                "188",
                "1",
            ],
        ]

        results = self.query_raw_data_view_for_builder(
            self.view_builder,
            dimensions=self.expected_result_columns,
        )

        expected = pd.DataFrame(expected_output, columns=self.expected_result_columns)
        expected = expected.set_index(self.expected_result_columns)
        print(expected)
        print(results)
        assert_frame_equal(expected, results)

    def _build_client(
        self,
        client_id: str,
        first_name: str,
        middle_name: str,
        last_name: str,
        birth_date: str,
        ethnicity_code: str,
        gender_code: str,
        race_code: str,
    ) -> Tuple[Optional[str], ...]:
        return tuple(
            [
                "",  # Adult_Ind
                birth_date,  # Birth_Date
            ]
            + [""] * 3
            + [race_code]  # Cis_1006_Race_Cd
            + [""] * 5
            + [ethnicity_code]  # Cis_1016_Hispanic_Cd
            + [""] * 7
            + [
                gender_code,  # Cis_9012_Gender_Cd
                "",
                "",
                client_id,  # Client_ID
            ]
            + [""] * 15
            + [
                first_name,  # First_Name
            ]
            + [""] * 8
            + [
                last_name,  # Last_Name
            ]
            + [""] * 9
            + [
                middle_name,  # Middle_Name
            ]
            + [""] * 38
        )
