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
"""Tests for the AssetGenerationClient"""

from unittest import TestCase
from unittest.mock import MagicMock, patch

import responses
from requests import HTTPError

from recidiviz.common.constants.states import StateCode
from recidiviz.outliers.constants import INCARCERATION_STARTS_TECHNICAL_VIOLATION
from recidiviz.reporting.asset_generation.client import AssetGenerationClient
from recidiviz.reporting.context.outliers_supervision_officer_supervisor.fixtures import (
    create_fixture,
    get_metric_fixtures_for_state,
    highlighted_officers_fixture_adverse,
    other_officers_fixture_adverse,
    target_fixture_adverse,
)


class AssetGenerationClientTest(TestCase):
    """Tests for the AssetGenerationClient"""

    def setUp(self) -> None:
        self.client = AssetGenerationClient()

    @patch("recidiviz.reporting.asset_generation.client.fetch_id_token")
    @patch("recidiviz.reporting.asset_generation.client.in_gcp")
    def test_auth(self, in_gcp_mock: MagicMock, fetch_id_token_mock: MagicMock) -> None:
        test_endpoint = "path/to/endpoint"
        test_token = "test-token-value"
        in_gcp_mock.return_value = True
        fetch_id_token_mock.return_value = test_token

        with responses.RequestsMock() as rsps:
            rsps.post(
                f"http://asset-generation-test/generate/{test_endpoint}",
                status=200,
                match=[
                    responses.matchers.header_matcher(
                        {"Authorization": f"Bearer {test_token}"}
                    )
                ],
            )
            self.client._generate(  # pylint: disable=protected-access
                test_endpoint, json={}
            )

    def test_generate_error(self) -> None:
        test_endpoint = "path/to/endpoint"
        with responses.RequestsMock() as rsps:
            rsps.post(
                f"http://asset-generation-test/generate/{test_endpoint}",
                status=400,
            )

            with self.assertRaises(HTTPError):
                self.client._generate(  # pylint: disable=protected-access
                    test_endpoint, json={}
                )

    def test_outliers_supervisor_chart(self) -> None:
        state_code = StateCode.US_XX
        test_metric = create_fixture(
            get_metric_fixtures_for_state(state_code)[
                INCARCERATION_STARTS_TECHNICAL_VIOLATION
            ],
            target_fixture_adverse,
            other_officers_fixture_adverse,
            highlighted_officers_fixture_adverse,
        )

        test_asset_url = "/path/to/asset"

        with responses.RequestsMock() as rsps:
            rsps.post(
                "http://asset-generation-test/generate/outliers-supervisor-chart",
                status=200,
                json={"url": test_asset_url},
                match=[
                    responses.matchers.json_params_matcher(
                        {
                            "stateCode": "US_XX",
                            "width": 400,
                            "id": "test-asset-id",
                            "data": {
                                "target": 0.05428241659992843,
                                "otherOfficers": {
                                    "MET": [
                                        0.013664782299427202,
                                        0,
                                        0,
                                        0.01986070301447383,
                                        0.023395936157938592,
                                    ],
                                    "NEAR": [
                                        0.05557247259439707,
                                        0.06803989188181564,
                                        0.0880180859080633,
                                    ],
                                    "FAR": [
                                        0.24142872891632675,
                                        0.2114256751864456,
                                        0.10346978115432588,
                                    ],
                                },
                                "highlightedOfficers": [
                                    {
                                        "externalId": "jsc",
                                        "name": "Jeanette Schneider-Cox",
                                        "rate": 0.19904024430145054,
                                        "targetStatus": "FAR",
                                        "prevRate": 0.15804024430145053,
                                        "supervisorExternalId": "abc123",
                                        "supervisorExternalIds": ["abc123"],
                                        "prevTargetStatus": None,
                                        "supervisionDistrict": "1",
                                    },
                                    {
                                        "externalId": "mm",
                                        "name": "Mario Mccarthy",
                                        "rate": 0.10228673915480327,
                                        "targetStatus": "FAR",
                                        "prevRate": 0.08228673915480327,
                                        "supervisorExternalId": "abc123",
                                        "supervisorExternalIds": ["abc123"],
                                        "prevTargetStatus": None,
                                        "supervisionDistrict": "1",
                                    },
                                    {
                                        "externalId": "rl",
                                        "name": "Ryan Luna",
                                        "rate": 0.129823,
                                        "targetStatus": "FAR",
                                        "prevRate": 0.121354,
                                        "supervisorExternalId": "abc123",
                                        "supervisorExternalIds": ["abc123"],
                                        "prevTargetStatus": None,
                                        "supervisionDistrict": "1",
                                    },
                                ],
                            },
                        }
                    )
                ],
            )

            response = self.client.generate_outliers_supervisor_chart(
                state_code=state_code,
                asset_id="test-asset-id",
                width=400,
                data=test_metric,
            )
        self.assertEqual(response.url, f"http://asset-generation-test{test_asset_url}")
