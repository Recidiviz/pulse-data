# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for TypesenseBackfillClient."""
import unittest
from unittest.mock import MagicMock, patch

from recidiviz.common.constants.states import StateCode
from recidiviz.workflows.etl.typesense_backfill_client import TypesenseBackfillClient

FUNCTION_URL = "https://typesense-backfill-abc123-uc.a.run.app"


def _response_body(imported: int) -> dict:
    return {"totals": {"imported": imported, "failed": 0, "deleted": 0}}


@patch("recidiviz.workflows.etl.typesense_backfill_client.in_gcp", return_value=True)
class TestTypesenseBackfillClient(unittest.TestCase):
    """Tests the Typesense backfill client's authenticated trigger call."""

    @patch("recidiviz.workflows.etl.typesense_backfill_client.requests.post")
    @patch("recidiviz.workflows.etl.typesense_backfill_client.fetch_id_token")
    @patch("recidiviz.workflows.etl.typesense_backfill_client.get_secret")
    def test_trigger_backfill_posts_with_oidc_token(
        self,
        mock_get_secret: MagicMock,
        mock_fetch_id_token: MagicMock,
        mock_post: MagicMock,
        _mock_in_gcp: MagicMock,
    ) -> None:
        mock_get_secret.return_value = FUNCTION_URL
        mock_fetch_id_token.return_value = "id-token"
        mock_post.return_value.json.return_value = _response_body(imported=10)

        TypesenseBackfillClient().trigger_backfill(
            state_code=StateCode.US_XX, collection="clientCollection"
        )

        mock_fetch_id_token.assert_called_once()
        self.assertEqual(FUNCTION_URL, mock_fetch_id_token.call_args.kwargs["audience"])
        mock_post.assert_called_once_with(
            FUNCTION_URL,
            json={"stateCode": "US_XX", "collections": ["clientCollection"]},
            headers={"Authorization": "Bearer id-token"},
            timeout=60,
        )
        mock_post.return_value.raise_for_status.assert_called_once()

    @patch("recidiviz.workflows.etl.typesense_backfill_client.requests.post")
    @patch("recidiviz.workflows.etl.typesense_backfill_client.fetch_id_token")
    @patch("recidiviz.workflows.etl.typesense_backfill_client.get_secret")
    def test_trigger_opportunities_backfill_includes_source_collection(
        self,
        mock_get_secret: MagicMock,
        mock_fetch_id_token: MagicMock,
        mock_post: MagicMock,
        _mock_in_gcp: MagicMock,
    ) -> None:
        mock_get_secret.return_value = FUNCTION_URL
        mock_fetch_id_token.return_value = "id-token"
        mock_post.return_value.json.return_value = _response_body(imported=812)

        TypesenseBackfillClient().trigger_opportunities_backfill(
            state_code=StateCode.US_XX,
            source_collection="US_XX-supervisionLevelDowngrade",
        )

        mock_post.assert_called_once_with(
            FUNCTION_URL,
            json={
                "stateCode": "US_XX",
                "collections": ["opportunities"],
                "sourceCollection": "US_XX-supervisionLevelDowngrade",
            },
            headers={"Authorization": "Bearer id-token"},
            timeout=60,
        )
        mock_post.return_value.raise_for_status.assert_called_once()

    @patch("recidiviz.workflows.etl.typesense_backfill_client.requests.post")
    @patch("recidiviz.workflows.etl.typesense_backfill_client.fetch_id_token")
    @patch("recidiviz.workflows.etl.typesense_backfill_client.get_secret")
    def test_trigger_opportunities_backfill_warns_when_nothing_imported(
        self,
        mock_get_secret: MagicMock,
        mock_fetch_id_token: MagicMock,
        mock_post: MagicMock,
        _mock_in_gcp: MagicMock,
    ) -> None:
        mock_get_secret.return_value = FUNCTION_URL
        mock_fetch_id_token.return_value = "id-token"
        mock_post.return_value.json.return_value = _response_body(imported=0)

        with self.assertLogs(level="WARNING") as log_context:
            TypesenseBackfillClient().trigger_opportunities_backfill(
                state_code=StateCode.US_XX,
                source_collection="US_XX-supervisionLevelDowngrade",
            )

        self.assertIn(
            "Typesense backfill imported 0 documents", "\n".join(log_context.output)
        )

    @patch("recidiviz.workflows.etl.typesense_backfill_client.requests.post")
    @patch("recidiviz.workflows.etl.typesense_backfill_client.fetch_id_token")
    @patch("recidiviz.workflows.etl.typesense_backfill_client.get_secret")
    def test_trigger_backfill_tolerates_unreadable_response_body(
        self,
        mock_get_secret: MagicMock,
        mock_fetch_id_token: MagicMock,
        mock_post: MagicMock,
        _mock_in_gcp: MagicMock,
    ) -> None:
        """The backfill function is deployed separately, so a 200 whose body we cannot
        parse must not look like a failed trigger — the backfill has already run."""
        mock_get_secret.return_value = FUNCTION_URL
        mock_fetch_id_token.return_value = "id-token"
        mock_post.return_value.json.side_effect = ValueError("not JSON")
        mock_post.return_value.text = ""

        with self.assertLogs(level="WARNING") as log_context:
            TypesenseBackfillClient().trigger_backfill(
                state_code=StateCode.US_XX, collection="clientCollection"
            )

        mock_post.assert_called_once()
        self.assertIn("Could not read import totals", "\n".join(log_context.output))

    @patch("recidiviz.workflows.etl.typesense_backfill_client.requests.post")
    @patch("recidiviz.workflows.etl.typesense_backfill_client.fetch_id_token")
    @patch("recidiviz.workflows.etl.typesense_backfill_client.get_secret")
    def test_trigger_backfill_tolerates_missing_totals_field(
        self,
        mock_get_secret: MagicMock,
        mock_fetch_id_token: MagicMock,
        mock_post: MagicMock,
        _mock_in_gcp: MagicMock,
    ) -> None:
        mock_get_secret.return_value = FUNCTION_URL
        mock_fetch_id_token.return_value = "id-token"
        mock_post.return_value.json.return_value = {}
        mock_post.return_value.text = "{}"

        with self.assertLogs(level="WARNING") as log_context:
            TypesenseBackfillClient().trigger_backfill(
                state_code=StateCode.US_XX, collection="clientCollection"
            )

        self.assertIn("Could not read import totals", "\n".join(log_context.output))

    @patch("recidiviz.workflows.etl.typesense_backfill_client.requests.post")
    @patch("recidiviz.workflows.etl.typesense_backfill_client.fetch_id_token")
    @patch("recidiviz.workflows.etl.typesense_backfill_client.get_secret")
    def test_trigger_backfill_skips_when_secret_unset(
        self,
        mock_get_secret: MagicMock,
        mock_fetch_id_token: MagicMock,
        mock_post: MagicMock,
        _mock_in_gcp: MagicMock,
    ) -> None:
        mock_get_secret.return_value = None

        TypesenseBackfillClient().trigger_backfill(
            state_code=StateCode.US_XX, collection="clientCollection"
        )

        mock_fetch_id_token.assert_not_called()
        mock_post.assert_not_called()

    @patch("recidiviz.workflows.etl.typesense_backfill_client.requests.post")
    @patch("recidiviz.workflows.etl.typesense_backfill_client.get_secret")
    def test_trigger_backfill_skips_outside_gcp(
        self,
        mock_get_secret: MagicMock,
        mock_post: MagicMock,
        mock_in_gcp: MagicMock,
    ) -> None:
        mock_in_gcp.return_value = False

        TypesenseBackfillClient().trigger_backfill(
            state_code=StateCode.US_XX, collection="clientCollection"
        )

        mock_get_secret.assert_not_called()
        mock_post.assert_not_called()
