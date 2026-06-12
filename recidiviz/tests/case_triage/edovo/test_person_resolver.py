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
"""Unit tests for the Edovo person resolver."""
from unittest import TestCase
from unittest.mock import MagicMock, patch

from google.cloud import bigquery

from recidiviz.case_triage.edovo.person_resolver import (
    PersonNotFoundError,
    resolve_person_by_doc_id,
)

MODULE = "recidiviz.case_triage.edovo.person_resolver"

_DOC_ID = "A123456"
_PERSON_ID = "9876543"


def _make_bq_client(rows: list[dict]) -> MagicMock:
    mock_client = MagicMock()
    mock_client.run_query_async.return_value = iter(rows)
    return mock_client


class TestResolvePersonByDocId(TestCase):
    @patch(f"{MODULE}.project_id", return_value="recidiviz-123")
    def test_returns_person_id_when_found(self, _mock_pid: MagicMock) -> None:
        bq_client = _make_bq_client([{"person_id": _PERSON_ID}])
        result = resolve_person_by_doc_id(bq_client, _DOC_ID)
        self.assertEqual(result, _PERSON_ID)

    @patch(f"{MODULE}.project_id", return_value="recidiviz-123")
    def test_raises_when_not_found(self, _mock_pid: MagicMock) -> None:
        bq_client = _make_bq_client([])
        with self.assertRaises(PersonNotFoundError) as cm:
            resolve_person_by_doc_id(bq_client, _DOC_ID)
        self.assertEqual(cm.exception.doc_id, _DOC_ID)

    @patch(f"{MODULE}.project_id", return_value="recidiviz-123")
    def test_query_uses_correct_parameters(self, _mock_pid: MagicMock) -> None:
        bq_client = _make_bq_client([{"person_id": _PERSON_ID}])
        resolve_person_by_doc_id(bq_client, _DOC_ID)

        _, kwargs = bq_client.run_query_async.call_args
        params = {p.name: p.value for p in kwargs["query_parameters"]}
        self.assertEqual(params["state_code"], "US_CO")
        self.assertEqual(params["id_type"], "US_CO_DOC_ID")
        self.assertEqual(params["external_id"], _DOC_ID)

    @patch(f"{MODULE}.project_id", return_value="recidiviz-123")
    def test_query_references_correct_project(self, _mock_pid: MagicMock) -> None:
        bq_client = _make_bq_client([{"person_id": _PERSON_ID}])
        resolve_person_by_doc_id(bq_client, _DOC_ID)

        _, kwargs = bq_client.run_query_async.call_args
        self.assertIn(
            "recidiviz-123.normalized_state.state_person_external_id",
            kwargs["query_str"],
        )

    @patch(f"{MODULE}.project_id", return_value="recidiviz-123")
    def test_person_id_coerced_to_str(self, _mock_pid: MagicMock) -> None:
        bq_client = _make_bq_client([{"person_id": 9876543}])
        result = resolve_person_by_doc_id(bq_client, _DOC_ID)
        self.assertIsInstance(result, str)
        self.assertEqual(result, "9876543")
