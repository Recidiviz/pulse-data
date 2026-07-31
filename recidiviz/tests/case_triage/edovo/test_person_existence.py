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
"""Unit tests for the Edovo person existence check."""
from unittest import TestCase
from unittest.mock import MagicMock, patch

from recidiviz.big_query.big_query_utils import schema_field_for_type
from recidiviz.case_triage.edovo.external_id_matching import PERSON_EXTERNAL_ID_ADDRESS
from recidiviz.case_triage.edovo.person_existence import (
    PersonNotFoundError,
    assert_person_exists,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)

MODULE = "recidiviz.case_triage.edovo.person_existence"

_EXTERNAL_ID = "A123456"
_US_CO_ID_TYPE = "US_CO_ADCNUMBER"


def _make_bq_client(rows: list[dict]) -> MagicMock:
    mock_client = MagicMock()
    mock_client.run_query_async.return_value = iter(rows)
    return mock_client


class TestAssertPersonExists(TestCase):
    """Tests for confirming an Edovo external id resolves to a known person."""

    @patch(f"{MODULE}.project_id", return_value="recidiviz-123")
    def test_does_not_raise_when_found(self, _mock_pid: MagicMock) -> None:
        bq_client = _make_bq_client([{"person_id": "9876543"}])
        assert_person_exists(bq_client, StateCode.US_CO, _EXTERNAL_ID)

    @patch(f"{MODULE}.project_id", return_value="recidiviz-123")
    def test_raises_when_not_found(self, _mock_pid: MagicMock) -> None:
        bq_client = _make_bq_client([])
        with self.assertRaises(PersonNotFoundError) as cm:
            assert_person_exists(bq_client, StateCode.US_CO, _EXTERNAL_ID)
        self.assertEqual(cm.exception.person_external_id, _EXTERNAL_ID)

    @patch(f"{MODULE}.project_id", return_value="recidiviz-123")
    def test_query_uses_correct_parameters(self, _mock_pid: MagicMock) -> None:
        bq_client = _make_bq_client([{"person_id": "9876543"}])
        assert_person_exists(bq_client, StateCode.US_CO, _EXTERNAL_ID)

        _, kwargs = bq_client.run_query_async.call_args
        params = {p.name: p.value for p in kwargs["query_parameters"]}
        self.assertEqual(params["state_code"], "US_CO")
        self.assertEqual(params["id_type"], _US_CO_ID_TYPE)
        self.assertEqual(params["external_id"], _EXTERNAL_ID)

    @patch(f"{MODULE}.project_id", return_value="recidiviz-123")
    def test_submitted_id_is_bound_without_normalization(
        self, _mock_pid: MagicMock
    ) -> None:
        """Normalization belongs in the SQL comparison only: the value we bind —
        and go on to persist — stays exactly what Edovo submitted. Whether the
        comparison then matches is covered against the emulator below."""
        bq_client = _make_bq_client([{"person_id": "9876543"}])
        assert_person_exists(bq_client, StateCode.US_CO, "000123456")

        _, kwargs = bq_client.run_query_async.call_args
        params = {p.name: p.value for p in kwargs["query_parameters"]}
        self.assertEqual(params["external_id"], "000123456")

    @patch(f"{MODULE}.project_id", return_value="recidiviz-123")
    def test_query_references_correct_project(self, _mock_pid: MagicMock) -> None:
        bq_client = _make_bq_client([{"person_id": "9876543"}])
        assert_person_exists(bq_client, StateCode.US_CO, _EXTERNAL_ID)

        _, kwargs = bq_client.run_query_async.call_args
        self.assertIn(
            "recidiviz-123.normalized_state.state_person_external_id",
            kwargs["query_str"],
        )


class TestAssertPersonExistsAgainstEmulator(BigQueryEmulatorTestCase):
    """Runs the production existence query against the BigQuery emulator.

    The mocks above return a fixed result, so they say nothing about whether the
    comparison matches. These run the real query over a real table, so a SQL
    engine verifies the zero-stripping rather than us asserting it.
    """

    _TABLE_ADDRESS = PERSON_EXTERNAL_ID_ADDRESS

    def setUp(self) -> None:
        super().setUp()
        self.project_id_override = patch(
            f"{MODULE}.project_id", return_value=self.project_id
        )
        self.project_id_override.start()
        self.create_mock_table(
            address=self._TABLE_ADDRESS,
            schema=[
                schema_field_for_type("state_code", str),
                schema_field_for_type("external_id", str),
                schema_field_for_type("id_type", str),
            ],
        )

    def tearDown(self) -> None:
        self.project_id_override.stop()
        super().tearDown()

    def _load_person(
        self,
        *,
        external_id: str,
        state_code: str = StateCode.US_CO.value,
        id_type: str = _US_CO_ID_TYPE,
    ) -> None:
        self.load_rows_into_table(
            self._TABLE_ADDRESS,
            [
                {
                    "state_code": state_code,
                    "external_id": external_id,
                    "id_type": id_type,
                }
            ],
        )

    def test_padded_submitted_id_matches_unpadded_stored_id(self) -> None:
        self._load_person(external_id="123456")
        assert_person_exists(self.bq_client, StateCode.US_CO, "000123456")

    def test_unpadded_submitted_id_matches_padded_stored_id(self) -> None:
        self._load_person(external_id="000123456")
        assert_person_exists(self.bq_client, StateCode.US_CO, "123456")

    def test_raises_when_id_differs_by_more_than_leading_zeros(self) -> None:
        self._load_person(external_id="000123456")
        with self.assertRaises(PersonNotFoundError):
            assert_person_exists(self.bq_client, StateCode.US_CO, "1234567")

    def test_raises_when_ids_differ_only_by_trailing_zeros(self) -> None:
        """Only *leading* zeros are normalized. ``TRIM`` on both ends would
        wrongly match these, so this pins the choice of ``LTRIM``."""
        self._load_person(external_id="100")
        with self.assertRaises(PersonNotFoundError):
            assert_person_exists(self.bq_client, StateCode.US_CO, "1000")

    def test_does_not_match_across_states(self) -> None:
        """Zero-stripping must not widen the match past the state filter."""
        self._load_person(external_id="123456", state_code=StateCode.US_XX.value)
        with self.assertRaises(PersonNotFoundError):
            assert_person_exists(self.bq_client, StateCode.US_CO, "000123456")

    def test_all_zero_stored_id_still_matches_itself(self) -> None:
        """Normalizing must not break a match that already worked: US_CO has one
        stored id that is entirely zeros, so it is left unstripped rather than
        collapsed to an empty string."""
        self._load_person(external_id="000000")
        assert_person_exists(self.bq_client, StateCode.US_CO, "000000")

    def test_shorter_zero_id_does_not_match_all_zero_stored_id(self) -> None:
        """Collapsing every all-zero id to one value would let these match, which
        would credit the wrong person."""
        self._load_person(external_id="000000")
        for submitted in ["0", "000", ""]:
            with self.subTest(submitted=submitted):
                with self.assertRaises(PersonNotFoundError):
                    assert_person_exists(self.bq_client, StateCode.US_CO, submitted)

    def test_does_not_match_across_id_types(self) -> None:
        """Zero-stripping must not widen the match past the id_type filter."""
        self._load_person(external_id="123456", id_type="US_CO_SOME_OTHER_ID")
        with self.assertRaises(PersonNotFoundError):
            assert_person_exists(self.bq_client, StateCode.US_CO, "000123456")
