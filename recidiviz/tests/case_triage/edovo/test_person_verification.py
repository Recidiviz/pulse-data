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
"""Unit tests for the Edovo person identity verification."""
import json
from unittest import TestCase
from unittest.mock import MagicMock, patch

from recidiviz.big_query.big_query_utils import schema_field_for_type
from recidiviz.case_triage.edovo.course_completion_models import (
    FIRST_NAME_FIELD,
    LAST_NAME_FIELD,
    CourseCompletionRequest,
)
from recidiviz.case_triage.edovo.external_id_matching import PERSON_EXTERNAL_ID_ADDRESS
from recidiviz.case_triage.edovo.person_verification import (
    PERSON_ADDRESS,
    PersonNameMismatchError,
    PersonNotFoundError,
    verify_person_identity,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)

MODULE = "recidiviz.case_triage.edovo.person_verification"

_EXTERNAL_ID = "A123456"
_US_CO_ID_TYPE = "US_CO_ADCNUMBER"
_FIRST_NAME = "Jane"
_LAST_NAME = "Doe"


def _make_bq_client(rows: list[dict[str, str]]) -> MagicMock:
    mock_client = MagicMock()
    mock_client.run_query_async.return_value = iter(rows)
    return mock_client


def _name_row(given_names: str, surname: str) -> dict[str, str]:
    return {"given_names": given_names, "surname": surname}


class TestMismatchedFieldNames(TestCase):
    """Tests that the field names reported to Edovo are the ones they sent."""

    def test_reported_field_names_are_request_field_names(self) -> None:
        """``mismatched_fields`` tells Edovo which submitted field disagreed, so
        each name has to be a field they actually sent. Renaming a request field
        without updating these constants would leave us naming a field that no
        longer exists, which this catches."""
        self.assertIn(FIRST_NAME_FIELD, CourseCompletionRequest.model_fields)
        self.assertIn(LAST_NAME_FIELD, CourseCompletionRequest.model_fields)


class TestVerifyPersonIdentity(TestCase):
    """Tests for confirming an Edovo external id resolves to the named person."""

    @patch(f"{MODULE}.project_id", return_value="recidiviz-123")
    def test_does_not_raise_when_name_matches(self, _mock_pid: MagicMock) -> None:
        bq_client = _make_bq_client([_name_row("JANE", "DOE")])
        verify_person_identity(
            bq_client=bq_client,
            state_code=StateCode.US_CO,
            person_external_id=_EXTERNAL_ID,
            first_name=_FIRST_NAME,
            last_name=_LAST_NAME,
        )

    @patch(f"{MODULE}.project_id", return_value="recidiviz-123")
    def test_raises_not_found_when_no_record(self, _mock_pid: MagicMock) -> None:
        bq_client = _make_bq_client([])
        with self.assertRaisesRegex(
            PersonNotFoundError, r"^No person found for the provided external_id\.$"
        ) as cm:
            verify_person_identity(
                bq_client=bq_client,
                state_code=StateCode.US_CO,
                person_external_id=_EXTERNAL_ID,
                first_name=_FIRST_NAME,
                last_name=_LAST_NAME,
            )
        self.assertEqual(cm.exception.person_external_id, _EXTERNAL_ID)

    @patch(f"{MODULE}.project_id", return_value="recidiviz-123")
    def test_raises_mismatch_on_different_surname(self, _mock_pid: MagicMock) -> None:
        bq_client = _make_bq_client([_name_row("JANE", "SMITH")])
        with self.assertRaisesRegex(
            PersonNameMismatchError,
            r"^The provided name does not match our record for this external_id\.$",
        ) as cm:
            verify_person_identity(
                bq_client=bq_client,
                state_code=StateCode.US_CO,
                person_external_id=_EXTERNAL_ID,
                first_name=_FIRST_NAME,
                last_name=_LAST_NAME,
            )
        self.assertEqual(cm.exception.person_external_id, _EXTERNAL_ID)
        self.assertEqual(["last_name"], cm.exception.mismatched_fields)

    @patch(f"{MODULE}.project_id", return_value="recidiviz-123")
    def test_raises_mismatch_on_different_first_name(
        self, _mock_pid: MagicMock
    ) -> None:
        bq_client = _make_bq_client([_name_row("ROBERT", "DOE")])
        with self.assertRaisesRegex(
            PersonNameMismatchError,
            r"^The provided name does not match our record for this external_id\.$",
        ) as cm:
            verify_person_identity(
                bq_client=bq_client,
                state_code=StateCode.US_CO,
                person_external_id=_EXTERNAL_ID,
                first_name=_FIRST_NAME,
                last_name=_LAST_NAME,
            )
        self.assertEqual(["first_name"], cm.exception.mismatched_fields)

    @patch(f"{MODULE}.project_id", return_value="recidiviz-123")
    def test_raises_mismatch_naming_both_fields(self, _mock_pid: MagicMock) -> None:
        bq_client = _make_bq_client([_name_row("ROBERT", "SMITH")])
        with self.assertRaisesRegex(
            PersonNameMismatchError,
            r"^The provided name does not match our record for this external_id\.$",
        ) as cm:
            verify_person_identity(
                bq_client=bq_client,
                state_code=StateCode.US_CO,
                person_external_id=_EXTERNAL_ID,
                first_name=_FIRST_NAME,
                last_name=_LAST_NAME,
            )
        self.assertEqual(["first_name", "last_name"], cm.exception.mismatched_fields)

    @patch(f"{MODULE}.project_id", return_value="recidiviz-123")
    def test_matches_when_stored_given_names_include_a_middle_name(
        self, _mock_pid: MagicMock
    ) -> None:
        bq_client = _make_bq_client([_name_row("JANE MARIE", "DOE")])
        verify_person_identity(
            bq_client=bq_client,
            state_code=StateCode.US_CO,
            person_external_id=_EXTERNAL_ID,
            first_name=_FIRST_NAME,
            last_name=_LAST_NAME,
        )

    @patch(f"{MODULE}.project_id", return_value="recidiviz-123")
    def test_matches_across_punctuation_and_case(self, _mock_pid: MagicMock) -> None:
        bq_client = _make_bq_client([_name_row("jane", "o'brien-doe")])
        verify_person_identity(
            bq_client=bq_client,
            state_code=StateCode.US_CO,
            person_external_id=_EXTERNAL_ID,
            first_name="JANE",
            last_name="OBrien Doe",
        )

    @patch(f"{MODULE}.project_id", return_value="recidiviz-123")
    def test_matches_when_we_hold_no_name(self, _mock_pid: MagicMock) -> None:
        """An incomplete record on our side is not identifier drift on Edovo's,
        so a name part we hold no value for cannot fail the check."""
        bq_client = _make_bq_client([_name_row("", "")])
        verify_person_identity(
            bq_client=bq_client,
            state_code=StateCode.US_CO,
            person_external_id=_EXTERNAL_ID,
            first_name=_FIRST_NAME,
            last_name=_LAST_NAME,
        )

    @patch(f"{MODULE}.project_id", return_value="recidiviz-123")
    def test_matches_when_any_candidate_matches(self, _mock_pid: MagicMock) -> None:
        """One id resolving to two records must not fail merely because the
        second record is someone else."""
        bq_client = _make_bq_client(
            [_name_row("ROBERT", "SMITH"), _name_row("JANE", "DOE")]
        )
        verify_person_identity(
            bq_client=bq_client,
            state_code=StateCode.US_CO,
            person_external_id=_EXTERNAL_ID,
            first_name=_FIRST_NAME,
            last_name=_LAST_NAME,
        )

    @patch(f"{MODULE}.project_id", return_value="recidiviz-123")
    def test_reports_both_fields_when_no_single_candidate_matches_both(
        self, _mock_pid: MagicMock
    ) -> None:
        """Each part matches a different record, so neither part alone is the
        problem — the pairing is."""
        bq_client = _make_bq_client(
            [_name_row("JANE", "SMITH"), _name_row("ROBERT", "DOE")]
        )
        with self.assertRaisesRegex(
            PersonNameMismatchError,
            r"^The provided name does not match our record for this external_id\.$",
        ) as cm:
            verify_person_identity(
                bq_client=bq_client,
                state_code=StateCode.US_CO,
                person_external_id=_EXTERNAL_ID,
                first_name=_FIRST_NAME,
                last_name=_LAST_NAME,
            )
        self.assertEqual(["first_name", "last_name"], cm.exception.mismatched_fields)

    @patch(f"{MODULE}.project_id", return_value="recidiviz-123")
    def test_query_uses_correct_parameters(self, _mock_pid: MagicMock) -> None:
        bq_client = _make_bq_client([_name_row("JANE", "DOE")])
        verify_person_identity(
            bq_client=bq_client,
            state_code=StateCode.US_CO,
            person_external_id=_EXTERNAL_ID,
            first_name=_FIRST_NAME,
            last_name=_LAST_NAME,
        )

        _, kwargs = bq_client.run_query_async.call_args
        params = {p.name: p.value for p in kwargs["query_parameters"]}
        self.assertEqual(params["state_code"], "US_CO")
        self.assertEqual(params["id_type"], _US_CO_ID_TYPE)
        self.assertEqual(params["external_id"], _EXTERNAL_ID)

    @patch(f"{MODULE}.project_id", return_value="recidiviz-123")
    def test_names_are_not_bound_as_query_parameters(
        self, _mock_pid: MagicMock
    ) -> None:
        """The name comparison happens in Python, so the query filters on the
        id alone and returns every candidate for comparison."""
        bq_client = _make_bq_client([_name_row("JANE", "DOE")])
        verify_person_identity(
            bq_client=bq_client,
            state_code=StateCode.US_CO,
            person_external_id=_EXTERNAL_ID,
            first_name=_FIRST_NAME,
            last_name=_LAST_NAME,
        )

        _, kwargs = bq_client.run_query_async.call_args
        params = {p.name: p.value for p in kwargs["query_parameters"]}
        self.assertEqual({"state_code", "id_type", "external_id"}, set(params))

    @patch(f"{MODULE}.project_id", return_value="recidiviz-123")
    def test_submitted_id_is_bound_without_normalization(
        self, _mock_pid: MagicMock
    ) -> None:
        """Normalization belongs in the SQL comparison only: the value we bind —
        and go on to persist — stays exactly what Edovo submitted. Whether the
        comparison then matches is covered against the emulator below."""
        bq_client = _make_bq_client([_name_row("JANE", "DOE")])
        verify_person_identity(
            bq_client=bq_client,
            state_code=StateCode.US_CO,
            person_external_id="000123456",
            first_name=_FIRST_NAME,
            last_name=_LAST_NAME,
        )

        _, kwargs = bq_client.run_query_async.call_args
        params = {p.name: p.value for p in kwargs["query_parameters"]}
        self.assertEqual(params["external_id"], "000123456")

    @patch(f"{MODULE}.project_id", return_value="recidiviz-123")
    def test_query_references_correct_project(self, _mock_pid: MagicMock) -> None:
        bq_client = _make_bq_client([_name_row("JANE", "DOE")])
        verify_person_identity(
            bq_client=bq_client,
            state_code=StateCode.US_CO,
            person_external_id=_EXTERNAL_ID,
            first_name=_FIRST_NAME,
            last_name=_LAST_NAME,
        )

        _, kwargs = bq_client.run_query_async.call_args
        self.assertIn(
            "recidiviz-123.normalized_state.state_person_external_id",
            kwargs["query_str"],
        )
        self.assertIn(
            "recidiviz-123.normalized_state.state_person",
            kwargs["query_str"],
        )


class TestVerifyPersonIdentityAgainstEmulator(BigQueryEmulatorTestCase):
    """Runs the production verification query against the BigQuery emulator.

    The mocks above return a fixed result, so they say nothing about whether the
    comparison matches. These run the real query over real tables, so a SQL
    engine verifies the zero-stripping and the name extraction rather than us
    asserting them.
    """

    def setUp(self) -> None:
        super().setUp()
        self.project_id_override = patch(
            f"{MODULE}.project_id", return_value=self.project_id
        )
        self.project_id_override.start()
        self.create_mock_table(
            address=PERSON_EXTERNAL_ID_ADDRESS,
            schema=[
                schema_field_for_type("state_code", str),
                schema_field_for_type("person_id", int),
                schema_field_for_type("external_id", str),
                schema_field_for_type("id_type", str),
            ],
        )
        self.create_mock_table(
            address=PERSON_ADDRESS,
            schema=[
                schema_field_for_type("state_code", str),
                schema_field_for_type("person_id", int),
                schema_field_for_type("full_name", str),
            ],
        )

    def tearDown(self) -> None:
        self.project_id_override.stop()
        super().tearDown()

    def _load_person(
        self,
        *,
        external_id: str,
        given_names: str,
        surname: str,
        person_id: int = 1,
        state_code: str = StateCode.US_CO.value,
        id_type: str = _US_CO_ID_TYPE,
    ) -> None:
        self.load_rows_into_table(
            PERSON_EXTERNAL_ID_ADDRESS,
            [
                {
                    "state_code": state_code,
                    "person_id": person_id,
                    "external_id": external_id,
                    "id_type": id_type,
                }
            ],
        )
        self.load_rows_into_table(
            PERSON_ADDRESS,
            [
                {
                    "state_code": state_code,
                    "person_id": person_id,
                    "full_name": json.dumps(
                        {"given_names": given_names, "surname": surname}
                    ),
                }
            ],
        )

    def _verify(self, submitted_id: str) -> None:
        verify_person_identity(
            bq_client=self.bq_client,
            state_code=StateCode.US_CO,
            person_external_id=submitted_id,
            first_name=_FIRST_NAME,
            last_name=_LAST_NAME,
        )

    def test_stored_name_is_extracted_from_full_name_json(self) -> None:
        self._load_person(external_id="123456", given_names="JANE", surname="DOE")
        self._verify("123456")

    def test_external_id_with_no_person_row_is_still_found(self) -> None:
        """Before this check compared names it read one table, so an external id
        with no matching person row counted as found. A missing person row must
        not become PERSON_NOT_FOUND and reject a learner we do have — the
        foreign key that should prevent it is not enforced in this dataset."""
        self.load_rows_into_table(
            PERSON_EXTERNAL_ID_ADDRESS,
            [
                {
                    "state_code": StateCode.US_CO.value,
                    "person_id": 999,
                    "external_id": "123456",
                    "id_type": _US_CO_ID_TYPE,
                }
            ],
        )

        self._verify("123456")

    def test_raises_mismatch_when_stored_name_is_another_person(self) -> None:
        self._load_person(external_id="123456", given_names="ROBERT", surname="SMITH")
        with self.assertRaisesRegex(
            PersonNameMismatchError,
            r"^The provided name does not match our record for this external_id\.$",
        ):
            self._verify("123456")

    def test_padded_submitted_id_matches_unpadded_stored_id(self) -> None:
        self._load_person(
            external_id="123456", given_names=_FIRST_NAME, surname=_LAST_NAME
        )
        self._verify("000123456")

    def test_unpadded_submitted_id_matches_padded_stored_id(self) -> None:
        self._load_person(
            external_id="000123456", given_names=_FIRST_NAME, surname=_LAST_NAME
        )
        self._verify("123456")

    def test_raises_when_id_differs_by_more_than_leading_zeros(self) -> None:
        self._load_person(
            external_id="000123456", given_names=_FIRST_NAME, surname=_LAST_NAME
        )
        with self.assertRaisesRegex(
            PersonNotFoundError, r"^No person found for the provided external_id\.$"
        ):
            self._verify("1234567")

    def test_raises_when_ids_differ_only_by_trailing_zeros(self) -> None:
        """Only *leading* zeros are normalized. ``TRIM`` on both ends would
        wrongly match these, so this pins the choice of ``LTRIM``."""
        self._load_person(
            external_id="100", given_names=_FIRST_NAME, surname=_LAST_NAME
        )
        with self.assertRaisesRegex(
            PersonNotFoundError, r"^No person found for the provided external_id\.$"
        ):
            self._verify("1000")

    def test_does_not_match_across_states(self) -> None:
        """Zero-stripping must not widen the match past the state filter."""
        self._load_person(
            external_id="123456",
            given_names=_FIRST_NAME,
            surname=_LAST_NAME,
            state_code=StateCode.US_XX.value,
        )
        with self.assertRaisesRegex(
            PersonNotFoundError, r"^No person found for the provided external_id\.$"
        ):
            self._verify("000123456")

    def test_all_zero_stored_id_still_matches_itself(self) -> None:
        """Normalizing must not break a match that already worked: US_CO has one
        stored id that is entirely zeros, so it is left unstripped rather than
        collapsed to an empty string."""
        self._load_person(
            external_id="000000", given_names=_FIRST_NAME, surname=_LAST_NAME
        )
        self._verify("000000")

    def test_shorter_zero_id_does_not_match_all_zero_stored_id(self) -> None:
        """Collapsing every all-zero id to one value would let these match, which
        would credit the wrong person."""
        self._load_person(
            external_id="000000", given_names=_FIRST_NAME, surname=_LAST_NAME
        )
        for submitted in ["0", "000", ""]:
            with self.subTest(submitted=submitted):
                with self.assertRaisesRegex(
                    PersonNotFoundError,
                    r"^No person found for the provided external_id\.$",
                ):
                    self._verify(submitted)

    def test_does_not_match_across_id_types(self) -> None:
        """Zero-stripping must not widen the match past the id_type filter."""
        self._load_person(
            external_id="123456",
            given_names=_FIRST_NAME,
            surname=_LAST_NAME,
            id_type="US_CO_SOME_OTHER_ID",
        )
        with self.assertRaisesRegex(
            PersonNotFoundError, r"^No person found for the provided external_id\.$"
        ):
            self._verify("000123456")
