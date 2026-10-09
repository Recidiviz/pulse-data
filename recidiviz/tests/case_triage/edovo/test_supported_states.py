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
"""Tests the states the Edovo course-completion endpoint accepts, and US_AR
support for course completions."""
import json
from datetime import datetime, timezone
from decimal import Decimal
from unittest import TestCase
from unittest.mock import patch

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.big_query_utils import schema_field_for_type
from recidiviz.calculator.query.state.views.reference.product_person_external_id_helpers import (
    get_product_display_person_external_id_types_by_state,
)
from recidiviz.case_triage.edovo.course_completion_models import CourseCompletionRequest
from recidiviz.case_triage.edovo.external_id_matching import PERSON_EXTERNAL_ID_ADDRESS
from recidiviz.case_triage.edovo.person_verification import (
    PERSON_ADDRESS,
    PersonNotFoundError,
    verify_person_identity,
)
from recidiviz.case_triage.edovo.supported_states import SUPPORTED_STATES
from recidiviz.common.constants.state.external_id_types import (
    US_AR_ADCNUMBER,
    US_AR_OFFENDERID,
    US_CO_ADCNUMBER,
)
from recidiviz.common.constants.state.state_system_type import StateSystemType
from recidiviz.common.constants.states import StateCode
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)

PERSON_VERIFICATION_MODULE = "recidiviz.case_triage.edovo.person_verification"

_AR_FIRST_NAME = "Alex"
_AR_LAST_NAME = "Rivera"

_AR_ID_NO_LEADING_ZEROS = "742301"
_AR_ID_WITH_LEADING_ZEROS = "007423"

_AR_PAYLOAD: dict[str, object] = {
    "person_external_id": _AR_ID_NO_LEADING_ZEROS,
    "state_code": StateCode.US_AR.value,
    "course_id": "ar-course-001",
    "course_name": "Synthetic AR Course",
    "content_hours": 4.0,
    "completed_at": "2026-08-13T14:00:00Z",
    "first_name": _AR_FIRST_NAME,
    "last_name": _AR_LAST_NAME,
    "facility": "AR-SYNTHETIC-UNIT",
    "api_version": 2,
}


def _verify_ar_person(
    bq_client: BigQueryClientImpl, state_code: StateCode, external_id: str
) -> None:
    """Verifies |external_id| against the name ``_load_person`` stores.

    These tests cover identifier matching, so the name is held constant and
    always agrees; the name comparison itself is covered in
    ``test_person_verification.py``.
    """
    verify_person_identity(
        bq_client=bq_client,
        state_code=state_code,
        person_external_id=external_id,
        first_name=_AR_FIRST_NAME,
        last_name=_AR_LAST_NAME,
    )


class TestSupportedStates(TestCase):
    """Tests the supported-state configuration."""

    def test_us_ar_maps_to_adcnumber(self) -> None:
        self.assertEqual(US_AR_ADCNUMBER, SUPPORTED_STATES[StateCode.US_AR])

    def test_us_co_still_maps_to_adcnumber(self) -> None:
        self.assertEqual(US_CO_ADCNUMBER, SUPPORTED_STATES[StateCode.US_CO])

    def test_every_id_type_is_prefixed_with_its_own_state(self) -> None:
        for state_code, id_type in SUPPORTED_STATES.items():
            with self.subTest(state_code=state_code):
                self.assertTrue(
                    id_type.startswith(f"{state_code.value}_"),
                    f"id_type [{id_type}] is not an id_type of [{state_code.value}]",
                )

    def test_id_types_agree_with_the_product_display_id_type(self) -> None:
        """Edovo identifies a learner by the DOC-facing number, which is also the
        id Workflows displays for an incarceration product — today the two agree
        for every supported state.

        They are asserted equal here rather than SUPPORTED_STATES being derived
        from the product helper, because the two are different kinds of decision
        that only happen to coincide. What Edovo sends is fixed by our API
        contract with them; the product display id is ours to change. Deriving
        one from the other would let an internal product change silently alter
        which id_type the external endpoint accepts. This test instead fails
        loudly if either side moves, so the divergence gets a human decision.
        """
        product_id_types = get_product_display_person_external_id_types_by_state(
            StateSystemType.INCARCERATION
        )
        for state_code, edovo_id_type in SUPPORTED_STATES.items():
            with self.subTest(state_code=state_code):
                self.assertEqual(
                    product_id_types[state_code],
                    edovo_id_type,
                    f"The Edovo id_type for [{state_code.value}] no longer matches "
                    f"the product display id type. If this is intentional, confirm "
                    f"the change against the Edovo API spec before updating either "
                    f"side — Edovo's submitted id is set by contract, not by our "
                    f"product configuration.",
                )


class TestUsArCourseCompletionRequest(TestCase):
    def test_ar_payload_is_accepted_and_typed_as_adcnumber(self) -> None:
        request = CourseCompletionRequest.model_validate(_AR_PAYLOAD)

        self.assertEqual(StateCode.US_AR.value, request.state_code)
        self.assertEqual(US_AR_ADCNUMBER, request.id_type)
        self.assertEqual(_AR_ID_NO_LEADING_ZEROS, request.person_external_id)
        self.assertEqual(Decimal("4.0"), request.content_hours)
        self.assertEqual(
            datetime(2026, 8, 13, 14, 0, 0, tzinfo=timezone.utc), request.completed_at
        )

    def test_leading_zero_ar_id_is_held_verbatim(self) -> None:
        request = CourseCompletionRequest.model_validate(
            {**_AR_PAYLOAD, "person_external_id": _AR_ID_WITH_LEADING_ZEROS}
        )
        self.assertEqual(_AR_ID_WITH_LEADING_ZEROS, request.person_external_id)


class TestUsArPersonVerificationAgainstEmulator(BigQueryEmulatorTestCase):
    """Tests US_AR person matching."""

    _TABLE_ADDRESS = PERSON_EXTERNAL_ID_ADDRESS

    def setUp(self) -> None:
        super().setUp()
        self.project_id_override = patch(
            f"{PERSON_VERIFICATION_MODULE}.project_id", return_value=self.project_id
        )
        self.project_id_override.start()
        self.create_mock_table(
            address=self._TABLE_ADDRESS,
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
        state_code: str = StateCode.US_AR.value,
        id_type: str = US_AR_ADCNUMBER,
    ) -> None:
        self.load_rows_into_table(
            self._TABLE_ADDRESS,
            [
                {
                    "state_code": state_code,
                    "person_id": 1,
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
                    "person_id": 1,
                    "full_name": json.dumps(
                        {"given_names": _AR_FIRST_NAME, "surname": _AR_LAST_NAME}
                    ),
                }
            ],
        )

    def test_unpadded_ar_id_round_trips(self) -> None:
        self._load_person(external_id=_AR_ID_NO_LEADING_ZEROS)
        _verify_ar_person(self.bq_client, StateCode.US_AR, _AR_ID_NO_LEADING_ZEROS)

    def test_leading_zero_ar_id_round_trips(self) -> None:
        self._load_person(external_id=_AR_ID_WITH_LEADING_ZEROS)
        _verify_ar_person(self.bq_client, StateCode.US_AR, _AR_ID_WITH_LEADING_ZEROS)

    def test_padded_submission_matches_leading_zero_stored_id(self) -> None:
        self._load_person(external_id=_AR_ID_WITH_LEADING_ZEROS)
        _verify_ar_person(
            self.bq_client, StateCode.US_AR, f"000{_AR_ID_WITH_LEADING_ZEROS}"
        )

    def test_stripped_submission_matches_leading_zero_stored_id(self) -> None:
        self._load_person(external_id=_AR_ID_WITH_LEADING_ZEROS)
        _verify_ar_person(self.bq_client, StateCode.US_AR, "7423")

    def test_unknown_ar_id_raises_person_not_found(self) -> None:
        self._load_person(external_id=_AR_ID_NO_LEADING_ZEROS)
        with self.assertRaisesRegex(
            PersonNotFoundError, r"^No person found for the provided external_id\.$"
        ):
            _verify_ar_person(self.bq_client, StateCode.US_AR, "999999")

    def test_ar_id_does_not_match_the_same_digits_in_another_state(self) -> None:
        self._load_person(
            external_id=_AR_ID_NO_LEADING_ZEROS, state_code=StateCode.US_CO.value
        )
        with self.assertRaisesRegex(
            PersonNotFoundError, r"^No person found for the provided external_id\.$"
        ):
            _verify_ar_person(self.bq_client, StateCode.US_AR, _AR_ID_NO_LEADING_ZEROS)

    def test_ar_id_does_not_match_an_ar_offenderid(self) -> None:
        self._load_person(external_id=_AR_ID_NO_LEADING_ZEROS, id_type=US_AR_OFFENDERID)
        with self.assertRaisesRegex(
            PersonNotFoundError, r"^No person found for the provided external_id\.$"
        ):
            _verify_ar_person(self.bq_client, StateCode.US_AR, _AR_ID_NO_LEADING_ZEROS)

    def test_distinct_fixed_width_ar_ids_cannot_collide(self) -> None:
        self._load_person(external_id=_AR_ID_WITH_LEADING_ZEROS)
        for other_six_character_id in ["074230", "742300", "070423"]:
            with self.subTest(external_id=other_six_character_id):
                with self.assertRaisesRegex(
                    PersonNotFoundError,
                    r"^No person found for the provided external_id\.$",
                ):
                    _verify_ar_person(
                        self.bq_client, StateCode.US_AR, other_six_character_id
                    )

    def test_seven_character_id_would_collide_with_a_six_character_id(self) -> None:
        self._load_person(external_id=_AR_ID_WITH_LEADING_ZEROS)
        _verify_ar_person(
            self.bq_client, StateCode.US_AR, f"0{_AR_ID_WITH_LEADING_ZEROS}"
        )
