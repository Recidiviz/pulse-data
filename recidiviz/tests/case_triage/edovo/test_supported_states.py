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
"""Tests US_AR support for Edovo course completions."""
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from unittest import TestCase
from unittest.mock import MagicMock, patch

from flask import Flask

from recidiviz.big_query.big_query_utils import schema_field_for_type
from recidiviz.case_triage.edovo.course_completion_models import CourseCompletionRequest
from recidiviz.case_triage.edovo.credit_calculator import (
    HOURS_PER_CREDIT_BY_STATE,
    calculate_all_pending_credits,
)
from recidiviz.case_triage.edovo.external_id_matching import PERSON_EXTERNAL_ID_ADDRESS
from recidiviz.case_triage.edovo.person_existence import (
    PersonNotFoundError,
    assert_person_exists,
)
from recidiviz.case_triage.edovo.supported_states import SUPPORTED_STATES
from recidiviz.common.constants.state.external_id_types import (
    US_AR_ADCNUMBER,
    US_AR_OFFENDERID,
    US_CO_ADCNUMBER,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.case_triage.schema import (
    EdovoCourseCompletion,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_flask_utils import setup_scoped_sessions
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult

PERSON_EXISTENCE_MODULE = "recidiviz.case_triage.edovo.person_existence"
CREDIT_CALCULATOR_MODULE = "recidiviz.case_triage.edovo.credit_calculator"

_AR_ID_NO_LEADING_ZEROS = "742301"
_AR_ID_WITH_LEADING_ZEROS = "007423"

_AR_PAYLOAD: dict[str, object] = {
    "person_external_id": _AR_ID_NO_LEADING_ZEROS,
    "state_code": StateCode.US_AR.value,
    "course_id": "ar-course-001",
    "course_name": "Synthetic AR Course",
    "content_hours": 4.0,
    "completed_at": "2026-08-13T14:00:00Z",
}


class TestSupportedStates(TestCase):
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


class TestUsArPersonExistenceAgainstEmulator(BigQueryEmulatorTestCase):
    """Tests US_AR person matching."""

    _TABLE_ADDRESS = PERSON_EXTERNAL_ID_ADDRESS

    def setUp(self) -> None:
        super().setUp()
        self.project_id_override = patch(
            f"{PERSON_EXISTENCE_MODULE}.project_id", return_value=self.project_id
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
        state_code: str = StateCode.US_AR.value,
        id_type: str = US_AR_ADCNUMBER,
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

    def test_unpadded_ar_id_round_trips(self) -> None:
        self._load_person(external_id=_AR_ID_NO_LEADING_ZEROS)
        assert_person_exists(self.bq_client, StateCode.US_AR, _AR_ID_NO_LEADING_ZEROS)

    def test_leading_zero_ar_id_round_trips(self) -> None:
        self._load_person(external_id=_AR_ID_WITH_LEADING_ZEROS)
        assert_person_exists(self.bq_client, StateCode.US_AR, _AR_ID_WITH_LEADING_ZEROS)

    def test_padded_submission_matches_leading_zero_stored_id(self) -> None:
        self._load_person(external_id=_AR_ID_WITH_LEADING_ZEROS)
        assert_person_exists(
            self.bq_client, StateCode.US_AR, f"000{_AR_ID_WITH_LEADING_ZEROS}"
        )

    def test_stripped_submission_matches_leading_zero_stored_id(self) -> None:
        self._load_person(external_id=_AR_ID_WITH_LEADING_ZEROS)
        assert_person_exists(self.bq_client, StateCode.US_AR, "7423")

    def test_unknown_ar_id_raises_person_not_found(self) -> None:
        self._load_person(external_id=_AR_ID_NO_LEADING_ZEROS)
        with self.assertRaises(PersonNotFoundError):
            assert_person_exists(self.bq_client, StateCode.US_AR, "999999")

    def test_ar_id_does_not_match_the_same_digits_in_another_state(self) -> None:
        self._load_person(
            external_id=_AR_ID_NO_LEADING_ZEROS, state_code=StateCode.US_CO.value
        )
        with self.assertRaises(PersonNotFoundError):
            assert_person_exists(
                self.bq_client, StateCode.US_AR, _AR_ID_NO_LEADING_ZEROS
            )

    def test_ar_id_does_not_match_an_ar_offenderid(self) -> None:
        self._load_person(external_id=_AR_ID_NO_LEADING_ZEROS, id_type=US_AR_OFFENDERID)
        with self.assertRaises(PersonNotFoundError):
            assert_person_exists(
                self.bq_client, StateCode.US_AR, _AR_ID_NO_LEADING_ZEROS
            )

    def test_distinct_fixed_width_ar_ids_cannot_collide(self) -> None:
        self._load_person(external_id=_AR_ID_WITH_LEADING_ZEROS)
        for other_six_character_id in ["074230", "742300", "070423"]:
            with self.subTest(external_id=other_six_character_id):
                with self.assertRaises(PersonNotFoundError):
                    assert_person_exists(
                        self.bq_client, StateCode.US_AR, other_six_character_id
                    )

    def test_seven_character_id_would_collide_with_a_six_character_id(self) -> None:
        self._load_person(external_id=_AR_ID_WITH_LEADING_ZEROS)
        assert_person_exists(
            self.bq_client, StateCode.US_AR, f"0{_AR_ID_WITH_LEADING_ZEROS}"
        )


class TestUsArAgainstTheCoCreditCalculator(TestCase):
    """Tests US_AR behavior in the CO credit calculator."""

    postgres_launch_result: OnDiskPostgresLaunchResult

    @classmethod
    def setUpClass(cls) -> None:
        cls.postgres_launch_result = (
            local_postgres_helpers.start_on_disk_postgresql_database()
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.postgres_launch_result
        )

    def setUp(self) -> None:
        self.project_id_patcher = patch(
            f"{CREDIT_CALCULATOR_MODULE}.project_id", return_value="recidiviz-123"
        )
        self.project_id_patcher.start()
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
        self.overridden_env_vars = (
            local_persistence_helpers.update_local_sqlalchemy_postgres_env_vars(
                self.postgres_launch_result
            )
        )
        db_url = local_persistence_helpers.postgres_db_url_from_env_vars()
        engine = setup_scoped_sessions(Flask(__name__), SchemaType.CASE_TRIAGE, db_url)
        self.database_key.declarative_meta.metadata.create_all(engine)

    def tearDown(self) -> None:
        self.project_id_patcher.stop()
        local_postgres_helpers.restore_local_env_vars(self.overridden_env_vars)
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    def test_us_ar_has_no_configured_pooling_rate(self) -> None:
        self.assertIn(StateCode.US_AR, SUPPORTED_STATES)
        self.assertNotIn(StateCode.US_AR, HOURS_PER_CREDIT_BY_STATE)

    def test_an_ar_completion_stops_the_calculator_rather_than_being_credited(
        self,
    ) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add(
                EdovoCourseCompletion(
                    idempotency_key=uuid.uuid4(),
                    person_external_id=_AR_ID_NO_LEADING_ZEROS,
                    id_type=US_AR_ADCNUMBER,
                    state_code=StateCode.US_AR.value,
                    course_id="ar-course-001",
                    course_name="Synthetic AR Course",
                    content_hours=6.0,
                    completed_at=datetime(2026, 8, 13, tzinfo=timezone.utc),
                    received_at=datetime(2026, 8, 13, tzinfo=timezone.utc),
                )
            )

        bq_client = MagicMock()
        bq_client.run_query_async.return_value = iter(
            [{"stripped_external_id": _AR_ID_NO_LEADING_ZEROS, "person_id": 111}]
        )
        with SessionFactory.using_database(self.database_key) as session:
            with self.assertRaisesRegex(
                ValueError,
                r"No Edovo earned-time credit policy configured for state \[US_AR\]",
            ):
                calculate_all_pending_credits(session=session, bq_client=bq_client)

    def test_one_person_under_two_paddings_evades_the_capture_time_guard(self) -> None:
        for external_id in [_AR_ID_WITH_LEADING_ZEROS, "7423"]:
            with SessionFactory.using_database(self.database_key) as session:
                session.add(
                    EdovoCourseCompletion(
                        idempotency_key=uuid.uuid4(),
                        person_external_id=external_id,
                        id_type=US_AR_ADCNUMBER,
                        state_code=StateCode.US_AR.value,
                        course_id="ar-course-001",
                        course_name="Synthetic AR Course",
                        content_hours=6.0,
                        completed_at=datetime(2026, 8, 13, tzinfo=timezone.utc),
                        received_at=datetime(2026, 8, 13, tzinfo=timezone.utc),
                    )
                )

        with SessionFactory.using_database(self.database_key) as session:
            stored = [
                record.person_external_id
                for record in session.query(EdovoCourseCompletion).all()
            ]
        self.assertEqual([_AR_ID_WITH_LEADING_ZEROS, "7423"], sorted(stored))
