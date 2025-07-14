# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for normalize_state_person.py"""
import datetime
import unittest

from recidiviz.common.constants.state.state_person import StateGender
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactLocation,
    StateSupervisionContactReason,
    StateSupervisionContactStatus,
    StateSupervisionContactType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.entity_utils import get_all_entity_classes_in_module
from recidiviz.persistence.entity.state import entities, normalized_entities
from recidiviz.persistence.entity.state.normalized_entities import NormalizedStatePerson
from recidiviz.pipelines.ingest.state.normalization import normalize_state_person
from recidiviz.pipelines.ingest.state.normalization.normalize_state_person import (
    build_normalized_state_person,
)
from recidiviz.tests.persistence.entity.state.entities_test_utils import (
    generate_full_graph_state_person,
)
from recidiviz.tests.pipelines.fake_state_calculation_config_manager import (
    start_pipeline_delegate_getter_patchers,
)


class TestNormalizeStatePerson(unittest.TestCase):
    """Tests for normalize_state_person.py"""

    def setUp(self) -> None:
        self.delegate_patchers = start_pipeline_delegate_getter_patchers(
            normalize_state_person
        )

    def tearDown(self) -> None:
        for patcher in self.delegate_patchers:
            patcher.stop()

    def test_build_normalized_state_person_full_tree(self) -> None:
        pre_normalization_person = generate_full_graph_state_person(
            set_back_edges=True, include_person_back_edges=True, set_ids=True
        )

        # Test mostly checks that this does not crash
        normalized_person = build_normalized_state_person(
            person=pre_normalization_person,
            staff_external_id_to_staff_id={
                ("EMP1", "US_XX_STAFF_ID"): 1000,
                ("EMP2", "US_XX_STAFF_ID"): 2000,
                ("EMP3", "US_XX_STAFF_ID"): 3000,
            },
            expected_output_entities=get_all_entity_classes_in_module(
                normalized_entities
            ),
        )

        self.assertIsInstance(normalized_person, NormalizedStatePerson)

    def test_normalize_person_simple(self) -> None:
        pre_normalization_person = entities.StatePerson(
            person_id=1,
            state_code=StateCode.US_XX.value,
            birthdate=datetime.date(day=1, month=1, year=1991),
            full_name='{"given_names": "FIRST1", "middle_names": "MID1", "name_suffix": "", "surname": "LAST1"}',
            gender=StateGender.MALE,
            gender_raw_text="M",
            external_ids=[
                entities.StatePersonExternalId(
                    person_external_id_id=1,
                    state_code=StateCode.US_XX.value,
                    external_id="ELITE_ID_123",
                    id_type="US_XX_ID_TYPE",
                    is_current_display_id_for_type=None,
                    id_active_from_datetime=None,
                    id_active_to_datetime=None,
                )
            ],
            supervision_contacts=[
                entities.StateSupervisionContact(
                    supervision_contact_id=1,
                    external_id="CONTACT_ID",
                    status=StateSupervisionContactStatus.COMPLETED,
                    status_raw_text="COMPLETED",
                    contact_type=StateSupervisionContactType.DIRECT,
                    contact_type_raw_text="FACE_TO_FACE",
                    contact_date=datetime.date(year=1111, month=1, day=2),
                    state_code=StateCode.US_XX.value,
                    contact_reason=StateSupervisionContactReason.GENERAL_CONTACT,
                    contact_reason_raw_text="GENERAL_CONTACT",
                    location=StateSupervisionContactLocation.RESIDENCE,
                    location_raw_text="RESIDENCE",
                    verified_employment=True,
                    resulted_in_arrest=False,
                    contacting_staff_external_id="EMP2",
                    contacting_staff_external_id_type="US_XX_STAFF_ID",
                )
            ],
        )

        expected_normalized_person = normalized_entities.NormalizedStatePerson(
            person_id=1,
            state_code=StateCode.US_XX.value,
            birthdate=datetime.date(day=1, month=1, year=1991),
            full_name='{"given_names": "FIRST1", "middle_names": "MID1", "name_suffix": "", "surname": "LAST1"}',
            gender=StateGender.MALE,
            gender_raw_text="M",
            external_ids=[
                normalized_entities.NormalizedStatePersonExternalId(
                    person_external_id_id=1,
                    state_code=StateCode.US_XX.value,
                    external_id="ELITE_ID_123",
                    id_type="US_XX_ID_TYPE",
                    is_current_display_id_for_type=True,
                    # TODO(#45291): This value should change to True once normalization
                    #  for this field is implemented.
                    is_stable_id_for_type=None,
                    id_active_from_datetime=None,
                    id_active_to_datetime=None,
                )
            ],
            supervision_contacts=[
                normalized_entities.NormalizedStateSupervisionContact(
                    supervision_contact_id=1,
                    external_id="CONTACT_ID",
                    status=StateSupervisionContactStatus.COMPLETED,
                    status_raw_text="COMPLETED",
                    contact_type=StateSupervisionContactType.DIRECT,
                    contact_type_raw_text="FACE_TO_FACE",
                    contact_date=datetime.date(year=1111, month=1, day=2),
                    state_code=StateCode.US_XX.value,
                    contact_reason=StateSupervisionContactReason.GENERAL_CONTACT,
                    contact_reason_raw_text="GENERAL_CONTACT",
                    location=StateSupervisionContactLocation.RESIDENCE,
                    location_raw_text="RESIDENCE",
                    verified_employment=True,
                    resulted_in_arrest=False,
                    contacting_staff_external_id="EMP2",
                    contacting_staff_external_id_type="US_XX_STAFF_ID",
                    contacting_staff_id=2000,
                )
            ],
        )
        # Set back edges
        expected_normalized_person.external_ids[0].person = expected_normalized_person
        expected_normalized_person.supervision_contacts[
            0
        ].person = expected_normalized_person

        normalized_person = build_normalized_state_person(
            person=pre_normalization_person,
            staff_external_id_to_staff_id={
                ("EMP2", "US_XX_STAFF_ID"): 2000,
            },
            expected_output_entities=get_all_entity_classes_in_module(
                normalized_entities
            ),
        )

        self.assertEqual(expected_normalized_person, normalized_person)
