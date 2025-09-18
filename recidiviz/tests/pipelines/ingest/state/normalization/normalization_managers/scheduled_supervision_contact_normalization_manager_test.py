# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
# pylint: disable=protected-access
"""Tests for program_assignments_normalization_manager.py."""
import datetime
import unittest
from typing import List

from recidiviz.common.constants.state.state_scheduled_supervision_contact import (
    StateScheduledSupervisionContactMethod,
    StateScheduledSupervisionContactStatus,
    StateScheduledSupervisionContactType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.normalized_entities_utils import (
    AdditionalAttributesMap,
)
from recidiviz.persistence.entity.state.entities import StateScheduledSupervisionContact
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.scheduled_supervision_contact_normalization_manager import (
    ScheduledSupervisionContactNormalizationManager,
)
from recidiviz.pipelines.utils.execution_utils import (
    build_staff_external_id_to_staff_id_map,
)
from recidiviz.tests.pipelines.ingest.state.normalization.normalization_managers.assessment_normalization_manager_test import (
    STATE_PERSON_TO_STATE_STAFF_LIST,
)


class TestPrepareScheduledSupervisionContactsForCalculations(unittest.TestCase):
    """State-agnostic tests for pre-processing that happens to all supervision contacts
    regardless of state."""

    def setUp(self) -> None:
        self.state_code = "US_XX"

    def _normalized_scheduled_supervision_contacts_for_calculations(
        self, scheduled_supervision_contacts: List[StateScheduledSupervisionContact]
    ) -> List[StateScheduledSupervisionContact]:
        entity_normalization_manager = ScheduledSupervisionContactNormalizationManager(
            scheduled_supervision_contacts=scheduled_supervision_contacts,
            staff_external_id_to_staff_id=build_staff_external_id_to_staff_id_map(
                STATE_PERSON_TO_STATE_STAFF_LIST
            ),
        )

        (
            processed_scheduled_supervision_contacts,
            _,
        ) = (
            entity_normalization_manager.normalized_scheduled_supervision_contacts_and_additional_attributes()
        )

        return processed_scheduled_supervision_contacts

    def _normalized_scheduled_supervision_contacts_additional_attributes(
        self, scheduled_supervision_contacts: List[StateScheduledSupervisionContact]
    ) -> AdditionalAttributesMap:
        entity_normalization_manager = ScheduledSupervisionContactNormalizationManager(
            scheduled_supervision_contacts=scheduled_supervision_contacts,
            staff_external_id_to_staff_id=build_staff_external_id_to_staff_id_map(
                STATE_PERSON_TO_STATE_STAFF_LIST
            ),
        )

        (
            _,
            additional_attributes,
        ) = (
            entity_normalization_manager.normalized_scheduled_supervision_contacts_and_additional_attributes()
        )

        return additional_attributes

    def test_simple_scheduled_supervision_contacts_normalization(self) -> None:
        self.maxDiff = None
        sc_1 = StateScheduledSupervisionContact.new_with_defaults(
            scheduled_supervision_contact_id=1234,
            external_id="c1",
            state_code=StateCode.US_XX.value,
            scheduled_contact_date=datetime.date(2018, 3, 6),
            contact_type=StateScheduledSupervisionContactType.DIRECT,
            status=StateScheduledSupervisionContactStatus.SCHEDULED,
            update_datetime=datetime.datetime(2025, 1, 1),
            contact_method=StateScheduledSupervisionContactMethod.VIRTUAL,
            contact_method_raw_text="PHONE",
            contact_meeting_address="123 Fake St, Nonexistent, CA 02314",
        )
        sc_2 = StateScheduledSupervisionContact.new_with_defaults(
            scheduled_supervision_contact_id=1235,
            external_id="c2",
            state_code=StateCode.US_XX.value,
            scheduled_contact_date=datetime.date(2022, 1, 5),
            contact_type=StateScheduledSupervisionContactType.INTERNAL_UNKNOWN,
            status=StateScheduledSupervisionContactStatus.DELETED,
            update_datetime=datetime.datetime(2025, 1, 1),
            contact_method=StateScheduledSupervisionContactMethod.VIRTUAL,
            contact_method_raw_text="PHONE",
            contact_meeting_address=None,
        )
        scheduled_supervision_contacts = [sc_1, sc_2]

        normalized_contacts = (
            self._normalized_scheduled_supervision_contacts_for_calculations(
                scheduled_supervision_contacts=scheduled_supervision_contacts
            )
        )

        sc_1 = StateScheduledSupervisionContact.new_with_defaults(
            scheduled_supervision_contact_id=1234,
            external_id="c1",
            state_code=StateCode.US_XX.value,
            scheduled_contact_date=datetime.date(2018, 3, 6),
            scheduled_contact_datetime=datetime.datetime(2018, 3, 6, 0, 0, 0),
            contact_type=StateScheduledSupervisionContactType.DIRECT,
            status=StateScheduledSupervisionContactStatus.SCHEDULED,
            update_datetime=datetime.datetime(2025, 1, 1),
            contact_method=StateScheduledSupervisionContactMethod.VIRTUAL,
            contact_method_raw_text="PHONE",
            contact_meeting_address="123 Fake St, Nonexistent, CA 02314",
        )
        sc_2 = StateScheduledSupervisionContact.new_with_defaults(
            scheduled_supervision_contact_id=1235,
            external_id="c2",
            state_code=StateCode.US_XX.value,
            scheduled_contact_date=datetime.date(2022, 1, 5),
            scheduled_contact_datetime=datetime.datetime(2022, 1, 5, 0, 0, 0),
            contact_type=StateScheduledSupervisionContactType.INTERNAL_UNKNOWN,
            status=StateScheduledSupervisionContactStatus.DELETED,
            update_datetime=datetime.datetime(2025, 1, 1),
            contact_method=StateScheduledSupervisionContactMethod.VIRTUAL,
            contact_method_raw_text="PHONE",
            contact_meeting_address=None,
        )

        self.assertEqual([sc_1, sc_2], normalized_contacts)

    def test_scheduled_datetime_scheduled_supervision_contacts_normalization(
        self,
    ) -> None:
        self.maxDiff = None
        sc_1 = StateScheduledSupervisionContact.new_with_defaults(
            scheduled_supervision_contact_id=1234,
            external_id="a1",
            state_code=StateCode.US_XX.value,
            scheduled_contact_datetime=datetime.datetime(2025, 6, 1, 0, 0, 0),
            contact_type=StateScheduledSupervisionContactType.DIRECT,
            status=StateScheduledSupervisionContactStatus.SCHEDULED,
            update_datetime=datetime.datetime(2025, 1, 1),
            contact_method=StateScheduledSupervisionContactMethod.VIRTUAL,
            contact_method_raw_text="PHONE",
            contact_meeting_address="some address",
        )
        sc_2 = StateScheduledSupervisionContact.new_with_defaults(
            scheduled_supervision_contact_id=1235,
            external_id="a2",
            state_code=StateCode.US_XX.value,
            scheduled_contact_datetime=datetime.datetime(2024, 12, 31, 0, 0, 0),
            contact_type=StateScheduledSupervisionContactType.INTERNAL_UNKNOWN,
            status=StateScheduledSupervisionContactStatus.SCHEDULED,
            update_datetime=datetime.datetime(2025, 1, 1),
            contact_method=StateScheduledSupervisionContactMethod.VIRTUAL,
            contact_method_raw_text="PHONE",
            contact_meeting_address="some other address",
        )
        scheduled_supervision_contacts = [sc_1, sc_2]

        normalized_contacts = (
            self._normalized_scheduled_supervision_contacts_for_calculations(
                scheduled_supervision_contacts=scheduled_supervision_contacts
            )
        )

        sc_1 = StateScheduledSupervisionContact.new_with_defaults(
            scheduled_supervision_contact_id=1234,
            external_id="a1",
            state_code=StateCode.US_XX.value,
            scheduled_contact_date=datetime.date(2025, 6, 1),
            scheduled_contact_datetime=datetime.datetime(2025, 6, 1, 0, 0, 0),
            contact_type=StateScheduledSupervisionContactType.DIRECT,
            status=StateScheduledSupervisionContactStatus.SCHEDULED,
            update_datetime=datetime.datetime(2025, 1, 1),
            contact_method=StateScheduledSupervisionContactMethod.VIRTUAL,
            contact_method_raw_text="PHONE",
            contact_meeting_address="some address",
        )
        sc_2 = StateScheduledSupervisionContact.new_with_defaults(
            scheduled_supervision_contact_id=1235,
            external_id="a2",
            state_code=StateCode.US_XX.value,
            scheduled_contact_date=datetime.date(2024, 12, 31),
            scheduled_contact_datetime=datetime.datetime(2024, 12, 31, 0, 0, 0),
            contact_type=StateScheduledSupervisionContactType.INTERNAL_UNKNOWN,
            status=StateScheduledSupervisionContactStatus.SCHEDULED,
            update_datetime=datetime.datetime(2025, 1, 1),
            contact_method=StateScheduledSupervisionContactMethod.VIRTUAL,
            contact_method_raw_text="PHONE",
            contact_meeting_address="some other address",
        )

        self.assertEqual([sc_1, sc_2], normalized_contacts)

    def test_scheduled_supervision_contacts_additional_attributes(self) -> None:
        sc = StateScheduledSupervisionContact.new_with_defaults(
            scheduled_supervision_contact_id=1,
            external_id="c1",
            state_code=StateCode.US_XX.value,
            scheduled_contact_date=datetime.date(2018, 3, 6),
            contact_type=StateScheduledSupervisionContactType.DIRECT,
            status=StateScheduledSupervisionContactStatus.SCHEDULED,
            contacting_staff_external_id="EMP1",
            contacting_staff_external_id_type="US_XX_STAFF_ID",
            update_datetime=datetime.datetime(2025, 1, 1),
            contact_method=StateScheduledSupervisionContactMethod.VIRTUAL,
            contact_method_raw_text="PHONE",
            contact_meeting_address="some address",
        )
        scheduled_supervision_contacts = [sc]

        additional_attributes = (
            self._normalized_scheduled_supervision_contacts_additional_attributes(
                scheduled_supervision_contacts=scheduled_supervision_contacts
            )
        )

        expected_attributes: AdditionalAttributesMap = {
            StateScheduledSupervisionContact.__name__: {
                1: {"contacting_staff_id": 10000, "sequence_num": 0}
            }
        }

        self.assertDictEqual(additional_attributes, expected_attributes)

    def test_scheduled_supervision_contacts_additional_attributes_none(self) -> None:
        sc = StateScheduledSupervisionContact.new_with_defaults(
            scheduled_supervision_contact_id=1,
            external_id="c1",
            state_code=StateCode.US_XX.value,
            scheduled_contact_date=datetime.date(2018, 3, 6),
            contact_type=StateScheduledSupervisionContactType.DIRECT,
            status=StateScheduledSupervisionContactStatus.SCHEDULED,
            contacting_staff_external_id=None,
            contacting_staff_external_id_type=None,
            update_datetime=datetime.datetime(2025, 1, 1),
            contact_method=StateScheduledSupervisionContactMethod.VIRTUAL,
            contact_method_raw_text="PHONE",
            contact_meeting_address=None,
        )
        scheduled_supervision_contacts = [sc]

        additional_attributes = (
            self._normalized_scheduled_supervision_contacts_additional_attributes(
                scheduled_supervision_contacts=scheduled_supervision_contacts
            )
        )

        expected_attributes: AdditionalAttributesMap = {
            StateScheduledSupervisionContact.__name__: {
                1: {"contacting_staff_id": None, "sequence_num": 0}
            }
        }

        self.assertDictEqual(additional_attributes, expected_attributes)
