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

from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactStatus,
    StateSupervisionContactType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.normalized_entities_utils import (
    AdditionalAttributesMap,
)
from recidiviz.persistence.entity.state.entities import StateSupervisionContact
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.supervision_contact_normalization_manager import (
    SupervisionContactNormalizationManager,
)
from recidiviz.pipelines.utils.execution_utils import (
    build_staff_external_id_to_staff_id_map,
)
from recidiviz.tests.pipelines.ingest.state.normalization.normalization_managers.assessment_normalization_manager_test import (
    STATE_PERSON_TO_STATE_STAFF_LIST,
)


class TestPrepareSupervisionContactsForCalculations(unittest.TestCase):
    """State-agnostic tests for pre-processing that happens to all supervision contacts
    regardless of state."""

    def setUp(self) -> None:
        self.state_code = "US_XX"

    def _normalized_supervision_contacts_for_calculations(
        self, supervision_contacts: List[StateSupervisionContact]
    ) -> List[StateSupervisionContact]:
        entity_normalization_manager = SupervisionContactNormalizationManager(
            supervision_contacts=supervision_contacts,
            staff_external_id_to_staff_id=build_staff_external_id_to_staff_id_map(
                STATE_PERSON_TO_STATE_STAFF_LIST
            ),
        )

        (
            processed_supervision_contacts,
            _,
        ) = (
            entity_normalization_manager.normalized_supervision_contacts_and_additional_attributes()
        )

        return processed_supervision_contacts

    def _normalized_supervision_contacts_additional_attributes(
        self, supervision_contacts: List[StateSupervisionContact]
    ) -> AdditionalAttributesMap:
        entity_normalization_manager = SupervisionContactNormalizationManager(
            supervision_contacts=supervision_contacts,
            staff_external_id_to_staff_id=build_staff_external_id_to_staff_id_map(
                STATE_PERSON_TO_STATE_STAFF_LIST
            ),
        )

        (
            _,
            additional_attributes,
        ) = (
            entity_normalization_manager.normalized_supervision_contacts_and_additional_attributes()
        )

        return additional_attributes

    def test_simple_supervision_contacts_normalization(self) -> None:
        self.maxDiff = None
        sc_1 = StateSupervisionContact.new_with_defaults(
            supervision_contact_id=1234,
            external_id="c1",
            state_code=StateCode.US_XX.value,
            contact_date=datetime.date(2018, 3, 6),
            contact_type=StateSupervisionContactType.DIRECT,
            status=StateSupervisionContactStatus.COMPLETED,
        )
        sc_2 = StateSupervisionContact.new_with_defaults(
            supervision_contact_id=1234,
            external_id="c2",
            state_code=StateCode.US_XX.value,
            contact_date=datetime.date(2022, 1, 5),
            contact_type=StateSupervisionContactType.INTERNAL_UNKNOWN,
            status=StateSupervisionContactStatus.ATTEMPTED,
        )
        supervision_contacts = [sc_1, sc_2]

        normalized_contacts = self._normalized_supervision_contacts_for_calculations(
            supervision_contacts=supervision_contacts
        )

        sc_1 = StateSupervisionContact.new_with_defaults(
            supervision_contact_id=1234,
            external_id="c1",
            state_code=StateCode.US_XX.value,
            contact_date=datetime.date(2018, 3, 6),
            contact_datetime=datetime.datetime(2018, 3, 6, 0, 0, 0),
            contact_type=StateSupervisionContactType.DIRECT,
            status=StateSupervisionContactStatus.COMPLETED,
        )
        sc_2 = StateSupervisionContact.new_with_defaults(
            supervision_contact_id=1234,
            external_id="c2",
            state_code=StateCode.US_XX.value,
            contact_date=datetime.date(2022, 1, 5),
            contact_datetime=datetime.datetime(2022, 1, 5, 0, 0, 0),
            contact_type=StateSupervisionContactType.INTERNAL_UNKNOWN,
            status=StateSupervisionContactStatus.ATTEMPTED,
        )

        self.assertEqual([sc_1, sc_2], normalized_contacts)

    def test_supervision_contacts_additional_attributes(self) -> None:
        sc = StateSupervisionContact.new_with_defaults(
            supervision_contact_id=1,
            external_id="c1",
            state_code=StateCode.US_XX.value,
            contact_date=datetime.date(2018, 3, 6),
            contact_type=StateSupervisionContactType.DIRECT,
            status=StateSupervisionContactStatus.COMPLETED,
            contacting_staff_external_id="EMP1",
            contacting_staff_external_id_type="US_XX_STAFF_ID",
        )
        supervision_contacts = [sc]

        additional_attributes = (
            self._normalized_supervision_contacts_additional_attributes(
                supervision_contacts=supervision_contacts
            )
        )

        expected_attributes: AdditionalAttributesMap = {
            StateSupervisionContact.__name__: {1: {"contacting_staff_id": 10000}}
        }

        self.assertDictEqual(additional_attributes, expected_attributes)

    def test_supervision_contacts_additional_attributes_none(self) -> None:
        sc = StateSupervisionContact.new_with_defaults(
            supervision_contact_id=1,
            external_id="c1",
            state_code=StateCode.US_XX.value,
            contact_date=datetime.date(2018, 3, 6),
            contact_type=StateSupervisionContactType.DIRECT,
            status=StateSupervisionContactStatus.COMPLETED,
            contacting_staff_external_id=None,
            contacting_staff_external_id_type=None,
        )
        supervision_contacts = [sc]

        additional_attributes = (
            self._normalized_supervision_contacts_additional_attributes(
                supervision_contacts=supervision_contacts
            )
        )

        expected_attributes: AdditionalAttributesMap = {
            StateSupervisionContact.__name__: {1: {"contacting_staff_id": None}}
        }

        self.assertDictEqual(additional_attributes, expected_attributes)
