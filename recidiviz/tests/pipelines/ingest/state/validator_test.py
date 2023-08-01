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
"""Unit tests to test validations for ingested entities."""
import unittest

from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state.entities import (
    StatePersonExternalId,
    StateStaffExternalId,
)
from recidiviz.pipelines.ingest.state.validator import validate_root_entity


class TestEntityValidations(unittest.TestCase):
    """Tests validations functions"""

    def test_valid_external_id_state_staff_entities(self) -> None:
        entities = [
            state_entities.StateStaff(
                state_code="US_XX",
                external_ids=[
                    StateStaffExternalId(
                        external_id="100",
                        state_code="US_XX",
                        id_type="US_XX_EMPLOYEE",
                    ),
                    StateStaffExternalId(
                        external_id="200",
                        state_code="US_XX",
                        id_type="US_EMP",
                    ),
                ],
            )
        ]

        for entity in entities:
            validate_root_entity(entity)

    def test_missing_external_id_state_staff_entities(self) -> None:
        entities = [state_entities.StateStaff(state_code="US_XX", external_ids=[])]

        with self.assertRaisesRegex(
            ValueError,
            r"^Found \[<class 'recidiviz.persistence.entity.state.entities.StateStaff'>\] with id \[None\] missing an "
            r"external_id:",
        ):
            for entity in entities:
                validate_root_entity(entity)

    def test_two_external_ids_same_type_state_staff_entities(self) -> None:
        entities = [
            state_entities.StateStaff(
                state_code="US_XX",
                external_ids=[
                    StateStaffExternalId(
                        external_id="100",
                        state_code="US_XX",
                        id_type="US_XX_EMPLOYEE",
                    ),
                    StateStaffExternalId(
                        external_id="200",
                        state_code="US_XX",
                        id_type="US_XX_EMPLOYEE",
                    ),
                ],
            )
        ]

        with self.assertRaisesRegex(
            ValueError,
            r"Duplicate external id types for \[<class 'recidiviz.persistence.entity.state.entities.StateStaff'>\] with id "
            r"\[None\]: US_XX_EMPLOYEE",
        ):
            for entity in entities:
                validate_root_entity(entity)

    def test_two_external_ids_exact_same_state_staff_entities(self) -> None:
        entities = [
            state_entities.StateStaff(
                state_code="US_XX",
                external_ids=[
                    StateStaffExternalId(
                        external_id="100",
                        state_code="US_XX",
                        id_type="US_XX_EMPLOYEE",
                    ),
                    StateStaffExternalId(
                        external_id="100",
                        state_code="US_XX",
                        id_type="US_XX_EMPLOYEE",
                    ),
                ],
            )
        ]

        with self.assertRaisesRegex(
            ValueError,
            r"Duplicate external id types for \[<class 'recidiviz.persistence.entity.state.entities.StateStaff'>\] with id "
            r"\[None\]: US_XX_EMPLOYEE",
        ):
            for entity in entities:
                validate_root_entity(entity)

    def test_valid_external_id_state_person_entities(self) -> None:
        entities = [
            state_entities.StatePerson(
                state_code="US_XX",
                external_ids=[
                    StatePersonExternalId(
                        external_id="100",
                        state_code="US_XX",
                        id_type="US_XX_EMPLOYEE",
                    ),
                ],
            )
        ]

        for entity in entities:
            validate_root_entity(entity)

    def test_missing_external_id_state_person_entities(self) -> None:
        entities = [state_entities.StatePerson(state_code="US_XX", external_ids=[])]

        with self.assertRaisesRegex(
            ValueError,
            r"^Found \[<class 'recidiviz.persistence.entity.state.entities.StatePerson'>\] with id \[None\] missing an "
            r"external_id:",
        ):
            for entity in entities:
                validate_root_entity(entity)

    def test_two_external_ids_same_type_state_person_entities(self) -> None:
        entities = [
            state_entities.StatePerson(
                state_code="US_XX",
                external_ids=[
                    StatePersonExternalId(
                        external_id="100",
                        state_code="US_XX",
                        id_type="US_XX_EMPLOYEE",
                    ),
                    StatePersonExternalId(
                        external_id="200",
                        state_code="US_XX",
                        id_type="US_XX_EMPLOYEE",
                    ),
                ],
            )
        ]

        with self.assertRaisesRegex(
            ValueError,
            r"Duplicate external id types for \[<class 'recidiviz.persistence.entity.state.entities.StatePerson'>\] with id "
            r"\[None\]: US_XX_EMPLOYEE",
        ):
            for entity in entities:
                validate_root_entity(entity)

    def test_two_external_ids_exact_same_state_person_entities(self) -> None:
        entities = [
            state_entities.StatePerson(
                state_code="US_XX",
                external_ids=[
                    StatePersonExternalId(
                        external_id="100",
                        state_code="US_XX",
                        id_type="US_XX_EMPLOYEE",
                    ),
                    StatePersonExternalId(
                        external_id="100",
                        state_code="US_XX",
                        id_type="US_XX_EMPLOYEE",
                    ),
                ],
            )
        ]

        with self.assertRaisesRegex(
            ValueError,
            r"Duplicate external id types for \[<class 'recidiviz.persistence.entity.state.entities.StatePerson'>\] with id "
            r"\[None\]: US_XX_EMPLOYEE",
        ):
            for entity in entities:
                validate_root_entity(entity)
