# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests for database_invariant_validator.py."""

import unittest
from typing import List

from mock import MagicMock, create_autospec, patch

from recidiviz.persistence.database.schema.state import schema as state_schema, schema
from recidiviz.persistence.database.schema.state.dao import SessionIsDirtyError
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database_invariant_validator import (
    database_invariant_validator,
)

DATABASE_INVARIANT_VALIDATOR_MODULE = database_invariant_validator.__name__


def validator_that_succeeds(
    _session: Session, _region_code: str, _output_people: List[state_schema.StatePerson]
) -> bool:
    return True


def validator_that_fails(
    _session: Session, _region_code: str, _output_people: List[state_schema.StatePerson]
) -> bool:
    return False


def validator_that_throws(
    _session: Session, _region_code: str, _output_people: List[state_schema.StatePerson]
) -> bool:
    raise ValueError("Validation error")


def validator_that_throws_session_is_dirty(
    _session: Session, _region_code: str, _output_people: List[state_schema.StatePerson]
) -> bool:
    raise SessionIsDirtyError("Dirty session error")


class TestDatabaseInvariantValidator(unittest.TestCase):
    """Tests for database_invariant_validator.py."""

    def setUp(self) -> None:
        self.mock_session = create_autospec(Session)

    @patch(
        f"{DATABASE_INVARIANT_VALIDATOR_MODULE}.get_state_person_database_invariant_validators"
    )
    def test_invariant_validator_no_validations(
        self, mock_get_validators: MagicMock
    ) -> None:
        # Arrange
        mock_get_validators.return_value = []

        # Act
        errors = database_invariant_validator.validate_invariants(
            self.mock_session, "US_XX", schema.StatePerson, []
        )

        # Assert
        self.assertEqual(0, errors)
        mock_get_validators.assert_called_once()

    @patch(
        f"{DATABASE_INVARIANT_VALIDATOR_MODULE}.get_state_person_database_invariant_validators"
    )
    def test_invariant_validator_success(self, mock_get_validators: MagicMock) -> None:
        # Arrange
        mock_get_validators.return_value = [validator_that_succeeds]

        # Act
        errors = database_invariant_validator.validate_invariants(
            self.mock_session, "US_XX", schema.StatePerson, []
        )

        # Assert
        self.assertEqual(0, errors)
        mock_get_validators.assert_called_once()

    @patch(
        f"{DATABASE_INVARIANT_VALIDATOR_MODULE}.get_state_person_database_invariant_validators"
    )
    def test_invariant_validator_error(self, mock_get_validators: MagicMock) -> None:
        # Arrange
        mock_get_validators.return_value = [validator_that_fails]

        # Act
        errors = database_invariant_validator.validate_invariants(
            self.mock_session, "US_XX", schema.StatePerson, []
        )

        # Assert
        self.assertEqual(1, errors)
        mock_get_validators.assert_called_once()

    @patch(
        f"{DATABASE_INVARIANT_VALIDATOR_MODULE}.get_state_person_database_invariant_validators"
    )
    def test_invariant_validator_mixed_errors_and_success(
        self, mock_get_validators: MagicMock
    ) -> None:
        # Arrange
        mock_get_validators.return_value = [
            validator_that_succeeds,
            validator_that_fails,
            validator_that_succeeds,
            validator_that_fails,
        ]

        # Act
        errors = database_invariant_validator.validate_invariants(
            self.mock_session, "US_XX", schema.StatePerson, []
        )

        # Assert
        self.assertEqual(2, errors)
        mock_get_validators.assert_called_once()

    @patch(
        f"{DATABASE_INVARIANT_VALIDATOR_MODULE}.get_state_person_database_invariant_validators"
    )
    def test_invariant_validator_throws(self, mock_get_validators: MagicMock) -> None:
        # Arrange
        mock_get_validators.return_value = [validator_that_throws]

        # Act
        errors = database_invariant_validator.validate_invariants(
            self.mock_session, "US_XX", schema.StatePerson, []
        )

        # Assert
        self.assertEqual(1, errors)
        mock_get_validators.assert_called_once()

    @patch(
        f"{DATABASE_INVARIANT_VALIDATOR_MODULE}.get_state_person_database_invariant_validators"
    )
    def test_invariant_validator_throws_session_is_dirty(
        self, mock_get_validators: MagicMock
    ) -> None:
        # Arrange
        mock_get_validators.return_value = [validator_that_throws_session_is_dirty]

        # Act
        with self.assertRaises(SessionIsDirtyError):
            _ = database_invariant_validator.validate_invariants(
                self.mock_session, "US_XX", schema.StatePerson, []
            )

        # Assert
        mock_get_validators.assert_called_once()

    @patch(
        f"{DATABASE_INVARIANT_VALIDATOR_MODULE}.get_state_person_database_invariant_validators"
    )
    def test_invariant_validator_throws_and_failure(
        self, mock_get_validators: MagicMock
    ) -> None:
        # Arrange
        mock_get_validators.return_value = [validator_that_throws, validator_that_fails]

        # Act
        errors = database_invariant_validator.validate_invariants(
            self.mock_session, "US_XX", schema.StatePerson, []
        )

        # Assert
        self.assertEqual(2, errors)
        mock_get_validators.assert_called_once()

    @patch(
        f"{DATABASE_INVARIANT_VALIDATOR_MODULE}.get_state_staff_database_invariant_validators"
    )
    def test_invariant_validator_state_staff(
        self, mock_get_validators: MagicMock
    ) -> None:
        # Arrange
        mock_get_validators.return_value = [validator_that_succeeds]

        # Act
        errors = database_invariant_validator.validate_invariants(
            self.mock_session, "US_XX", schema.StateStaff, []
        )

        # Assert
        self.assertEqual(0, errors)
        mock_get_validators.assert_called_once()
