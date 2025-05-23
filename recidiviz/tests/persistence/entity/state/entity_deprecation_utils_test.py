# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests for entity_deprecation_utils.py"""
import datetime
import unittest

from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.persistence.entity.state import entities, entity_deprecation_utils


class TestValidateDeprecatedEntityFieldForStates(unittest.TestCase):
    """Tests the functionality of the validate_deprecated_entity_field_for_states
    function."""

    def test_validate_deprecated_entity_field_for_states(self) -> None:
        """Tests that the validate_deprecated_entity_field_for_states function will
        raise an error if an entity is instantiated with a deprecated field for the
        given state."""

        ip = entities.StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=datetime.date(2020, 1, 1),
        )

        with self.assertRaises(ValueError) as e:
            entity_deprecation_utils.validate_deprecated_entity_field_for_states(
                entity=ip,
                field_name="incarceration_type",
                deprecated_state_codes=["US_XX", "US_YY"],
            )

            self.assertEqual(
                "The [incarceration_type] field is deprecated for state_code: [US_XX]. "
                "This field should not be populated.",
                e,
            )

    def test_validate_deprecated_entity_field_for_states_relationship(self) -> None:
        """Tests that the validate_deprecated_entity_field_for_states function will
        raise an error if an entity is instantiated with a deprecated field for the
        given state."""

        ip = entities.StatePerson.new_with_defaults(
            state_code="US_XX",
            supervision_violations=[
                entities.StateSupervisionViolation.new_with_defaults(
                    state_code="US_XX",
                    external_id="sv1",
                )
            ],
        )

        with self.assertRaises(ValueError) as e:
            entity_deprecation_utils.validate_deprecated_entity_field_for_states(
                entity=ip,
                field_name="supervision_violations",
                deprecated_state_codes=["US_XX", "US_YY"],
            )

            self.assertEqual(
                "The [supervision_violations] relationship "
                "is deprecated for state_code: [US_XX]. This relationship "
                "should not be populated.",
                e,
            )

    def test_validate_deprecated_entity_field_for_states_not_deprecated(self) -> None:
        """Tests that the validate_deprecated_entity_field_for_states function will
        not raise an error if an entity is instantiated with a field that is not
        deprecated for the given state."""

        ip = entities.StateIncarcerationPeriod.new_with_defaults(
            state_code="US_WW",
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=datetime.date(2020, 1, 1),
        )

        # Assert no error raised
        entity_deprecation_utils.validate_deprecated_entity_field_for_states(
            entity=ip,
            field_name="incarceration_type",
            deprecated_state_codes=["US_XX", "US_YY"],
        )

    def test_validate_deprecated_entity_field_for_states_deprecated_unset(self) -> None:
        """Tests that the validate_deprecated_entity_field_for_states function will
        not raise an error if a field is deprecated for the given state, but the
        field is empty on the entity."""
        ip = entities.StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            external_id="ip1",
            incarceration_type=None,
            admission_date=datetime.date(2020, 1, 1),
        )

        # Assert no error raised
        entity_deprecation_utils.validate_deprecated_entity_field_for_states(
            entity=ip,
            field_name="incarceration_type",
            deprecated_state_codes=["US_XX", "US_YY"],
        )


class TestValidateDeprecatedEntityForStates(unittest.TestCase):
    """Tests the functionality of the validate_deprecated_entity_for_states
    function."""

    def test_validate_deprecated_entity_field_for_states(self) -> None:
        """Tests that the validate_deprecated_entity_for_states function will
        raise an error if a deprecated entity is instantiated for the given state."""
        ip = entities.StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            external_id="ip1",
            admission_date=datetime.date(2020, 1, 1),
        )

        with self.assertRaises(ValueError) as e:
            entity_deprecation_utils.validate_deprecated_entity_for_states(
                entity=ip,
                deprecated_state_codes=["US_XX", "US_YY"],
            )

            self.assertEqual(
                "The [StateIncarcerationPeriod] entity is deprecated for state_code: "
                "[US_XX]. This entity should not be instantiated.",
                e,
            )

    def test_validate_deprecated_entity_for_states_not_deprecated(self) -> None:
        """Tests that the validate_deprecated_entity_for_states function will
        not raise an error if an entity is instantiated that is not deprecated for
        the given state."""
        sp = entities.StateSupervisionPeriod.new_with_defaults(
            state_code="US_WW", external_id="sp1", start_date=datetime.date(2020, 1, 1)
        )

        # Assert no error raised
        entity_deprecation_utils.validate_deprecated_entity_for_states(
            entity=sp,
            deprecated_state_codes=["US_XX", "US_YY"],
        )
