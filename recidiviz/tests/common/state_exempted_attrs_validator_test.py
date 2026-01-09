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
"""Tests for state_exempted_attrs_validator.py"""
import unittest

import attr

from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.common.state_exempted_attrs_validator import state_exempted_validator


class AttrValidatorsTest(unittest.TestCase):
    """Tests for state_exempted_attrs_validator.py"""

    def test_state_exempted_validator(self) -> None:
        @attr.s
        class _TestClass:
            state_code: str = attr.ib(validator=attr_validators.is_str)
            my_required_str: str = attr.ib(
                default=None,
                validator=state_exempted_validator(
                    attr_validators.is_str,
                    exempted_state_validator=None,
                    exempted_states={StateCode.US_YY},
                ),
            )

        # These crash because the state is not exempted
        with self.assertRaises(TypeError):
            _ = _TestClass(
                state_code="US_XX",
                my_required_str=None,  # type: ignore[arg-type]
            )
        with self.assertRaises(TypeError):
            _ = _TestClass(
                state_code="US_XX",
                my_required_str=True,  # type: ignore[arg-type]
            )

        # These do not crash because the state is exempted
        _ = _TestClass(
            state_code="US_YY",
            my_required_str=None,  # type: ignore[arg-type]
        )
        _ = _TestClass(
            state_code="US_YY",
            my_required_str=True,  # type: ignore[arg-type]
        )

    def test_state_exempted_validator_composite(self) -> None:
        @attr.s
        class _TestClass:
            state_code: str = attr.ib(validator=attr_validators.is_str)
            my_required_str: str = attr.ib(
                default=None,
                validator=attr.validators.and_(
                    attr_validators.is_opt_str,
                    state_exempted_validator(
                        attr_validators.is_str,
                        exempted_state_validator=None,
                        exempted_states={StateCode.US_YY},
                    ),
                ),
            )

        # These crash because the state is not exempted
        with self.assertRaises(TypeError):
            _ = _TestClass(
                state_code="US_XX",
                my_required_str=None,  # type: ignore[arg-type]
            )
        with self.assertRaises(TypeError):
            _ = _TestClass(
                state_code="US_XX",
                my_required_str=True,  # type: ignore[arg-type]
            )

        # This does not crash because the state is exempted
        _ = _TestClass(
            state_code="US_YY",
            my_required_str=None,  # type: ignore[arg-type]
        )

        # This crashes because we're still checking is_opt_str on all states
        with self.assertRaises(TypeError):
            _ = _TestClass(
                state_code="US_YY",
                my_required_str=True,  # type: ignore[arg-type]
            )

    def test_state_exempted_validator_composite_using_list(self) -> None:
        """
        The same test as test_state_exempted_validator_composite, but using
        a list instead of validators.and_
        """

        @attr.s
        class _TestClass:
            state_code: str = attr.ib(validator=attr_validators.is_str)
            my_required_str: str = attr.ib(
                default=None,
                # Mypy didn't like this, but it is allowed in attrs
                validator=[  # type: ignore
                    attr_validators.is_opt_str,
                    state_exempted_validator(
                        attr_validators.is_str,
                        exempted_state_validator=None,
                        exempted_states={StateCode.US_YY},
                    ),
                ],
            )

        # These crash because the state is not exempted
        with self.assertRaises(TypeError):
            _ = _TestClass(
                state_code="US_XX",
                my_required_str=None,  # type: ignore[arg-type]
            )
        with self.assertRaises(TypeError):
            _ = _TestClass(
                state_code="US_XX",
                my_required_str=True,  # type: ignore[arg-type]
            )

        # This does not crash because the state is exempted
        _ = _TestClass(
            state_code="US_YY",
            my_required_str=None,  # type: ignore[arg-type]
        )

        # This crashes because we're still checking is_opt_str on all states
        with self.assertRaises(TypeError):
            _ = _TestClass(
                state_code="US_YY",
                my_required_str=True,  # type: ignore[arg-type]
            )

    def test_state_exempted_validator_with_exempted_state_validator(self) -> None:
        """Test the new exempted_state_validator parameter that provides a fallback
        validator for exempted states.
        """

        @attr.s
        class _TestClass:
            state_code: str = attr.ib(validator=attr_validators.is_str)
            my_field: str | None = attr.ib(
                default=None,
                validator=state_exempted_validator(
                    # Primary validator: requires non-None string
                    attr_validators.is_str,
                    # Fallback for exempted states: allows None
                    exempted_state_validator=attr_validators.is_opt_str,
                    exempted_states={StateCode.US_YY},
                ),
            )

        # Non-exempted state: primary validator runs (requires non-None string)
        with self.assertRaises(TypeError):
            _ = _TestClass(
                state_code="US_XX",
                my_field=None,
            )
        # Non-exempted state: valid string works
        _ = _TestClass(
            state_code="US_XX",
            my_field="valid",
        )

        # Exempted state: fallback validator runs (allows None)
        _ = _TestClass(
            state_code="US_YY",
            my_field=None,
        )
        # Exempted state: valid string also works
        _ = _TestClass(
            state_code="US_YY",
            my_field="valid",
        )
        # Exempted state: invalid type fails (fallback validator still checks type)
        with self.assertRaises(TypeError):
            _ = _TestClass(
                state_code="US_YY",
                my_field=True,  # type: ignore[arg-type]
            )
