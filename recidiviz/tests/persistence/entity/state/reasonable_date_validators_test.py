# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests for reasonable_date_validators.py"""
import datetime
import re
import unittest
from unittest import mock

import attr

from recidiviz.common.attr_validators import (
    DateAboveUpperBoundError,
    DateBelowLowerBoundError,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state.reasonable_date_validators import (
    reasonable_projected_sentence_date_validator,
)


class TestReasonableProjectedSentenceDateValidator(unittest.TestCase):
    """Tests for the reasonable_projected_sentence_date_validator() function."""

    def test_valid_dates_for_standard_states(self) -> None:
        """Test that dates within standard bounds pass validation for standard states."""

        @attr.define
        class _TestEntity(Entity):
            state_code: str = attr.ib()
            projected_date: datetime.date | None = attr.ib(
                validator=reasonable_projected_sentence_date_validator()
            )

        # Test US_CA (standard state without custom bounds)
        # Valid dates within standard bounds (1900-01-02 to 2300-01-01)
        _ = _TestEntity(
            state_code=StateCode.US_XX.value, projected_date=datetime.date(1900, 1, 2)
        )  # Lower bound
        _ = _TestEntity(
            state_code=StateCode.US_XX.value, projected_date=datetime.date(2000, 1, 1)
        )  # Mid-range
        _ = _TestEntity(
            state_code=StateCode.US_XX.value, projected_date=datetime.date(2299, 12, 31)
        )  # Just below upper bound

    @mock.patch.dict(
        "recidiviz.persistence.entity.state.reasonable_date_validators.STATE_TO_VALID_FAR_FUTURE_SENTENCE_DATE_UPPER_BOUND",
        {
            StateCode.US_YY: datetime.date(2700, 1, 1),
            StateCode.US_WW: datetime.date(2500, 1, 1),
        },
    )
    def test_valid_far_future_dates_for_special_states(self) -> None:
        """Test that far-future dates are valid for states with custom upper bounds."""

        @attr.define
        class _TestEntity(Entity):
            state_code: str = attr.ib()
            projected_date: datetime.date | None = attr.ib(
                validator=reasonable_projected_sentence_date_validator()
            )

        # US_YY allows dates up to 2700-01-01
        _ = _TestEntity(
            state_code=StateCode.US_YY.value, projected_date=datetime.date(2500, 1, 1)
        )  # Beyond standard bound
        _ = _TestEntity(
            state_code=StateCode.US_YY.value, projected_date=datetime.date(2699, 12, 31)
        )  # Just below YY upper bound

        # US_WW allows dates up to 2500-01-01
        _ = _TestEntity(
            state_code=StateCode.US_WW.value, projected_date=datetime.date(2400, 1, 1)
        )  # Beyond standard bound
        _ = _TestEntity(
            state_code=StateCode.US_WW.value, projected_date=datetime.date(2499, 12, 31)
        )  # Just below WW upper bound

    def test_invalid_dates_below_lower_bound(self) -> None:
        """Test that dates before the lower bound raise DateBelowLowerBoundError."""

        @attr.define
        class _TestEntity(Entity):
            state_code: str = attr.ib()
            projected_date: datetime.date | None = attr.ib(
                validator=reasonable_projected_sentence_date_validator()
            )

        # Test date before 1900-01-02
        with self.assertRaisesRegex(
            DateBelowLowerBoundError,
            re.escape(
                "Found [projected_date] value on class [_TestEntity] with "
                "value [1900-01-01] which is less than [1900-01-02], the (inclusive) min "
                "allowed date."
            ),
        ):
            _ = _TestEntity(
                state_code=StateCode.US_XX.value,
                projected_date=datetime.date(1900, 1, 1),
            )

    def test_invalid_dates_above_upper_bound_standard_state(self) -> None:
        """Test that dates beyond upper bound for standard states raise error with helpful hint."""

        @attr.define
        class _TestEntity(Entity):
            state_code: str = attr.ib()
            projected_date: datetime.date | None = attr.ib(
                validator=reasonable_projected_sentence_date_validator()
            )

        # Test date at or beyond 2300-01-01 for a standard state (US_XX)
        with self.assertRaisesRegex(
            DateAboveUpperBoundError,
            re.escape(
                "Found [projected_date] value on class [_TestEntity] with "
                "value [2300-01-01] which is greater than or equal to [2300-01-01], the "
                "(exclusive) max allowed date.\n\n"
                "If this far-future date is legitimate for US_XX "
                "(e.g., many consecutive, non-life sentences), add an entry to "
                "STATE_TO_VALID_FAR_FUTURE_SENTENCE_DATE_UPPER_BOUND in "
                "recidiviz/persistence/entity/state/reasonable_date_validators.py"
            ),
        ):
            _ = _TestEntity(
                state_code=StateCode.US_XX.value,
                projected_date=datetime.date(2300, 1, 1),
            )

    @mock.patch.dict(
        "recidiviz.persistence.entity.state.reasonable_date_validators.STATE_TO_VALID_FAR_FUTURE_SENTENCE_DATE_UPPER_BOUND",
        {
            StateCode.US_YY: datetime.date(2700, 1, 1),
            StateCode.US_WW: datetime.date(2500, 1, 1),
        },
    )
    def test_invalid_dates_above_upper_bound_special_state(self) -> None:
        """Test that dates beyond custom upper bounds raise error without hint."""

        @attr.define
        class _TestEntity(Entity):
            state_code: str = attr.ib()
            projected_date: datetime.date | None = attr.ib(
                validator=reasonable_projected_sentence_date_validator()
            )

        # US_YY has custom bound of 2700-01-01, so dates >= 2700-01-01 should fail
        with self.assertRaisesRegex(
            DateAboveUpperBoundError,
            re.escape(
                "Found [projected_date] value on class [_TestEntity] with "
                "value [2700-01-01] which is greater than or equal to [2700-01-01], the "
                "(exclusive) max allowed date."
            ),
        ):
            _ = _TestEntity(
                state_code=StateCode.US_YY.value,
                projected_date=datetime.date(2700, 1, 1),
            )

        # US_WW has custom bound of 2500-01-01
        with self.assertRaisesRegex(
            DateAboveUpperBoundError,
            re.escape(
                "Found [projected_date] value on class [_TestEntity] with "
                "value [2500-01-01] which is greater than or equal to [2500-01-01], the "
                "(exclusive) max allowed date."
            ),
        ):
            _ = _TestEntity(
                state_code=StateCode.US_WW.value,
                projected_date=datetime.date(2500, 1, 1),
            )

    def test_exempted_states(self) -> None:
        """Test that exempted states only validate type, not bounds."""

        @attr.define
        class _TestEntity(Entity):
            state_code: str = attr.ib()
            projected_date: datetime.date | None = attr.ib(
                validator=reasonable_projected_sentence_date_validator(
                    exempted_states={
                        StateCode.US_YY,
                    }
                )
            )

        # Exempted state should accept dates outside normal bounds
        _ = _TestEntity(
            state_code=StateCode.US_YY.value, projected_date=datetime.date(1700, 1, 1)
        )  # Before lower bound
        _ = _TestEntity(
            state_code=StateCode.US_YY.value, projected_date=datetime.date(3000, 1, 1)
        )  # Way beyond upper bound

        # Non-exempted state should still validate bounds
        with self.assertRaises(DateAboveUpperBoundError):
            _ = _TestEntity(
                state_code=StateCode.US_XX.value,
                projected_date=datetime.date(2300, 1, 1),
            )

    def test_none_values(self) -> None:
        """Test that None values are allowed (optional field)."""

        @attr.define
        class _TestEntity(Entity):
            state_code: str = attr.ib()
            projected_date: datetime.date | None = attr.ib(
                validator=reasonable_projected_sentence_date_validator()
            )

        # None should be valid for any state
        _ = _TestEntity(state_code=StateCode.US_XX.value, projected_date=None)

    def test_entity_without_state_code(self) -> None:
        """Test that ValueError is raised if entity doesn't have state_code attribute."""

        @attr.define
        class _TestEntityWithoutStateCode(Entity):
            # No state_code field
            projected_date: datetime.date | None = attr.ib(
                validator=reasonable_projected_sentence_date_validator()
            )

        with self.assertRaisesRegex(
            ValueError,
            r"Class \[.*_TestEntityWithoutStateCode.*\] does not have state_code",
        ):
            _ = _TestEntityWithoutStateCode(projected_date=datetime.date(2000, 1, 1))
