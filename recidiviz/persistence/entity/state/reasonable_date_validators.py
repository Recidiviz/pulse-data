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
"""Defines a set of validators that can be reused to validate that there are reasonable
dates in a variety of date / datetime fields.
"""
from functools import cache
from typing import Any, Callable

import attr

from recidiviz.common import attr_validators
from recidiviz.common.attr_validators import DateAboveUpperBoundError
from recidiviz.common.constants.reasonable_dates import (
    BIRTHDATE_REASONABLE_LOWER_BOUND,
    STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND,
    STANDARD_DATE_FIELD_REASONABLE_UPPER_BOUND,
    STANDARD_DATETIME_FIELD_REASONABLE_LOWER_BOUND,
    STANDARD_DATETIME_FIELD_REASONABLE_UPPER_BOUND,
    US_AZ_PROJECTED_SENTENCE_DATE_REASONABLE_UPPER_BOUND,
    US_TN_PROJECTED_SENTENCE_DATE_REASONABLE_UPPER_BOUND,
    US_UT_PROJECTED_SENTENCE_DATE_REASONABLE_UPPER_BOUND,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.types import T, assert_type

###### Non-optional DATE field validators

# Validates that dates that may be in the past or future fall within a standard
# reasonable bound.
STANDARD_REASONABLE_DATE_VALIDATOR = attr_validators.is_reasonable_date(
    min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND,
    max_allowed_date_exclusive=STANDARD_DATE_FIELD_REASONABLE_UPPER_BOUND,
)
# Validates that dates that must be in the past fall within a standard reasonable
# bound.
STANDARD_REASONABLE_PAST_DATE_VALIDATOR = attr_validators.is_reasonable_past_date(
    min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND
)

###### Optional DATE field validators

# Validates that optional dates that may be in the past or future fall within a
# standard reasonable bound.
STANDARD_REASONABLE_OPT_DATE_VALIDATOR = attr_validators.is_opt_reasonable_date(
    min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND,
    max_allowed_date_exclusive=STANDARD_DATE_FIELD_REASONABLE_UPPER_BOUND,
)

# Validates that optional dates that must be in the past fall within a standard
# reasonable bound.
STANDARD_REASONABLE_OPT_PAST_DATE_VALIDATOR = (
    attr_validators.is_opt_reasonable_past_date(
        min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND
    )
)

# Validates that optional birthdates fall within a reasonable bound.
# Some states store records about people who are quite old and there's no reason not to
# ingest these people so we allow for a lower "reasonable" bound.
REASONABLE_OPT_BIRTHDATE_VALIDATOR = attr_validators.is_opt_reasonable_past_date(
    min_allowed_date_inclusive=BIRTHDATE_REASONABLE_LOWER_BOUND
)

###### Non-optional DATETIME field validators

# Validates that datetimes that must be in the past fall within a standard
# reasonable bound.
STANDARD_REASONABLE_PAST_DATETIME_VALIDATOR = (
    attr_validators.is_reasonable_past_datetime(
        min_allowed_datetime_inclusive=STANDARD_DATETIME_FIELD_REASONABLE_LOWER_BOUND
    )
)

###### Optional DATETIME field validators

# Validates that optional datetimes that may be in the past or future fall within
# a standard reasonable bound.
STANDARD_REASONABLE_OPT_DATETIME_VALIDATOR = attr_validators.is_opt_reasonable_datetime(
    min_allowed_datetime_inclusive=STANDARD_DATETIME_FIELD_REASONABLE_LOWER_BOUND,
    max_allowed_datetime_exclusive=STANDARD_DATETIME_FIELD_REASONABLE_UPPER_BOUND,
)

# Validates that optional datetimes that must be in the past fall within a
# standard reasonable bound.
STANDARD_REASONABLE_OPT_PAST_DATETIME_VALIDATOR = (
    attr_validators.is_opt_reasonable_past_datetime(
        min_allowed_datetime_inclusive=STANDARD_DATETIME_FIELD_REASONABLE_LOWER_BOUND
    )
)


# Some states that track projected sentence parole / release dates in the very far
# future which, in practice, would result in a life sentence but are technically correct
# (e.g. a release date 500 years in the future due to 10 consecutive 50-year sentences).
# For these states, we specify a custom bound for allowed
STATE_TO_VALID_FAR_FUTURE_SENTENCE_DATE_UPPER_BOUND = {
    StateCode.US_AZ: US_AZ_PROJECTED_SENTENCE_DATE_REASONABLE_UPPER_BOUND,
    StateCode.US_TN: US_TN_PROJECTED_SENTENCE_DATE_REASONABLE_UPPER_BOUND,
    StateCode.US_UT: US_UT_PROJECTED_SENTENCE_DATE_REASONABLE_UPPER_BOUND,
}


@cache
def _reasonable_projected_sentence_date_validator_for_state(
    state_code: StateCode,
) -> Callable[[Any, attr.Attribute, T], None]:
    """Returns a cached validator for projected sentence dates specific to a state."""
    state_specific_upper_bound = (
        STATE_TO_VALID_FAR_FUTURE_SENTENCE_DATE_UPPER_BOUND.get(state_code)
        or STANDARD_DATE_FIELD_REASONABLE_UPPER_BOUND
    )
    return attr_validators.is_opt_reasonable_date(
        min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND,
        max_allowed_date_exclusive=state_specific_upper_bound,
    )


def reasonable_projected_sentence_date_validator(
    # TODO(#38799): Remove this arg once all state exemptions have been removed from
    #  sentence projected date fields.
    exempted_states: (set[StateCode] | None) = None,
) -> Callable[[Any, attr.Attribute, T], None]:
    """Returns a validator for projected sentence dates that handles state-specific
    bounds.

    For states with known far-future legitimate dates, applies custom upper bounds. For
    exempted states (those with bad data being fixed), only validates that the value is
    an optional date without bound checking.
    """

    def _validator(instance: Any, attribute: attr.Attribute, value: T) -> None:
        if not hasattr(instance, "state_code"):
            raise ValueError(f"Class [{type(instance)}] does not have state_code")

        state_code = StateCode(assert_type(getattr(instance, "state_code"), str))

        if exempted_states and state_code in exempted_states:
            attr_validators.is_opt_date(instance, attribute, value)
            return

        try:
            _reasonable_projected_sentence_date_validator_for_state(state_code)(
                instance, attribute, value
            )
        except DateAboveUpperBoundError as e:
            if state_code in STATE_TO_VALID_FAR_FUTURE_SENTENCE_DATE_UPPER_BOUND:
                raise e

            # If no custom bound is set, add a hint in case it makes sense to set a
            #  custom upper bound.
            raise DateAboveUpperBoundError(
                f"{e}\n\n"
                f"If this far-future date is legitimate for {state_code.value} "
                f"(e.g., many consecutive, non-life sentences), add an entry to "
                f"STATE_TO_VALID_FAR_FUTURE_SENTENCE_DATE_UPPER_BOUND in "
                f"recidiviz/persistence/entity/state/reasonable_date_validators.py"
            ) from e

    return _validator
