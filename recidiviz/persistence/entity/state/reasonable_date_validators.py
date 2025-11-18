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
from recidiviz.common import attr_validators
from recidiviz.common.constants.reasonable_dates import (
    BIRTHDATE_REASONABLE_LOWER_BOUND,
    STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND,
    STANDARD_DATE_FIELD_REASONABLE_UPPER_BOUND,
    STANDARD_DATETIME_FIELD_REASONABLE_LOWER_BOUND,
    STANDARD_DATETIME_FIELD_REASONABLE_UPPER_BOUND,
)

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
