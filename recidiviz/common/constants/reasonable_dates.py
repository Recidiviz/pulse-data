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
"""Date constants for reasonable date bounds"""
import datetime

# Standard lower bound value for date fields in our schema. Dates before this date
# likely are erroneous values (e.g. typos in data) or data that is so old that it is too
# unreliable to ingest. This date was picked carefully to not include 1900-01-01, which
# is a common placeholder date used in place of NULL.
STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND = datetime.date.fromisoformat("1900-01-02")

# Standard upper bound value for maximum date projection fields in our schema.
# Dates before this date likely are erroneous values (e.g. typos in data).
STANDARD_DATE_FIELD_REASONABLE_UPPER_BOUND = datetime.date.fromisoformat("2300-01-01")

# Upper bound value used for sentence projected max dates
# TODO(#53535): Rename this / set more varied bounds where we think there are legit
#  dates that fall past this bound.
MAX_DATE_FIELD_REASONABLE_UPPER_BOUND = datetime.date.fromisoformat("2500-01-01")

# Standard lower bound value for person birthdate values in our schema.
# Some states store records about people who are quite old and there's no reason not to
# ingest these people.
BIRTHDATE_REASONABLE_LOWER_BOUND = datetime.date.fromisoformat("1700-01-01")

# Standard lower bound value for datetime fields in our schema. Dates before this date
# likely are erroneous values (e.g. typos in data) or data that is so old that it is too
# unreliable to ingest.
STANDARD_DATETIME_FIELD_REASONABLE_LOWER_BOUND = datetime.datetime.fromisoformat(
    STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND.isoformat()
)
# Standard upper bound value for datetime fields in our schema. Dates before this date
# likely are erroneous values (e.g. typos in data) or data that is so old that it is too
# unreliable to ingest.
STANDARD_DATETIME_FIELD_REASONABLE_UPPER_BOUND = datetime.datetime.fromisoformat(
    STANDARD_DATE_FIELD_REASONABLE_UPPER_BOUND.isoformat()
)
