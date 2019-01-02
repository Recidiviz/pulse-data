# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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


"""This file defines utility functions common to the recidiviz project"""

from datetime import datetime

import logging
import dateutil.parser as parser


def parse_datetime_string(datetime_string):
    """Converts string describing date to Python datetime object

    Dates are expressed differently in different records,
    typically following one of these patterns:

        "07/2001",
        "12/21/1991",
        "06/14/13",
        "02/15/1990 4:32", etc.

    This function parses several common variants and returns a datetime.

    Args:
        datetime_string: (string) Scraped string containing a datetime

    Returns:
        Python datetime object representing the date parsed from the string, or
        None if string wasn't one of our expected values (this is common,
        often NONE or LIFE are put in for these if life sentence).

    """
    if datetime_string and not datetime_string.isspace():
        try:
            # The year and month in the `default` do not matter. This protects
            # against an esoteric bug: parsing a 2-integer date with a month of
            # February while the local machine date is the 29th or higher (or
            # 30th or higher if the year of the string is a leap year).
            # Without a default day of `01` to fall back on, it falls back to
            # the local machine day, which will fail because February does not
            # have that many days.
            result = parser.parse(datetime_string, default=datetime(2018, 1, 1))
            # result = result.date()
        except ValueError as e:
            logging.debug("Couldn't parse date string '%s'", datetime_string)
            logging.debug(str(e))
            return None

        # If month-only date, manually force date to first of the month.
        if len(datetime_string.split("/")) == 2 \
                or len(datetime_string.split("-")) == 2:
            result = result.replace(day=1)

    else:
        return None

    return result


def parse_date_string(date_string):
    """Same as above parse_datetime_string method; however, returns a Python
    date object instead of a datetime object

    Args:
        date_string: (string) Scraped string containing a date

    Returns:
        Python date object representing the date parsed from the string, or
        None if string wasn't one of our expected values (this is common,
        often NONE or LIFE are put in for these if life sentence).
    """
    return parse_datetime_string(date_string).date()
