# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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

"""Helpers for working with parameters from requests."""

def get_value(arg_key, args, default=None):
    """Retrieves URL parameter from request handler params list

    Takes an MultiDict of key/value pairs (URL parameters from the request
    object), finds the key being sought, and returns the first value. The value
    will be stripped of leading or trailing whitespace and converted to lower
    case. If the key is not found, this returns the given default, or None. The
    given default will also be transformed like a found value.

    Args:
        param_key: (string) Key of the URL parameter being sought
        params: List of URL parameter key/value pairs, as a MultiDict (e.g.,
            [("key", "val"), ("key2", "val2"), ...])
        default: The default value to return if the param name is not found

    Returns:
        First value for given param_name if found
        Provided default value if not found
        None if no default provided and not found
        """
    return clean_value(args.get(arg_key, default))


def get_values(arg_key, args):
    """Same as above, but returns all values for a given key"""
    return [clean_value(val) for val in args.getlist(arg_key)]


def clean_value(value):
    if value:
        return value.lower().strip()
    return value
