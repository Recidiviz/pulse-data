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
from typing import List, Optional


def get_str_param_value(
        arg_key: str, args, default: Optional[str] = None,
        preserve_case: bool = False) -> Optional[str]:
    """Retrieves URL parameter from request handler params list

    Takes an MultiDict of key/value pairs (URL parameters from the request
    object), finds the key being sought, and returns the first value. The value
    will be stripped of leading or trailing whitespace and converted to lower
    case. If the key is not found, this returns the given default, or None. The
    given default will also be transformed like a found value.

    Args:
        arg_key: (string) Key of the URL parameter being sought
        args: List of URL parameter key/value pairs, as a MultiDict (e.g.,
            [("key", "val"), ("key2", "val2"), ...])
        default: The default value to return if the param name is not found
        preserve_case: Whether to preserve the original string case [False]

    Returns:
        First value for given param_name if found
        Provided default value if not found
        None if no default provided and not found
        """
    return clean_str_param_value(args.get(arg_key, default),
                                 preserve_case=preserve_case)


def get_bool_param_value(arg_key: str, args, default: bool) -> bool:

    str_value = get_str_param_value(arg_key, args)

    if str_value is None:
        return default

    return str_to_bool(str_value)


def str_to_bool(bool_str: str, arg_key=None):
    bool_str_lower = bool_str.lower()
    if bool_str_lower == 'true':
        return True
    if bool_str_lower == 'false':
        return False

    raise ValueError(
        f'Unexpected value {bool_str} for bool param {arg_key}')

def get_str_param_values(arg_key: str, args) -> List[str]:
    """Same as above, but returns all values for a given key"""
    return [clean_str_param_value(val) for val in args.getlist(arg_key)]


def clean_str_param_value(value: str, preserve_case: bool = False) -> str:
    if value:
        if preserve_case:
            return value.strip()
        return value.lower().strip()
    return value
