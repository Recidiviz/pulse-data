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

"""Helpers for working with parameters from requests."""


def get_param(param_key, params, default=None):
    """Retrieves URL parameter from request handler params list

    Takes an iterable list of key/value pairs (URL parameters from the request
    object), finds the key being sought, and returns its value. The value will
    be stripped of leading or trailing whitespace and converted to lower case.
    If the key is not found, this returns the given default, or None. The
    given default will not be transformed like a found value.

    Args:
        param_key: (string) Key of the URL parameter being sought
        params: List of URL parameter key/value pairs, in tuples (e.g.,
            [("key", "val"), ("key2", "val2"), ...])
        default: The default value to return if the param name is not found

    Returns:
        Value for given param_name if found
        Provided default value if not found
        None if no default provided and not found
    """
    for key, val in params:
        if key == param_key:
            return clean_value(val)

    return default


def get_arg(arg_key, args, default=None):
    """Same as above, but for arguments in Flask form"""
    return clean_value(args.get(arg_key, default))


def get_arg_list(arg_key, args):
    """Same as above, but allows for multiple arguments for a given key"""
    return [clean_value(val) for val in args.getlist(arg_key)]


def clean_value(value):
    if value:
        return value.lower().strip()
    return value
