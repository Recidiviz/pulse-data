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

"""Utilities for working with fixture data in ingest unit testing."""


import json
import os


def as_string(region_directory, filename):
    """Returns the contents of the given fixture file as a string.

    Assumes the fixture file has the given name (with extension included), and
    is located in /recidiviz/tests/ingest/{region_directory}/fixtures.

    Args:
        region_directory: (string) the region's directory
        filename: (string) the name of the file, including extension

    Returns:
        The contents of the fixture file as a string
    """
    return open(os.path.join(os.path.dirname(__file__),
                             region_directory,
                             'fixtures',
                             filename)).read()


def as_dict(region_directory, filename):
    """Returns the contents of the given fixture file as a dictionary.

    Assumes the fixture file has the given name (with extension included), and
    that the file is json, and is located in
    /recidiviz/tests/ingest/{region_directory}/fixtures.

    Args:
        region_directory: (string) the region's directory
        filename: (string) the name of the file, including extension

    Returns:
        The contents of the fixture file as a dict deserialized from json
    """
    contents = as_string(region_directory, filename)
    return json.loads(contents)
