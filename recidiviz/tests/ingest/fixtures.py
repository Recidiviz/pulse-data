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

"""Utilities for working with fixture data in ingest unit testing."""

import os
import inspect
import json
import xml
import html5lib
from lxml import html


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
    subdir = 'scrape/regions'
    if 'vendor' in region_directory:
        subdir = 'scrape'
    elif any(d in region_directory
             for d in ('aggregate', 'extractor', 'direct')):
        subdir = ''

    with open(os.path.join(os.path.dirname(__file__),
                           subdir,
                           region_directory,
                           'fixtures',
                           filename)) as fixture_file:
        string = fixture_file.read()
    return string


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


# TODO(#647): Refactor this to be usable outside the `ingest` module
def as_filepath(filename: str, subdir: str = 'fixtures') -> str:
    """Returns the filepath for the fixture file.

    Assumes the |filename| is in the |subdir| subdirectory relative to the
    caller's directory.
    """
    frame = inspect.stack()[1]
    module = inspect.getmodule(frame[0])
    caller_filepath = module.__file__

    return os.path.abspath(
        os.path.join(caller_filepath, '..', subdir, filename))


def as_html(region_directory, filename):
    content_string = as_string(region_directory, filename)
    return html.fromstring(content_string)


def as_html5(region_directory, filename):
    content_string = as_string(region_directory, filename)
    html5_etree = html5lib.parse(content_string)
    html5_string = xml.etree.ElementTree.tostring(html5_etree)
    return html.fromstring(html5_string)
