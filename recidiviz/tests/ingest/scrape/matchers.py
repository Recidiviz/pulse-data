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

"""Matchers for convenient unit testing in ingest."""


import json

import callee


class DeserializedJson(callee.Matcher):
    """An argument Matcher which can match serialized json against a
    deserialized object for comparison by deserializing the json.

    This is useful here because we pass around serialized json as strings that
    could have its fields in any order within the string, and we want to match
    it against the actual object that the json represents.
    """

    def __init__(self, comparison_object):
        self.comparison = comparison_object

    def match(self, value):
        return json.loads(value) == self.comparison
