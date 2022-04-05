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
# ============================================================================
"""Utils for parsing dates."""
import re


def munge_date_string(date_string: str) -> str:
    """Tranforms the input date string so it can be parsed, if necessary"""
    return re.sub(
        r'^((?P<year>\d+)y)?\s*((?P<month>\d+)m)?\s*((?P<day>\d+)d)?$',
        _date_component_match, date_string, flags=re.IGNORECASE)


def _date_component_match(match) -> str:
    components = []

    if match.group('year'):
        components.append('{year}year'.format(year=match.group('year')))
    if match.group('month'):
        components.append('{month}month'.format(month=match.group('month')))
    if match.group('day'):
        components.append('{day}day'.format(day=match.group('day')))

    return ' '.join(components)
