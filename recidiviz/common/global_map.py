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

"""A global map that takes values and spits out enums."""

from recidiviz.common.constants import bond
from recidiviz.common.constants import booking
from recidiviz.common.constants import charge
from recidiviz.common.constants import person


_WORLD_MAP = {
    # Bond enum maps
    'bond_type': bond.BOND_TYPE_MAP,
    'bond_status': bond.BOND_STATUS_MAP,

    # Booking enum maps
    'release_reason': booking.RELEASE_REASON_MAP,
    'custody_status': booking.CUSTODY_STATUS_MAP,
    'classification': booking.CLASSIFICATION_MAP,

    # Charge enum maps
    'charge_degree': charge.CHARGE_DEGREE_MAP,
    'charge_class': charge.CHARGE_CLASS_MAP,
    'charge_status': charge.CHARGE_STATUS_MAP,
    'court_type': charge.COURT_TYPE_MAP,

    # Person enum maps
    'gender': person.GENDER_MAP,
    'race': person.RACE_MAP,
    'ethnicity': person.ETHNICITY_MAP,
}

def _get_map(enum_type):
    if enum_type not in _WORLD_MAP:
        raise ValueError(
            'The enum type {0} could not be found'.format(enum_type))
    return _WORLD_MAP[enum_type]

def enum_contains_value(enum_type, value):
    map_to_use = _get_map(enum_type)
    return value.upper() in map_to_use

def convert_value_to_enum(enum_type, value):
    """This function will take a value and convert it into a standard value
    to be used in the database and frontned.

    Args:
        enum_type: The enum type we are trying to convert.
        value: The value we are converting.
    Returns:
        The converted value (constants enum) or raises an error if it can't find
        a conversion
    """
    map_to_use = _get_map(enum_type)
    if value is None:
        value = 'UNKNOWN'
    if value.upper() not in map_to_use:
        raise ValueError(
            'Cannot find {0} in the {1} map'.format(value, enum_type))
    return map_to_use[value.upper()]
