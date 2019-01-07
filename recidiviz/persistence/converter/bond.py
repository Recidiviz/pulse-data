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
# ============================================================================
"""Converts an ingest_info proto Bond to a persistence entity."""
from recidiviz.common.constants.bond import BondType, BondStatus
from recidiviz.persistence import entities
from recidiviz.persistence.converter import converter_utils
from recidiviz.persistence.converter.converter_utils import fn, \
    parse_external_id


def convert(proto):
    """Converts an ingest_info proto Bond to a persistence entity."""
    new = entities.Bond()

    new.external_id = fn(parse_external_id, 'bond_id', proto)
    new.amount_dollars, inferred_bond_type = fn(
        converter_utils.parse_bond_amount_and_infer_type, 'amount', proto,
        (None, None)) # default to a pair of Nones instead of None
    # |inferred_bond_type| defaults to CASH unless the bond amount contains
    # information about the type (i.e. NO_BOND or BOND_DENIED)
    new.bond_type = fn(BondType.from_str, 'bond_type', proto,
                       inferred_bond_type)
    # TODO(491): determine what status, if any, bonds with BondType.BOND_DENIED
    #  should have
    new.status = fn(BondStatus.from_str, 'status', proto,
                    BondStatus.ACTIVE if new.amount_dollars else None)

    return new
