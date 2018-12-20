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
from recidiviz.persistence.converter.converter_utils import fn, normalize, \
    parse_dollars


def convert(proto):
    """Converts an ingest_info proto Bond to a persistence entity."""
    new = entities.Bond()

    new.external_id = fn(normalize, 'bond_id', proto)
    new.amount_dollars = fn(parse_dollars, 'amount', proto)
    new.bond_type = fn(BondType.from_str, 'bond_type', proto)
    new.status = fn(BondStatus.from_str, 'status', proto, BondStatus.POSTED)

    return new
