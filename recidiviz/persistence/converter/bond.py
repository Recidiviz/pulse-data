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
from recidiviz.common.common_utils import normalize
from recidiviz.common.constants.bond import BondStatus, BondType
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.persistence import entities
from recidiviz.persistence.converter import converter_utils
from recidiviz.persistence.converter.converter_utils import (fn,
                                                             parse_external_id)


def convert(proto, metadata: IngestMetadata) -> entities.Bond:
    """Converts an ingest_info proto Bond to a persistence entity."""
    new = entities.Bond.builder()

    new.external_id = fn(parse_external_id, 'bond_id', proto)
    new.bond_agent = fn(normalize, 'bond_agent', proto)
    new.amount_dollars, new.bond_type, new.status = \
        converter_utils.parse_bond_amount_type_and_status(
            fn(normalize, 'amount', proto),
            provided_bond_type=fn(
                BondType.parse, 'bond_type', proto, metadata.enum_overrides),
            provided_status=fn(
                BondStatus.parse, 'status', proto, metadata.enum_overrides))
    new.bond_type_raw_text = fn(normalize, 'bond_type', proto)
    new.status_raw_text = fn(normalize, 'status', proto)

    return new.build()
