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

"""Converts an ingest_info proto StateBond to a persistence entity."""

from typing import cast, Optional

from recidiviz.common.str_field_utils import normalize, parse_date
from recidiviz.common.constants.bond import BondStatus, BondType
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import StateBond
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.utils import converter_utils
from recidiviz.persistence.ingest_info_converter.utils.converter_utils import (
    fn,
    parse_external_id,
    parse_region_code_with_override,
)
from recidiviz.persistence.ingest_info_converter.utils.enum_mappings import EnumMappings


def convert(proto: StateBond, metadata: IngestMetadata) -> entities.StateBond:
    """Converts an ingest_info proto StateBond to a persistence entity."""
    new = entities.StateBond.builder()

    enum_fields = {
        "status": BondStatus,
        "bond_type": BondType,
    }
    enum_mappings = EnumMappings(proto, enum_fields, metadata.enum_overrides)

    # enum values
    new.status = enum_mappings.get(BondStatus)
    new.status_raw_text = fn(normalize, "status", proto)
    new.bond_type = enum_mappings.get(BondType)
    new.bond_type_raw_text = fn(normalize, "bond_type", proto)

    # 1-to-1 mappings
    new.external_id = fn(parse_external_id, "state_bond_id", proto)
    new.date_paid = fn(parse_date, "date_paid", proto)
    new.state_code = parse_region_code_with_override(proto, "state_code", metadata)
    new.county_code = fn(normalize, "county_code", proto)
    new.bond_agent = fn(normalize, "bond_agent", proto)

    (
        new.amount_dollars,
        new.bond_type,
        new.status,
    ) = converter_utils.parse_bond_amount_type_and_status(
        fn(normalize, "amount", proto),
        provided_bond_type=cast(Optional[BondType], new.bond_type),
        provided_status=cast(Optional[BondStatus], new.status),
    )

    return new.build()
