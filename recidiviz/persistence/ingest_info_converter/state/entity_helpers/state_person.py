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
"""Converts an ingest_info proto StatePerson to a persistence entity."""

from recidiviz.common.constants.defaulting_and_normalizing_enum_parser import (
    DefaultingAndNormalizingEnumParser,
)
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.person_characteristics import Gender, ResidencyStatus
from recidiviz.common.constants.strict_enum_parser import StrictEnumParser
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import StatePerson
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.utils.converter_utils import (
    parse_residency_status,
)
from recidiviz.persistence.ingest_info_converter.utils.names import parse_name


# TODO(#8905): Delete this file once all states have been migrated to v2 ingest
#  mappings.
def copy_fields_to_builder(
    state_person_builder: entities.StatePerson.Builder,
    proto: StatePerson,
    metadata: IngestMetadata,
) -> None:
    """Mutates the provided |state_person_builder| by converting an
    ingest_info proto StatePerson.

    Note: This will not copy children into the Builder!
    """
    new = state_person_builder

    # Enum mappings
    new.gender = DefaultingAndNormalizingEnumParser(
        getattr(proto, "gender"), Gender, metadata.enum_overrides
    )
    new.gender_raw_text = getattr(proto, "gender")

    # 1-to-1 mappings
    new.full_name = parse_name(proto)
    new.birthdate = getattr(proto, "birthdate")
    new.current_address = getattr(proto, "current_address")
    new.residency_status = StrictEnumParser(
        raw_text=getattr(proto, "current_address"),
        enum_cls=ResidencyStatus,
        enum_overrides=EnumOverrides.Builder()
        .add_mapper_fn(parse_residency_status, ResidencyStatus)
        .build(),
    )
    new.state_code = metadata.region
