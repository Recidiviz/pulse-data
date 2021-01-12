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

from recidiviz.common.constants.person_characteristics import Gender
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.common.str_field_utils import normalize
from recidiviz.ingest.models.ingest_info_pb2 import StatePerson
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.utils.converter_utils import \
    fn, parse_residency_status, parse_birthdate, parse_region_code_with_override
from recidiviz.persistence.ingest_info_converter.utils.enum_mappings import \
    EnumMappings
from recidiviz.persistence.ingest_info_converter.utils.names import parse_name


def copy_fields_to_builder(state_person_builder: entities.StatePerson.Builder,
                           proto: StatePerson,
                           metadata: IngestMetadata):
    """Mutates the provided |state_person_builder| by converting an
    ingest_info proto StatePerson.

    Note: This will not copy children into the Builder!
    """
    enum_fields = {
        'gender': Gender,
    }
    enum_mappings = EnumMappings(proto, enum_fields, metadata.enum_overrides)

    new = state_person_builder

    # Enum mappings
    new.gender = enum_mappings.get(Gender)
    new.gender_raw_text = fn(normalize, 'gender', proto)

    # 1-to-1 mappings
    new.full_name = parse_name(proto)
    new.birthdate, new.birthdate_inferred_from_age = parse_birthdate(
        proto, 'birthdate', 'age')
    new.current_address = fn(normalize, 'current_address', proto)
    new.residency_status = fn(
        parse_residency_status, 'current_address', proto)
    new.state_code = parse_region_code_with_override(
        proto, 'state_code', metadata)
