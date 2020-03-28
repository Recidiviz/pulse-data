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

"""Converts an ingest_info proto StatePersonExternalId to a persistence
entity."""
from recidiviz.common.common_utils import get_external_id
from recidiviz.common.str_field_utils import normalize
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import StatePersonExternalId
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.utils.converter_utils import \
    fn, parse_external_id, parse_region_code_with_override


def convert(proto: StatePersonExternalId,
            metadata: IngestMetadata) -> entities.StatePersonExternalId:
    """Converts an ingest_info proto Hold to a persistence entity."""
    new = entities.StatePersonExternalId.builder()

    new.external_id = \
        fn(_parse_state_external_id, 'state_person_external_id_id', proto)
    new.id_type = fn(normalize, 'id_type', proto)
    new.state_code = parse_region_code_with_override(
        proto, 'state_code', metadata)

    return new.build()


def _parse_state_external_id(id_str):
    """Undoes preprocessing of state_person_external_ids done when converting
    from ingest_info py -> ingest_info proto.
    """
    synthetic_id = parse_external_id(id_str)
    external_id = get_external_id(synthetic_id=synthetic_id)
    return normalize(external_id)
