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
"""Converts an ingest_info proto StateSentenceGroup to a persistence entity."""
from recidiviz.common import common_utils
from recidiviz.common.constants.enum_parser import EnumParser
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import StateSentenceGroup
from recidiviz.persistence.entity.state import entities


def copy_fields_to_builder(
    sentence_group_builder: entities.StateSentenceGroup.Builder,
    proto: StateSentenceGroup,
    metadata: IngestMetadata,
) -> None:
    """Mutates the provided |sentence_group_builder| by converting an
    ingest_info proto StateSentenceGroup.

    Note: This will not copy children into the Builder!
    """
    new = sentence_group_builder

    # Enum mappings
    new.status = EnumParser(
        getattr(proto, "status"), StateSentenceStatus, metadata.enum_overrides
    )
    new.status_raw_text = getattr(proto, "status")

    # 1-to-1 mappings
    state_sentence_group_id = getattr(proto, "state_sentence_group_id")
    new.external_id = (
        None
        if common_utils.is_generated_id(state_sentence_group_id)
        else state_sentence_group_id
    )
    new.date_imposed = getattr(proto, "date_imposed")
    new.state_code = metadata.region
    new.county_code = getattr(proto, "county_code")
    new.min_length_days = getattr(proto, "min_length")
    new.max_length_days = getattr(proto, "max_length")
    new.is_life = getattr(proto, "is_life")
