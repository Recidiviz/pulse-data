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
# =============================================================================

"""Converts an ingest_info proto StateSupervisionContact to a persistence entity."""
from recidiviz.common import common_utils
from recidiviz.common.constants.defaulting_and_normalizing_enum_parser import (
    DefaultingAndNormalizingEnumParser,
)
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactLocation,
    StateSupervisionContactMethod,
    StateSupervisionContactReason,
    StateSupervisionContactStatus,
    StateSupervisionContactType,
)
from recidiviz.common.ingest_metadata import LegacyStateIngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import StateSupervisionContact
from recidiviz.persistence.entity.state import entities


# TODO(#8905): Delete this file once all states have been migrated to v2 ingest
#  mappings.
def copy_fields_to_builder(
    supervision_contact_builder: entities.StateSupervisionContact.Builder,
    proto: StateSupervisionContact,
    metadata: LegacyStateIngestMetadata,
) -> None:
    """Mutates the provided |supervision_contact_builder| by converting an ingest_info proto StateSupervisionContact.

    Note: This will not copy children into the Builder!
    """
    new = supervision_contact_builder

    # 1-to-1 mappings

    state_supervision_contact_id = getattr(proto, "state_supervision_contact_id")
    new.external_id = (
        None
        if common_utils.is_generated_id(state_supervision_contact_id)
        else state_supervision_contact_id
    )

    new.contact_date = getattr(proto, "contact_date")
    new.resulted_in_arrest = getattr(proto, "resulted_in_arrest")
    new.verified_employment = getattr(proto, "verified_employment")
    new.state_code = metadata.region

    # enum values
    new.status = DefaultingAndNormalizingEnumParser(
        getattr(proto, "status"),
        StateSupervisionContactStatus,
        metadata.enum_overrides,
    )
    new.status_raw_text = getattr(proto, "status")
    new.contact_type = DefaultingAndNormalizingEnumParser(
        getattr(proto, "contact_type"),
        StateSupervisionContactType,
        metadata.enum_overrides,
    )
    new.contact_type_raw_text = getattr(proto, "contact_type")
    new.contact_reason = DefaultingAndNormalizingEnumParser(
        getattr(proto, "contact_reason"),
        StateSupervisionContactReason,
        metadata.enum_overrides,
    )
    new.contact_reason_raw_text = getattr(proto, "contact_reason")
    new.location = DefaultingAndNormalizingEnumParser(
        getattr(proto, "location"),
        StateSupervisionContactLocation,
        metadata.enum_overrides,
    )
    new.location_raw_text = getattr(proto, "location")
    new.contact_method = DefaultingAndNormalizingEnumParser(
        getattr(proto, "contact_method"),
        StateSupervisionContactMethod,
        metadata.enum_overrides,
    )
    new.contact_method_raw_text = getattr(proto, "contact_method")
