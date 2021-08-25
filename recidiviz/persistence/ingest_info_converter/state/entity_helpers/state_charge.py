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
"""Converts an ingest_info proto StateCharge to a persistence entity."""
from recidiviz.common import common_utils
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.enum_parser import EnumParser
from recidiviz.common.constants.state.state_charge import StateChargeClassificationType
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import StateCharge
from recidiviz.persistence.entity.state import entities


# TODO(#8905): Delete this file once all states have been migrated to v2 ingest
#  mappings.
def copy_fields_to_builder(
    new: entities.StateCharge.Builder, proto: StateCharge, metadata: IngestMetadata
) -> None:
    """Mutates the provided |charge_builder| by converting an ingest_info proto
    StateCharge.

    Note: This will not copy children into the Builder!
    """
    # Enum values
    new.status = EnumParser(
        getattr(proto, "status"), ChargeStatus, metadata.enum_overrides
    )
    new.status_raw_text = getattr(proto, "status")
    new.classification_type = EnumParser(
        getattr(proto, "classification_type"),
        StateChargeClassificationType,
        metadata.enum_overrides,
    )
    new.classification_type_raw_text = getattr(proto, "classification_type")

    # 1-to-1 mappings
    state_charge_id = getattr(proto, "state_charge_id")
    new.external_id = (
        None if common_utils.is_generated_id(state_charge_id) else state_charge_id
    )
    new.offense_date = getattr(proto, "offense_date")
    new.date_charged = getattr(proto, "date_charged")
    new.state_code = metadata.region
    new.county_code = getattr(proto, "county_code")
    new.statute = getattr(proto, "statute")

    new.ncic_code = getattr(proto, "ncic_code")
    new.description = getattr(proto, "description")

    new.attempted = getattr(proto, "attempted")
    new.classification_subtype = getattr(proto, "classification_subtype")
    new.offense_type = getattr(proto, "offense_type")
    new.is_violent = getattr(proto, "is_violent")
    new.is_sex_offense = getattr(proto, "is_sex_offense")
    new.counts = getattr(proto, "counts")
    new.charge_notes = getattr(proto, "charge_notes")
    new.is_controlling = getattr(proto, "is_controlling")
    new.charging_entity = getattr(proto, "charging_entity")
