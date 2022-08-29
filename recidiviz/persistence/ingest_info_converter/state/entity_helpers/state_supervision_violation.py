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

"""Converts an ingest_info proto StateSupervisionViolation to a
persistence entity."""
from recidiviz.common import common_utils
from recidiviz.common.ingest_metadata import LegacyStateIngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import StateSupervisionViolation
from recidiviz.persistence.entity.state import entities


# TODO(#8905): Delete this file once all states have been migrated to v2 ingest
#  mappings.
def copy_fields_to_builder(
    supervision_violation_builder: entities.StateSupervisionViolation.Builder,
    proto: StateSupervisionViolation,
    metadata: LegacyStateIngestMetadata,
) -> None:
    """Converts an ingest_info proto StateSupervisionViolation to a
    persistence entity."""
    new = supervision_violation_builder

    # 1-to-1 mappings

    state_supervision_violation_id = getattr(proto, "state_supervision_violation_id")
    new.external_id = (
        None
        if common_utils.is_generated_id(state_supervision_violation_id)
        else state_supervision_violation_id
    )
    new.violation_date = getattr(proto, "violation_date")
    new.state_code = metadata.region
    new.is_violent = getattr(proto, "is_violent")
    new.is_sex_offense = getattr(proto, "is_sex_offense")
