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

"""Converts an ingest_info proto StateCourtCase to a persistence entity."""
from recidiviz.common import common_utils
from recidiviz.common.constants.defaulting_and_normalizing_enum_parser import (
    DefaultingAndNormalizingEnumParser,
)
from recidiviz.common.constants.state.state_court_case import (
    StateCourtCaseStatus,
    StateCourtType,
)
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import StateCourtCase
from recidiviz.persistence.entity.state import entities


# TODO(#8905): Delete this file once all states have been migrated to v2 ingest
#  mappings.
def copy_fields_to_builder(
    court_case_builder: entities.StateCourtCase.Builder,
    proto: StateCourtCase,
    metadata: IngestMetadata,
) -> None:

    """Mutates the provided |court_case_builder| by converting an ingest_info
    proto StateCourtCase.

    Note: This will not copy children into the Builder!
    """
    new = court_case_builder

    # enum values
    new.status = DefaultingAndNormalizingEnumParser(
        getattr(proto, "status"), StateCourtCaseStatus, metadata.enum_overrides
    )
    new.status_raw_text = getattr(proto, "status")

    new.court_type = DefaultingAndNormalizingEnumParser(
        getattr(proto, "court_type"), StateCourtType, metadata.enum_overrides
    )
    new.court_type_raw_text = getattr(proto, "court_type")

    # 1-to-1 mappings
    state_court_case_id = getattr(proto, "state_court_case_id")
    new.external_id = (
        None
        if common_utils.is_generated_id(state_court_case_id)
        else state_court_case_id
    )
    new.date_convicted = getattr(proto, "date_convicted")
    new.next_court_date = getattr(proto, "next_court_date")
    new.state_code = metadata.region
    new.county_code = getattr(proto, "county_code")
    new.judicial_district_code = getattr(proto, "judicial_district_code")
