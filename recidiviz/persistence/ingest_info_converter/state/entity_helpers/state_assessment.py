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

"""Converts an ingest_info proto StateAssessment to a persistence entity."""

from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentType,
    StateAssessmentLevel,
)
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.common.str_field_utils import parse_date, normalize, parse_int
from recidiviz.ingest.models.ingest_info_pb2 import StateAssessment
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.utils.converter_utils import (
    fn,
    parse_external_id,
    parse_region_code_with_override,
)
from recidiviz.persistence.ingest_info_converter.utils.enum_mappings import EnumMappings


def copy_fields_to_builder(
    state_assessment_builder: entities.StateAssessment.Builder,
    proto: StateAssessment,
    metadata: IngestMetadata,
) -> None:
    """Mutates the provided |state_assessment_builder| by converting an
    ingest_info proto StateAssessment.

    Note: This will not copy children into the Builder!
    """

    new = state_assessment_builder

    enum_fields = {
        "assessment_class": StateAssessmentClass,
        "assessment_type": StateAssessmentType,
        "assessment_level": StateAssessmentLevel,
    }
    enum_mappings = EnumMappings(proto, enum_fields, metadata.enum_overrides)

    # Enum mappings
    new.assessment_class = enum_mappings.get(StateAssessmentClass)
    new.assessment_class_raw_text = fn(normalize, "assessment_class", proto)
    new.assessment_type = enum_mappings.get(StateAssessmentType)
    new.assessment_type_raw_text = fn(normalize, "assessment_type", proto)
    new.assessment_level = enum_mappings.get(StateAssessmentLevel)
    new.assessment_level_raw_text = fn(normalize, "assessment_level", proto)

    # 1-to-1 mappings
    new.external_id = fn(parse_external_id, "state_assessment_id", proto)
    new.assessment_date = fn(parse_date, "assessment_date", proto)
    new.state_code = parse_region_code_with_override(proto, "state_code", metadata)
    new.assessment_score = fn(parse_int, "assessment_score", proto)
    new.assessment_metadata = fn(normalize, "assessment_metadata", proto)
