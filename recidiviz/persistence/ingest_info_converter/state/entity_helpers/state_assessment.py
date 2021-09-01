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
from recidiviz.common import common_utils
from recidiviz.common.constants.defaulting_and_normalizing_enum_parser import (
    DefaultingAndNormalizingEnumParser,
)
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentLevel,
    StateAssessmentType,
)
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import StateAssessment
from recidiviz.persistence.entity.state import entities


# TODO(#8905): Delete this file once all states have been migrated to v2 ingest
#  mappings.
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

    # Enum mappings
    new.assessment_class = DefaultingAndNormalizingEnumParser(
        getattr(proto, "assessment_class"),
        StateAssessmentClass,
        metadata.enum_overrides,
    )
    new.assessment_class_raw_text = getattr(proto, "assessment_class")

    new.assessment_type = DefaultingAndNormalizingEnumParser(
        getattr(proto, "assessment_type"), StateAssessmentType, metadata.enum_overrides
    )
    new.assessment_type_raw_text = getattr(proto, "assessment_type")

    new.assessment_level = DefaultingAndNormalizingEnumParser(
        getattr(proto, "assessment_level"),
        StateAssessmentLevel,
        metadata.enum_overrides,
    )
    new.assessment_level_raw_text = getattr(proto, "assessment_level")

    # 1-to-1 mappings
    state_assessment_id = getattr(proto, "state_assessment_id")
    new.external_id = (
        None
        if common_utils.is_generated_id(state_assessment_id)
        else state_assessment_id
    )
    new.assessment_date = getattr(proto, "assessment_date")
    new.state_code = metadata.region
    new.assessment_score = getattr(proto, "assessment_score")
    new.assessment_metadata = getattr(proto, "assessment_metadata")
