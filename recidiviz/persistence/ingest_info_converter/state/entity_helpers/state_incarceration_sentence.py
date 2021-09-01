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

"""Converts an ingest_info proto StateIncarcerationSentence to a
persistence entity."""
from recidiviz.common import common_utils
from recidiviz.common.constants.defaulting_and_normalizing_enum_parser import (
    DefaultingAndNormalizingEnumParser,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import StateIncarcerationSentence
from recidiviz.persistence.entity.state import entities


# TODO(#8905): Delete this file once all states have been migrated to v2 ingest
#  mappings.
def copy_fields_to_builder(
    incarceration_sentence_builder: entities.StateIncarcerationSentence.Builder,
    proto: StateIncarcerationSentence,
    metadata: IngestMetadata,
) -> None:
    """Mutates the provided |incarceration_sentence_builder| by converting an
    ingest_info proto StateIncarcerationSentence.

    Note: This will not copy children into the Builder!
    """
    new = incarceration_sentence_builder

    # Enum mappings
    new.status = DefaultingAndNormalizingEnumParser(
        getattr(proto, "status"), StateSentenceStatus, metadata.enum_overrides
    )
    new.status_raw_text = getattr(proto, "status")

    new.incarceration_type = DefaultingAndNormalizingEnumParser(
        getattr(proto, "incarceration_type"),
        StateIncarcerationType,
        metadata.enum_overrides,
    )
    new.incarceration_type_raw_text = getattr(proto, "incarceration_type")

    # 1-to-1 mappings

    state_incarceration_sentence_id = getattr(proto, "state_incarceration_sentence_id")
    new.external_id = (
        None
        if common_utils.is_generated_id(state_incarceration_sentence_id)
        else state_incarceration_sentence_id
    )
    new.date_imposed = getattr(proto, "date_imposed")
    new.start_date = getattr(proto, "start_date")
    new.projected_min_release_date = getattr(proto, "projected_min_release_date")
    new.projected_max_release_date = getattr(proto, "projected_max_release_date")
    new.completion_date = getattr(proto, "completion_date")
    new.parole_eligibility_date = getattr(proto, "parole_eligibility_date")
    new.state_code = metadata.region
    new.county_code = getattr(proto, "county_code")
    new.min_length_days = getattr(proto, "min_length")
    new.max_length_days = getattr(proto, "max_length")
    new.is_life = getattr(proto, "is_life")
    new.is_capital_punishment = getattr(proto, "is_capital_punishment")
    new.parole_possible = getattr(proto, "parole_possible")
    new.initial_time_served_days = getattr(proto, "initial_time_served")
    new.good_time_days = getattr(proto, "good_time")
    new.earned_time_days = getattr(proto, "earned_time")
