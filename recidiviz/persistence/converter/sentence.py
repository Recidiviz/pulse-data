# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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
"""Converts an ingest_info proto Sentence to a persistence entity."""
from recidiviz.common.constants.sentence import SentenceStatus
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.persistence import entities
from recidiviz.persistence.converter.converter_utils import normalize, \
    parse_bool, parse_days, parse_dollars, fn, parse_external_id


def convert(proto, metadata: IngestMetadata) -> entities.Sentence:
    """Converts an ingest_info proto Sentence to a persistence entity."""
    new = entities.Sentence.builder()

    new.external_id = fn(parse_external_id, 'sentence_id', proto)
    new.sentencing_region = fn(normalize, 'sentencing_region', proto)
    new.min_length_days = fn(parse_days, 'min_length', proto)
    new.max_length_days = fn(parse_days, 'max_length', proto)
    new.is_life = fn(parse_bool, 'is_life', proto)
    new.is_probation = fn(parse_bool, 'is_probation', proto)
    new.is_suspended = fn(parse_bool, 'is_suspended', proto)
    new.fine_dollars = fn(parse_dollars, 'fine_dollars', proto)
    new.parole_possible = fn(parse_bool, 'parole_possible', proto)
    new.post_release_supervision_length_days = \
        fn(parse_days, 'post_release_supervision_length', proto)
    new.status = fn(SentenceStatus.parse, 'status', proto,
                    metadata.enum_overrides,
                    default=SentenceStatus.UNKNOWN_FOUND_IN_SOURCE)
    new.status_raw_text = fn(normalize, 'status', proto)

    return new.build()
