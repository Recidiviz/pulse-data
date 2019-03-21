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
from datetime import date
from typing import Optional, Tuple

from recidiviz.common.common_utils import normalize
from recidiviz.common.constants.sentence import SentenceStatus
from recidiviz.common.date import parse_date
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.persistence import entities
from recidiviz.persistence.converter.converter_utils import (fn, parse_bool,
                                                             parse_days,
                                                             parse_dollars,
                                                             parse_external_id)
from recidiviz.persistence.converter.enum_mappings import EnumMappings


def convert(proto, metadata: IngestMetadata) -> entities.Sentence:
    """Converts an ingest_info proto Sentence to a persistence entity."""
    new = entities.Sentence.builder()

    enum_fields = {
        'status': SentenceStatus.parse,
    }
    enum_mappings = EnumMappings(proto, enum_fields, metadata.enum_overrides)

    # Enum mappings
    new.status = enum_mappings.get(SentenceStatus,
                                   default=SentenceStatus.PRESENT_WITHOUT_INFO)
    new.status_raw_text = fn(normalize, 'status', proto)

    # 1-to-1 mappings
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
    new.date_imposed = fn(parse_date, 'date_imposed', proto)
    new.completion_date, new.projected_completion_date = _parse_completion_date(
        proto, metadata)

    _set_status_if_needed(new)

    return new.build()


def _parse_completion_date(
        proto,
        metadata: IngestMetadata) -> Tuple[Optional[date], Optional[date]]:
    """Reads completion_date and projected_completion_date from |proto|.

    If completion_date is in the future relative to scrape time, will be
    treated as projected_completion_date instead.
    """
    completion_date = fn(parse_date, 'completion_date', proto)
    projected_completion_date = fn(
        parse_date, 'projected_completion_date', proto)

    if completion_date and completion_date > metadata.ingest_time.date():
        projected_completion_date = completion_date
        completion_date = None

    return completion_date, projected_completion_date


def _set_status_if_needed(new):
    # completion_date is guaranteed to be in the past by _parse_completion_date
    if new.completion_date and \
            new.status is SentenceStatus.PRESENT_WITHOUT_INFO:
        new.status = SentenceStatus.COMPLETED
