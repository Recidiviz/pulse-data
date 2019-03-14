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
"""Converts an ingest_info proto Booking to a persistence entity."""
from recidiviz.common.common_utils import normalize
from recidiviz.common.constants.booking import (AdmissionReason,
                                                Classification, CustodyStatus,
                                                ReleaseReason)
from recidiviz.common.date import parse_date
from recidiviz.persistence.converter.converter_utils import (fn,
                                                             parse_external_id)


def copy_fields_to_builder(booking_builder, proto, metadata):
    """Mutates the provided |booking_builder| by converting an ingest_info proto
     Booking.

     Note: This will not copy children into the Builder!
     """
    new = booking_builder

    # 1-to-1 mappings
    new.external_id = fn(parse_external_id, 'booking_id', proto)
    new.projected_release_date = fn(parse_date, 'projected_release_date', proto)
    new.admission_reason = fn(AdmissionReason.parse, 'admission_reason',
                              proto, metadata.enum_overrides)
    new.admission_reason_raw_text = fn(normalize, 'admission_reason', proto)
    new.release_reason = fn(ReleaseReason.parse, 'release_reason', proto,
                            metadata.enum_overrides)
    new.release_reason_raw_text = fn(normalize, 'release_reason', proto)
    new.custody_status = fn(
        CustodyStatus.parse, 'custody_status', proto,
        metadata.enum_overrides, default=CustodyStatus.UNKNOWN_FOUND_IN_SOURCE)
    new.custody_status_raw_text = fn(normalize, 'custody_status', proto)
    new.facility = fn(normalize, 'facility', proto)
    new.classification = fn(Classification.parse, 'classification', proto,
                            metadata.enum_overrides)
    new.classification_raw_text = fn(normalize, 'classification', proto)

    # Inferred attributes
    new.admission_date, new.admission_date_inferred = \
        _parse_admission(proto, metadata)
    new.release_date, new.release_date_inferred = _parse_release_date(proto)
    _set_custody_status_if_needed(new)

    # Metadata
    new.last_seen_time = metadata.ingest_time


def _set_custody_status_if_needed(new):
    if (new.release_date and new.custody_status
            is CustodyStatus.UNKNOWN_FOUND_IN_SOURCE):
        new.custody_status = CustodyStatus.RELEASED


def _parse_release_date(proto):
    release_date = fn(parse_date, 'release_date', proto)
    release_date_inferred = None if release_date is None else False

    return release_date, release_date_inferred


def _parse_admission(proto, metadata):
    admission_date = fn(parse_date, 'admission_date', proto)

    if admission_date is None:
        admission_date = metadata.ingest_time.date()
        admission_date_inferred = True
    else:
        admission_date_inferred = False

    return admission_date, admission_date_inferred
