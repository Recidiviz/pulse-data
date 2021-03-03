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
"""Converts an ingest_info proto Booking to a persistence entity."""
from datetime import date
from typing import Optional, Tuple

from recidiviz.common.str_field_utils import normalize, parse_date
from recidiviz.common.constants.county.booking import (
    AdmissionReason,
    Classification,
    CustodyStatus,
    ReleaseReason,
)
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.common.fid import validate_fid
from recidiviz.persistence.ingest_info_converter.utils.converter_utils import (
    fn,
    parse_external_id,
)
from recidiviz.persistence.ingest_info_converter.utils.enum_mappings import EnumMappings


def copy_fields_to_builder(booking_builder, proto, metadata):
    """Mutates the provided |booking_builder| by converting an ingest_info proto
    Booking.

    Note: This will not copy children into the Builder!
    """
    new = booking_builder

    enum_fields = {
        "admission_reason": AdmissionReason,
        "release_reason": ReleaseReason,
        "custody_status": CustodyStatus,
        "classification": Classification,
    }
    enum_mappings = EnumMappings(proto, enum_fields, metadata.enum_overrides)

    # Enum mappings
    new.admission_reason = enum_mappings.get(AdmissionReason)
    new.admission_reason_raw_text = fn(normalize, "admission_reason", proto)
    new.release_reason = enum_mappings.get(ReleaseReason)
    new.release_reason_raw_text = fn(normalize, "release_reason", proto)
    new.custody_status = enum_mappings.get(
        CustodyStatus, default=CustodyStatus.PRESENT_WITHOUT_INFO
    )
    new.custody_status_raw_text = fn(normalize, "custody_status", proto)
    new.classification = enum_mappings.get(Classification)
    new.classification_raw_text = fn(normalize, "classification", proto)

    # 1-to-1 mappings
    new.external_id = fn(parse_external_id, "booking_id", proto)
    new.projected_release_date = fn(parse_date, "projected_release_date", proto)
    new.facility = fn(normalize, "facility", proto)
    new.facility_id = fn(
        validate_fid, "facility_id", proto, default=metadata.facility_id
    )

    # Inferred attributes
    new.admission_date, new.admission_date_inferred = _parse_admission(proto, metadata)
    (
        new.release_date,
        new.projected_release_date,
        new.release_date_inferred,
    ) = _parse_release_date(proto, metadata)
    _set_custody_status_if_needed(new)

    # Metadata
    new.last_seen_time = metadata.ingest_time
    # Will be overwritten by first_seen_time value in database if one is already
    # present
    new.first_seen_time = metadata.ingest_time


def _set_custody_status_if_needed(new):
    # release_date is guaranteed to be in the past by _parse_release_date
    if new.release_date and new.custody_status is CustodyStatus.PRESENT_WITHOUT_INFO:
        new.custody_status = CustodyStatus.RELEASED


def _parse_release_date(
    proto, metadata: IngestMetadata
) -> Tuple[Optional[date], Optional[date], Optional[bool]]:
    """Reads release_date and projected_release_date from |proto|.

    If release_date is present on proto, sets release_date_inferred to (False).

    If release_date is in the future relative to scrape time, will be treated
    as projected_release_date instead.
    """
    release_date = fn(parse_date, "release_date", proto)
    projected_release_date = fn(parse_date, "projected_release_date", proto)

    if release_date and release_date > metadata.ingest_time.date():
        projected_release_date = release_date
        release_date = None

    release_date_inferred = None if release_date is None else False

    return release_date, projected_release_date, release_date_inferred


def _parse_admission(proto, metadata):
    admission_date = fn(parse_date, "admission_date", proto)

    if admission_date is None:
        admission_date = metadata.ingest_time.date()
        admission_date_inferred = True
    else:
        admission_date_inferred = False

    return admission_date, admission_date_inferred
