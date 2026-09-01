# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Region-specific cleaning of raw data file names before normalization.

Some states deliver raw files whose names embed extra content (e.g. a per-file
timestamp) that would otherwise be parsed as part of the file tag and fail to
match the region's raw data config. This module strips that content at the point
a freshly-arrived file is normalized, keyed by region code, so that downstream
file tag matching sees a clean name.

This only strips the name; it does not decide the file's update_datetime. That is
sourced separately (from the object's preserved creation time, else the current
time) by the normalization Cloud Function.
"""
import datetime
import re
from collections.abc import Callable

from recidiviz.common.constants.states import StateCode

# US_NYC embeds a per-file MMDDYYYYHHMM timestamp immediately before the extension,
# e.g. PIC_FEED_081920260030.csv. The non-greedy file tag group and the anchored
# 12-digit timestamp split the name at the trailing timestamp even when the tag
# itself contains underscores.
_US_NYC_TIMESTAMP_FORMAT = "%m%d%Y%H%M"
_US_NYC_TIMESTAMPED_FILE_NAME_REGEX = re.compile(
    r"^(?P<file_tag>.+?)_(?P<timestamp>\d{12})\.(?P<extension>[^.]+)$"
)


def _clean_us_nyc_file_name(file_name: str) -> str:
    """Returns |file_name| with a trailing MMDDYYYYHHMM timestamp stripped if one exists."""
    match = _US_NYC_TIMESTAMPED_FILE_NAME_REGEX.match(file_name)
    if not match:
        return file_name
    try:
        datetime.datetime.strptime(match.group("timestamp"), _US_NYC_TIMESTAMP_FORMAT)
    except ValueError:
        return file_name
    return f"{match.group('file_tag')}.{match.group('extension')}"


# Per-region raw file name cleaners, keyed by region code. A region absent from
# this map has no cleaning applied. A cleaner returns the stripped name, or the
# unmodified name if it does not match its expected pattern.
#
# TODO(OBT-47097): This central single-module registry is intentional while only one
# region needs a cleaner and each cleaner is a pure str -> str. Refactor to
# the per-region-module + factory pattern (like SftpDownloadDelegateFactory) once
# BOTH: (1) a second region needs a cleaner or the logic grows region-specific
# complexity beyond a simple pure function, AND (2) we can keep the
# ingest_filename_normalization Cloud Function's dependency closure minimal (per-region
# modules kept import-light, or dispatch that does not import every region eagerly).
_RAW_FILE_NAME_CLEANERS: dict[str, Callable[[str], str]] = {
    StateCode.US_NYC.value: _clean_us_nyc_file_name,
}


def clean_raw_file_name(region_code: str, file_name: str) -> str:
    """Returns |file_name| with any region-specific content (e.g. NYC's embedded
    timestamp) stripped, so the parsed file tag matches the region's raw data
    config. Returns |file_name| unchanged when the region has no cleaner or the name
    does not match the region's expected pattern.
    """
    cleaner = _RAW_FILE_NAME_CLEANERS.get(region_code.upper())
    if cleaner is None:
        return file_name
    return cleaner(file_name)
