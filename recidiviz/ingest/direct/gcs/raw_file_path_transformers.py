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
"""Region-specific transformation of raw data file paths before normalization.

Some states deliver raw files whose blob names embed extra content that would
otherwise be parsed as part of the file tag and fail to match the region's raw
data config, or that land in a subfolder of the ingest bucket instead of the
bucket root (e.g. a vendor's SFTP upload). This module transforms the blob's
relative path at the point a freshly-arrived file is normalized, keyed by
region code, stripping embedded content and/or a subfolder prefix so that
downstream file tag matching sees a clean, bucket-root file name.

This only computes the transformed path; it does not decide the file's
update_datetime. That is sourced separately (from the object's preserved
creation time, else the current time) by the normalization Cloud Function.

Each region's transformer is a small, self-contained function, so they 
all live in this one file, looked up by region code.

TODO(OBT-47097): Split these into per-region modules selected by a factory (like
SftpDownloadDelegateFactory -- this isn't trivial: this file is imported by the 
ingest_filename_normalization Cloud Function, which must stay lightweight, so the split 
has to avoid importing every region's code just to handle one file) or design a more wholesale
solution to SFTPGO-managed file management.
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


def _transform_us_nyc_file_path(blob_name: str) -> str:
    """Returns |blob_name| with a trailing MMDDYYYYHHMM timestamp stripped if one exists."""
    match = _US_NYC_TIMESTAMPED_FILE_NAME_REGEX.match(blob_name)
    if not match:
        return blob_name
    try:
        datetime.datetime.strptime(match.group("timestamp"), _US_NYC_TIMESTAMP_FORMAT)
    except ValueError:
        return blob_name
    return f"{match.group('file_tag')}.{match.group('extension')}"


# TN's "cashless" SFTP vendor account uploads directly to this subfolder of the
# ingest bucket instead of the bucket root; TN's MiCase (TOMIS 2.0) vendor
# uploads into a dated subfolder under this prefix, e.g.
# "tn-micase-sftp/2026-01-01/FOO.csv". Both need flattening to the bucket root
# for downstream unprocessed-file discovery, which matches on a bucket-root
# "unprocessed_" prefix.
_US_TN_CASHLESS_SUBFOLDER_REGEX = re.compile(r"^tn-cashless-sftp/(?P<file_name>[^/]+)$")
_US_TN_MICASE_SUBFOLDER_REGEX = re.compile(
    r"^tn-micase-sftp/\d{4}-\d{2}-\d{2}/(?P<file_name>[^/]+)$"
)
_US_TN_VENDOR_SUBFOLDER_REGEXES = [
    _US_TN_CASHLESS_SUBFOLDER_REGEX,
    _US_TN_MICASE_SUBFOLDER_REGEX,
]


def _transform_us_tn_file_path(blob_name: str) -> str:
    """Returns |blob_name| flattened to a bucket-root file name if it is in one of
    TN's known vendor-upload subfolders, or |blob_name| unchanged otherwise.
    """
    for pattern in _US_TN_VENDOR_SUBFOLDER_REGEXES:
        match = pattern.match(blob_name)
        if match:
            return match.group("file_name")
    return blob_name


# Per-region raw file path transformers, keyed by region code. A region absent from
# this map has no transformation applied. A transformer returns the transformed
# relative path, or the unmodified path if it does not match its expected pattern.
_RAW_FILE_PATH_TRANSFORMERS: dict[str, Callable[[str], str]] = {
    StateCode.US_NYC.value: _transform_us_nyc_file_path,
    StateCode.US_TN.value: _transform_us_tn_file_path,
}


def transform_raw_file_path(region_code: str, blob_name: str) -> str:
    """Returns |blob_name| (the file's relative path from the ingest bucket root)
    with any region-specific content stripped and/or relocated to the bucket root,
    so the result is both a clean file tag for the region's raw data config to match
    against and the bucket-root path normalization should move the file to. Returns
    |blob_name| unchanged when the region has no transformer or the path does not
    match the region's expected pattern(s).
    """
    transformer = _RAW_FILE_PATH_TRANSFORMERS.get(region_code.upper())
    if transformer is None:
        return blob_name
    return transformer(blob_name)
