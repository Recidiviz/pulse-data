# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Utils for extracting information from ingest bucket names."""

import re
from typing import Match, Optional

INGEST_PRIMARY_BUCKET_SUFFIX = ""
INGEST_SECONDARY_BUCKET_SUFFIX = "-secondary"
INGEST_SFTP_BUCKET_SUFFIX = "-sftp"

_DIRECT_INGEST_BUCKET_REGEX = re.compile(
    r"(?P<project>recidiviz-(?:.*))-direct-ingest-state-"
    r"(?P<state_code>[a-z]{2}-[a-z]{2})"
    rf"(?P<suffix>{INGEST_SECONDARY_BUCKET_SUFFIX}|"
    rf"-upload-testing|{INGEST_SFTP_BUCKET_SUFFIX})?$"
)

_DIRECT_INGEST_STORAGE_BUCKET_REGEX = re.compile(
    rf"(?P<project>recidiviz-(?:.*))-direct-ingest-state-storage"
    rf"(?P<suffix>{INGEST_SECONDARY_BUCKET_SUFFIX})?$"
)


def get_region_code_from_direct_ingest_bucket(ingest_bucket_name: str) -> Optional[str]:
    match_obj: Optional[Match] = re.match(
        _DIRECT_INGEST_BUCKET_REGEX, ingest_bucket_name
    )
    if match_obj is None:
        return None

    region_code_match = match_obj.groupdict().get("state_code", None)
    if not region_code_match:
        return None
    return region_code_match.replace("-", "_")


def is_primary_ingest_bucket(ingest_bucket_name: str) -> bool:
    match_obj: Optional[Match] = re.match(
        _DIRECT_INGEST_BUCKET_REGEX, ingest_bucket_name
    )
    if match_obj is None:
        raise ValueError(f"Invalid ingest bucket [{ingest_bucket_name}]")
    return match_obj.group("suffix") is None


def is_secondary_ingest_bucket(ingest_bucket_name: str) -> bool:
    match_obj: Optional[Match] = re.match(
        _DIRECT_INGEST_BUCKET_REGEX, ingest_bucket_name
    )
    if match_obj is None:
        raise ValueError(f"Invalid ingest bucket [{ingest_bucket_name}]")
    return match_obj.group("suffix") == INGEST_SECONDARY_BUCKET_SUFFIX


def build_ingest_bucket_name(*, project_id: str, region_code: str, suffix: str) -> str:
    """
    This function returns the bucket name for the GCS bucket
    that initially receives raw data from a state, either through
    SFTP or direct upload.

    Files can be dumped into this bucket with the format <file_tag>.csv and a process
    will rename them to the expected format of unprocessed_<timestamp>_raw_<file_tag>.csv
    """
    normalized_region_code = region_code.lower().replace("_", "-")
    bucket_name = f"{project_id}-direct-ingest-state-{normalized_region_code}{suffix}"
    if not re.match(_DIRECT_INGEST_BUCKET_REGEX, bucket_name):
        raise ValueError(
            f"Generated ingest bucket name [{bucket_name}] does not match expected regex."
        )
    return bucket_name


def build_ingest_storage_bucket_name(*, project_id: str, suffix: str) -> str:
    """
    This function returns the bucket name for the GCS bucket
    that stores processed raw data for a state and instance.

    It has a subdirectory for each state, which each have
    subdirectores of "raw" and "deprecated". For example, US_TN has

    us_tn/
      - us_tn/raw/
      - us_tn/deprecated/
    """
    bucket_name = f"{project_id}-direct-ingest-state-storage{suffix}"
    if not re.match(_DIRECT_INGEST_STORAGE_BUCKET_REGEX, bucket_name):
        raise ValueError(
            f"Generated ingest bucket name [{bucket_name}] does not match expected regex."
        )

    return bucket_name
