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
from typing import Optional, Match

_DIRECT_INGEST_BUCKET_REGEX = re.compile(
    r"(?P<project>recidiviz-(?:.*))-direct-ingest-"
    r"(?:(state-(?P<state_code>[a-z]{2}-[a-z]{2}))|"
    r"(county-(?P<county_code>[a-z]{2}-[a-z]{2}-[a-z]*)))"
    r"(?P<suffix>-secondary|-upload-testing)?$"
)


def get_region_code_from_direct_ingest_bucket(ingest_bucket_name: str) -> Optional[str]:
    match_obj: Optional[Match] = re.match(
        _DIRECT_INGEST_BUCKET_REGEX, ingest_bucket_name
    )
    if match_obj is None:
        return None

    region_code_match = match_obj.groupdict().get(
        "state_code", None
    ) or match_obj.groupdict().get("county_code", None)
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
    return match_obj.group("suffix") == "-secondary"
