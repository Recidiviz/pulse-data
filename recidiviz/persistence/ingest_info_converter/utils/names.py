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
"""Utilities for working with names in the ingest info converter.

TODO(#1861): Add unit tests instead of relying on implicit testing elsewhere
"""

from typing import Optional

from recidiviz.common.str_field_utils import NormalizedJSON, normalize
from recidiviz.persistence.ingest_info_converter.utils.converter_utils import fn


def parse_name(proto) -> Optional[NormalizedJSON]:
    """Parses name into a single string."""
    names = {
        "full_name": fn(normalize, "full_name", proto),
        "given_names": fn(normalize, "given_names", proto),
        "middle_names": fn(normalize, "middle_names", proto),
        "surname": fn(normalize, "surname", proto),
        "name_suffix": fn(normalize, "name_suffix", proto),
    }
    names = {k: v for k, v in names.items() if v}

    if names.get("full_name") and any(
        (
            names.get("given_names"),
            names.get("middle_names"),
            names.get("surname"),
            names.get("name_suffix"),
        )
    ):
        raise ValueError(
            "Cannot have full_name and surname/middle/given_names/name_suffix"
        )

    return NormalizedJSON(**names) if names else None
