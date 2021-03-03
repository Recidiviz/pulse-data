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
# =============================================================================
"""Shared util functions dealing with direct ingest of regions."""
import os
from typing import List
import recidiviz


_REGIONS_DIR = os.path.dirname(recidiviz.ingest.direct.regions.__file__)


def get_existing_region_dir_paths() -> List[str]:
    """Returns list of paths to all region directories in ingest/direct/regions."""
    return [os.path.join(_REGIONS_DIR, d) for d in get_existing_region_dir_names()]


def get_existing_region_dir_names() -> List[str]:
    """Returns list of region directories existing in ingest/direct/regions."""
    return [
        d
        for d in os.listdir(_REGIONS_DIR)
        if os.path.isdir(os.path.join(_REGIONS_DIR, d)) and not d.startswith("__")
    ]
