# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""
Common utility functions used to manipulate facility_ids.
Facility IDs are unique identifiers (ints) which are mapped 1:1 to each
facility in the United States.

The `data_sets/fid.csv` file contains a mapping of jid
to facility_id.
"""

import pandas as pd

from recidiviz.tests.ingest.fixtures import as_filepath


_FID = pd.read_csv(as_filepath("fid.csv", subdir="data_sets"), dtype={"vera_jid": str})


def fid_exists(fid: str) -> bool:
    """Returns T/F depending on if FID exists in fid.csv"""
    return fid in _FID["facility_uid"].to_list()


def validate_fid(fid: str) -> str:
    """Raises an error if the jurisdiction id string is not properly formatted."""
    if not fid_exists(fid):
        raise ValueError(f"FID [{fid}] is not listed in fid.csv")
    return fid
