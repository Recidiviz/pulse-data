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
"""Contains logic for fuzzy matching FIPS."""

import difflib
from typing import Iterable

import pandas as pd

from recidiviz.common.errors import FipsMergingError


def fuzzy_join(df1: pd.DataFrame, df2: pd.DataFrame, cutoff: float) -> pd.DataFrame:
    """Merges df1 to df2 by choosing the closest index (fuzzy) to join on."""
    df1.index = df1.index.map(lambda x: best_match(x, df2.index, cutoff))
    return df1.join(df2)


def best_match(
    county_name: str, known_county_names: Iterable[str], cutoff: float
) -> str:
    """Returns the closest match of |county_name| in |known_county_names|."""
    close_matches = difflib.get_close_matches(
        county_name, known_county_names, n=1, cutoff=cutoff
    )

    if not close_matches:
        raise FipsMergingError(
            f"Failed to fuzzy match '{county_name}' to known county_names in the state: "
            f"{known_county_names}"
        )

    closest_match = close_matches[0]

    # Cast to a str since `difflib.get_close_matches` returns a Sequence[str]
    return str(closest_match)
