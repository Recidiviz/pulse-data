# Recidiviz - a platform for tracking granular recidivism metrics in real time
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
"""
Common utility functions used to manipulate fips.

FIPS county codes are unique identifiers (ints) which are mapped 1:1 to each
county in the United States. Mapping scraped data to a FIPS county code is
important because it provides a universal junction for joining multiple data
sets.

The `data_sets/fips.csv` file contains a mapping of (county_name, state)
to FIPS county code. It's important to note that when joining scraped data with
a FIPS county code, we must take the county's state into account since
multiple states share the same county names.

More info: https://en.wikipedia.org/wiki/FIPS_county_code
"""
import difflib
from typing import Iterable

import pandas as pd
import us

from recidiviz.common.errors import FipsMergingError
from recidiviz.tests.ingest.fixtures import as_filepath

# Float between [0, 1] which sets the required fuzzy matching certainty
_FUZZY_MATCH_CUTOFF = 0.75

_FIPS = pd.read_csv(as_filepath('fips.csv', subdir='data_sets'))


def add_column_to_df(df: pd.DataFrame, county_names: pd.Series,
                     state: us.states) -> pd.DataFrame:
    """Add a new fips column to |df|.

    The provided |county_names| must be the same length as |df| and map each
    |df| row to the county_name that should be used to join against fips.csv.
    """
    old_index = df.index
    df.index = county_names.apply(_sanitize_county_name)

    df = _fuzzy_join(df, _get_fips_for(state))

    df.index = old_index
    return df


def _get_fips_for(state: us.states) -> pd.DataFrame:
    """Get the [county_name, fips] df, filtering for the given |state|."""

    # Copy _FIPS to allow mutating the view created after filtering by state
    fips = _FIPS.copy()

    fips = fips[fips.state_code == int(state.fips)]
    if fips.empty:
        raise FipsMergingError(
            'Failed to find FIPS codes for state: {}'.format(state))

    fips['county_name'] = fips['county_name'].apply(_sanitize_county_name)
    fips = fips.set_index('county_name')
    return fips[['fips']]


def _sanitize_county_name(county_name: str) -> str:
    """To ease fuzzy matching, ensure county_names fit a common shape."""
    return county_name.lower().replace(' county', '')


def _fuzzy_join(df1: pd.DataFrame, df2: pd.DataFrame):
    """Merges df1 to df2 by choosing the closest index (fuzzy) to join on."""
    df1.index = df1.index.map(lambda x: _best_match(x, df2.index))
    return df1.join(df2)


def _best_match(county_name: str, known_county_names: Iterable[str]) -> str:
    """Returns the closest match of |county_name| in |known_county_names|."""
    close_matches = difflib.get_close_matches(county_name, known_county_names,
                                              n=1, cutoff=_FUZZY_MATCH_CUTOFF)

    if not close_matches:
        raise FipsMergingError(
            'Failed to fuzzy match "{}" to known county_names in the state: '
            '{}'.format(county_name, known_county_names))

    best_match = close_matches[0]

    # Cast to a str since `difflib.get_close_matches` returns a Sequence[str]
    return str(best_match)
