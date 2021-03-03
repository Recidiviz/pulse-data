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
"""
Common utility functions used to manipulate fips.

FIPS county codes are unique identifiers (ints) which are mapped 1:1 to each
county in the United States. Mapping scraped data to a FIPS county code is
important because it provides a universal junction for joining multiple data
sets.

The `data_sets/fips.csv` file contains a mapping of (county_name, state) to FIPS
county code. It's important to note that when joining scraped data with a FIPS
county code, we must take the county's state into account since multiple states
share the same county names.

More info: https://en.wikipedia.org/wiki/FIPS_county_code
"""
import re
from typing import Tuple
import pandas as pd
import us

from recidiviz.common.errors import FipsMergingError
from recidiviz.common.fips_fuzzy_matching import fuzzy_join
from recidiviz.tests.ingest.fixtures import as_filepath

# Float between [0, 1] which sets the required fuzzy matching certainty
_FUZZY_MATCH_CUTOFF = 0.75

_FIPS = pd.read_csv(as_filepath("fips.csv", subdir="data_sets"), dtype={"fips": str})


def add_column_to_df(
    df: pd.DataFrame, county_names: pd.Series, state: us.states
) -> pd.DataFrame:
    """Add a new fips column to |df|.

    The provided |county_names| must be the same length as |df| and map each
    |df| row to the county_name that should be used to join against fips.csv.
    """
    old_index = df.index
    df.index = county_names.apply(_sanitize_county_name)

    df = fuzzy_join(df, get_fips_for(state), _FUZZY_MATCH_CUTOFF)

    df.index = old_index
    return df


def get_fips_for(state: us.states) -> pd.DataFrame:
    """Get the [county_name, fips] df, filtering for the given |state|."""

    # Copy _FIPS to allow mutating the view created after filtering by state
    fips = _FIPS.copy()

    fips = fips[fips.state_code == int(state.fips)]
    if fips.empty:
        raise FipsMergingError("Failed to find FIPS codes for state: {}".format(state))

    fips["county_name"] = fips["county_name"].apply(_sanitize_county_name)
    fips = fips.set_index("county_name")
    return fips[["fips"]]


def validate_county_code(county_code: str) -> None:
    """
    Validate county_code is in proper format without spelling errors,
    county_code should be in lower case. It accepts and will not raise error for state codes, ex. 'us_ny'
    """
    cleaned_county_code = re.sub("^us_[a-z]{2}_", "", county_code)
    if re.search("^us_[a-z]{2}", cleaned_county_code) is not None:
        return
    # TODO(#6040): remove code for 'st_mary_parish' and 'translyvania' once issue fixed
    if cleaned_county_code in ["st_mary_parish", "translyvania", "carter_vendengine"]:
        return

    df_fips = _FIPS.copy()
    sanitized_counties = [
        _sanitize_county_name(row["county_name"]) for _, row in df_fips.iterrows()
    ]

    if cleaned_county_code not in sanitized_counties:
        raise ValueError(
            f"county_code does could not be found in sanitized fips: {county_code}"
        )


def _standardize_raw_state(state: str) -> str:
    """adds 'US_' to front of state and makes it uppercase, ex. US_WI"""
    if len(state) != 2:
        raise ValueError(
            f"state should only have 2 characters, current character count: {len(state)}"
        )
    return f"US_{state}".upper()


def get_state_and_county_for_fips(fips: int) -> Tuple[str, str]:
    """Get the [state, county] for the provided |fips| in this format ['US_AL','BARBOUR']."""

    # Copy _FIPS to allow mutating the view created after filtering by state
    df_fips = _FIPS.copy()
    df_fips["fips"] = pd.to_numeric(df_fips["fips"])
    df_fips = df_fips[df_fips.fips == fips]
    if df_fips.empty:
        raise FipsMergingError(f"Failed to find FIPS code: {fips}")
    raw_state = df_fips.iloc[0, 0]
    raw_county = df_fips.iloc[0, 3]

    return _standardize_raw_state(raw_state), _sanitize_county_name(raw_county).upper()


def _sanitize_county_name(county_name: str) -> str:
    """To ease fuzzy matching, ensure county_names fit a common shape. returns in lower case,
    ex. 'York Parish' -> 'york'. For county names, capitalized 'City' is kept and lower case 'city' is removed."""
    county = county_name.replace(" city", "")
    county = county.lower().replace(" county", "")
    county = county.replace(".", "")
    county = county.replace("'", "")
    county = county.replace(" borough", "")
    county = county.replace(" city and borough", "")
    county = county.replace(" census area", "")
    county = county.replace(" municipality", "")
    county = county.replace(" parish", "")
    county = county.replace(" municipio", "")
    county = county.replace(" ", "_")
    return county
