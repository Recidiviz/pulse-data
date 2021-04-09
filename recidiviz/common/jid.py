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
Common utility functions used to manipulate jurisdiction_ids.

Jurisdiction Ids are unique identifiers (ints) which are mapped 1:1 to each
jurisdiction in the United States. Each Jurisdiction Id starts with the FIPS
county code that that jurisdiction falls into. This means that a single
jurisdiction may map to multiple jurisdiction_ids if that jurisdiction falls
into multiple counties.

The `data_sets/jid.csv` file contains a mapping of (agency_name, state)
to jurisdiction_id. It's important to note that when joining scraped data with
a jurisdiction_id, we must take the county's state into account since
multiple states share the same county names.
"""

import pandas as pd
import us
from more_itertools import one

from recidiviz.common.errors import FipsMergingError
from recidiviz.common.fips import get_fips_for
from recidiviz.common.fips_fuzzy_matching import best_match
from recidiviz.tests.ingest.fixtures import as_filepath

# Float between [0, 1] which sets the required fuzzy matching certainty
_FUZZY_MATCH_CUTOFF = 0.75


_JID: pd.DataFrame = None


def _get_JID() -> pd.DataFrame:
    global _JID

    if _JID is None:
        _JID = pd.read_csv(
            as_filepath("jid.csv", subdir="data_sets"), dtype={"fips": str}
        )

    return _JID


def get(county_name: str, state: us.states.State) -> str:
    """Return the matching jurisdiction_id if one jurisdiction_id can be
    matched to the county_name, otherwise raise a FipsMergingError."""
    county_fips = _to_county_fips(county_name, state)
    return _to_jurisdiction_id(county_fips)


def validate_jid(jid: str) -> str:
    """Raises an error if the jurisdiction id string is not properly formatted."""
    if len(str(jid)) != 8 or not int(str(jid)):
        raise ValueError(
            f"Improperly formatted JID [{jid}], must be an 8 " "character integer."
        )
    return str(jid)


def _to_county_fips(county_name: str, state: us.states.State) -> int:
    """Lookup fips by county_name, filtering within the given state"""
    all_fips_for_state_df = get_fips_for(state)
    actual_county_name = best_match(
        county_name, all_fips_for_state_df.index, _FUZZY_MATCH_CUTOFF
    )

    try:
        return all_fips_for_state_df.at[actual_county_name, "fips"]
    except KeyError as e:
        raise FipsMergingError from e


def _to_jurisdiction_id(county_fips: int) -> str:
    """Lookup jurisdiction_id by manifest_agency_name, filtering within the
    given county_fips"""
    jid = _get_JID()
    jids_matching_county_fips = jid.loc[jid["fips"] == county_fips]

    # Some jurisdictions in jid.csv have no listed names.
    jids_matching_county_fips = jids_matching_county_fips.dropna(subset=["name"])

    # If only one jurisdiction in the county, assume it's a match
    if len(jids_matching_county_fips) == 1:
        return one(jids_matching_county_fips["jid"])

    raise FipsMergingError
