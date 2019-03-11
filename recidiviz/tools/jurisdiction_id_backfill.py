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
Tool to automatically backfill jurisdiction_id onto all region's
manifest.yaml.

Note: This tool will NOT add a jurisdiction_id to a region where multiple
jurisdiction_ids exist for the corresponding fips.
"""

import glob
import os
from typing import Optional

import pandas as pd
import us
from more_itertools import one

import recidiviz.ingest.scrape.regions
from recidiviz.common import fips, fips_fuzzy_matching

_REGIONS_DIR = os.path.dirname(recidiviz.ingest.scrape.regions.__file__)
_DATA_SETS_DIR = os.path.dirname(recidiviz.common.__file__) + '/data_sets'

_JID = pd.read_csv(_DATA_SETS_DIR + '/jid.csv')

# Float between [0, 1] which sets the required fuzzy matching certainty
_FUZZY_MATCH_CUTOFF = 0.75


def main():
    all_manifests_filenames = glob.glob(_REGIONS_DIR + '/**/manifest.yaml')

    errors = set()
    for manifest_filename in all_manifests_filenames:
        # Read county_name from manifest & state
        region_name = manifest_filename.split('/')[-2]
        state_name = region_name.split('_')[1]
        county_name = ' '.join(region_name.split('_')[2:])

        # Skip state level manifest files
        if not county_name:
            continue

        county_fips = _to_county_fips(county_name,
                                      us.states.lookup(state_name))
        with open(manifest_filename, 'a') as yaml_file:
            jid = _to_jurisdiction_id(county_fips)

            if not jid:
                errors.add(county_name)
                continue

            yaml_file.write('jurisdiction_id: ' + str(jid) + '\n')

    print(len(errors))
    print('Failed counties: ' + str(errors))


def _to_county_fips(manifest_county_name: str, state: us.states) -> int:
    """Lookup fips by manifest_county_name, filtering within the given state"""
    # pylint: disable=protected-access
    fips_for_state = fips.get_fips_for(state)
    # pylint: disable=protected-access
    actual_county_name = fips_fuzzy_matching.best_match(
        manifest_county_name, fips_for_state.index, _FUZZY_MATCH_CUTOFF)

    return fips_for_state.at[actual_county_name, 'fips']


def _to_jurisdiction_id(county_fips: int) -> Optional[str]:
    """Lookup jurisdction_id by manifest_agency_name, filtering within the
    given county_fips"""
    jids_matching_county_fips = _JID.loc[_JID['fips'] == county_fips]

    # Some jurisdictions in jid.csv have no listed names.
    jids_matching_county_fips = \
        jids_matching_county_fips.dropna(subset=['name'])

    # If only one jurisdiction in the county, assume it's a match
    if len(jids_matching_county_fips) == 1:
        return one(jids_matching_county_fips['jid'])

    return None


if __name__ == "__main__":
    main()
