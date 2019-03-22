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
Tool to automatically backfill jurisdiction_id onto all region's
manifest.yaml.

Note: This tool will NOT add a jurisdiction_id to a region where multiple
jurisdiction_ids exist for the corresponding fips.
"""

import glob
import os

import pandas as pd
import yaml
from ruamel.yaml import YAML

import recidiviz.ingest.scrape.regions

_REGIONS_DIR = os.path.dirname(recidiviz.ingest.scrape.regions.__file__)
_DATA_SETS_DIR = os.path.dirname(recidiviz.common.__file__) + '/data_sets'

_JID = pd.read_csv(_DATA_SETS_DIR + '/jid.csv')

# Float between [0, 1] which sets the required fuzzy matching certainty
_FUZZY_MATCH_CUTOFF = 0.75


def main():
    """Script to backfill jurisdiction ids on manifest files"""

    all_manifests_filenames = glob.glob(_REGIONS_DIR + '/**/manifest.yaml')

    for manifest_filename in all_manifests_filenames:
        # Read county_name from manifest & state
        region_name = manifest_filename.split('/')[-2]
        county_name = ' '.join(region_name.split('_')[2:])

        # Skip state level manifest files
        if not county_name:
            continue

        with open(manifest_filename, 'r+') as file:
            y_dict = yaml.load(file)

            jid = str(y_dict.get('jurisdiction_id'))
            jid = jid.zfill(8)
            y_dict['jurisdiction_id'] = jid

            file.seek(0)
            y = YAML()
            y.default_flow_style = False
            y.dump(y_dict, file)
            file.truncate()


if __name__ == "__main__":
    main()
