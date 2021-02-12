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

"""A script which generates the directories and files needed for direct ingest, given a state code.
Example usage:
python -m recidiviz.tools.ingest.direct.ingest_files_generator --region-code US_TN
"""

import argparse
import logging

from recidiviz.common.constants import states
from recidiviz.ingest.direct.direct_ingest_files_generator import DirectIngestFilesGenerator


def main(region_code: str) -> None:
    generator = DirectIngestFilesGenerator(region_code)
    generator.generate_all_new_dirs_and_files()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--region-code',
                        required=True,
                        help='The state to generate ingest files for.',
                        choices=[state.value for state in states.StateCode])

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    main(region_code=args.region_code.lower())
