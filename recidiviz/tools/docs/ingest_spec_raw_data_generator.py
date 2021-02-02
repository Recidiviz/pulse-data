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

"""A script which generates the "Raw Data Description" portion of a State Ingest Specification.

Parses the files available under `recidiviz/ingest/direct/regions/{state_code}/raw_data/` to produce documentation
which is suitable to be added to the state ingest specification. The docs will be printed to stdout upon completion.

Example usage:
python -m recidiviz.tools.docs.ingest_spec_raw_data_generator --state-code US_ND
"""

import argparse
import logging

from recidiviz.common.constants import states
from recidiviz.ingest.direct.direct_ingest_documentation_generator import DirectIngestDocumentationGenerator


def main(state_code: str) -> None:
    documentation_generator = DirectIngestDocumentationGenerator()
    documentation = documentation_generator.generate_raw_file_docs_for_region(state_code)
    print(documentation)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--state-code',
                        required=True,
                        help='The state to generate documentation for.',
                        choices=[state.value for state in states.StateCode])

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    main(state_code=args.state_code.lower())
