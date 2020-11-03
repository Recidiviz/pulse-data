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
"""Test script to verify manual upload is working correctly.

Example usage:
python -m recidiviz.tests.tools.development_scripts.justice_counts --manifest-file path/to/manifest.yaml
python -m recidiviz.tests.tools.development_scripts.justice_counts \
    --manifest-file recidiviz/tests/tools/justice_counts/fixtures/report1/manifest.yaml
"""


import argparse
import logging

import recidiviz
from recidiviz.persistence.database.base_schema import JusticeCountsBase
from recidiviz.tools.justice_counts import manual_upload
from recidiviz.tests.utils import fakes


def _create_parser():
    """Creates the CLI argument parser."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--manifest-file', required=False, type=str,
        help="The yaml describing how to ingest the data"
    )
    parser.add_argument(
        '--log', required=False, default='INFO', type=logging.getLevelName,
        help="Set the logging level"
    )
    return parser


def _configure_logging(level):
    root = logging.getLogger()
    root.setLevel(level)


if __name__ == '__main__':
    arg_parser = _create_parser()
    arguments = arg_parser.parse_args()

    _configure_logging(arguments.log)

    # TODO(#4386): Currently this starts a local postgres and talks to it as we aren't able to talk to a postgres
    # instance in GCP from local. Long-term there are a few options:
    # - Upload the files to a gcs bucket to be processed by the app
    #   - This is straightforward, but to see what is happening you have to go to GCP
    # - Create an endpoint that takes the parsed `Report` object
    #   - This allows any conversion errors to show up locally, and persistence errors to be communicated back
    #   - However, then `Report` becomes an api and can't evolve as easily (which maybe is true no matter what once
    #     manifest files have been created?)
    recidiviz.called_from_test = True
    fakes.start_on_disk_postgresql_database()
    fakes.use_on_disk_postgresql_database(JusticeCountsBase)

    manual_upload.ingest(arguments.manifest_file)

    # Don't cleanup the database so that user can query the data afterward.
    logging.info("To query the data, connect to the local database with `psql --dbname=recidiviz_test_db`")
