# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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

# pylint: disable=protected-access

"""
populate_test_db

Used to set up the local server environment for testing. Will not run on
production server.

You will need a secrets.yaml file with appropriate secrets to use this tool.
"""


from http import HTTPStatus
import logging

from flask import Blueprint
import yaml

from recidiviz.utils import environment, secrets
from recidiviz.utils.auth import authenticate_request

test_populator = Blueprint('test_populator', __name__)


@test_populator.route('/load_secrets')
@test_populator.route('/clear')  # kept for backwards-compatibility
@environment.local_only
@authenticate_request
def load_secrets():
    """Request handler to re-populate secrets


    Example queries:

        http://localhost:8080/test_populator/clear

    Params:
        N/A

    Returns:
        HTTP 200 if successful
        HTTP 400 if not
    """
    _load_secrets()

    logging.info("Completed loading secrets.")
    return ('', HTTPStatus.OK)


def _load_secrets():
    """Load secrets (proxy and user agent info) into datastore

    Get secrets (e.g. proxy info and user agent string) from secrets.yaml and
    write them into datastore.

    Args:
        N/A

    Returns:
        N/A
    """
    logging.info("Adding secrets...")

    secrets.clear_secrets()

    with open("secrets.yaml", 'r') as ymlfile:
        for name, value in yaml.load(ymlfile)['secrets'].items():
            secrets.set_secret(name, value)
