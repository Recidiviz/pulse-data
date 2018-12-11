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

"""Secrets for use at runtime."""

import logging

from google.appengine.ext import ndb


class Secret(ndb.Model):
    """Secrets to be used by the application.

    This is where sensitive info such as usernames and passwords to third-party
    services are kept. They're added manually in the gcloud console.

    Entity keys are expected to be in the format region_varname, to enforce
    uniqueness.

    Attributes:
        name: (string) Variable name
        value: (string) Variable value, set by admins in gcloud console
    """
    name = ndb.StringProperty()
    value = ndb.StringProperty()

CACHED_SECRETS = {}

def get_secret(secret_name):
    """Retrieve environment variable from local cache or datastore

    Helper function for scrapers to get secrets. First checks local cache, if
    not found will pull from datastore and populate local cache.

    Args:
        secret_name: Name of the secret to retrieve

    Returns:
        Secret value if found otherwise None
    """
    value = CACHED_SECRETS.get(secret_name)

    if not value:
        datastore_result = Secret.query(Secret.name == secret_name).get()

        if datastore_result:
            value = str(datastore_result.value)
            CACHED_SECRETS[secret_name] = value

        else:
            logging.error("Couldn't retrieve env var: %s." % secret_name)

    return value
