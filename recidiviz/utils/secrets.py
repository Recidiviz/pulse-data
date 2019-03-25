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

"""Secrets for use at runtime."""

import logging
from typing import Dict

from google.cloud import datastore
from recidiviz.utils import environment

_ds = None


def ds():
    global _ds
    if not _ds:
        _ds = environment.get_datastore_client()
    return _ds


@environment.test_only
def clear_ds():
    global _ds
    _ds = None

SECRET_KIND = 'Secret'
CACHED_SECRETS: Dict[str, str] = {}

def get_secret(name):
    """Retrieve secret from local cache or datastore

    Helper function for scrapers to get secrets. First checks local cache, if
    not found will pull from datastore and populate local cache.

    Args:
        name: Name of the secret to retrieve

    Returns:
        Secret value if found otherwise None
    """
    value = CACHED_SECRETS.get(name)

    if not value:
        query = ds().query(kind=SECRET_KIND)
        query.add_filter('name', '=', name)
        result = next(iter(query.fetch()), None)

        if result:
            value = str(result['value'])
            CACHED_SECRETS[name] = value

        else:
            logging.error("Couldn't retrieve env var: %s.", name)

    return value

@environment.local_only
def clear_secrets():
    query = ds().query(kind=SECRET_KIND)
    ds().delete_multi(secret.key for secret in query.fetch())

@environment.local_only
def set_secret(name, value):
    secret = datastore.Entity(key=ds().key(SECRET_KIND, name))
    secret.update({'name': name, 'value': value})
    ds().put(secret)
