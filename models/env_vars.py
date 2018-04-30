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


from google.appengine.api import memcache
from google.appengine.ext import ndb

import logging


class EnvironmentVariable(ndb.Model):
    """Environment variables loaded at runtime by the application (not from source)

    Environment variables pulled in at runtime. This is where sensitive info such
    as usernames and passwords to third-party services are kept. They're added
    manually in the gcloud console.

    Entity keys are expected to be in the format region_varname, to enforce
    uniqueness.

    Attributes:
        region: (string) Region code, or 'all'
        name: (string) Variable name
        value: (string) Variable value, set by admins in gcloud console
    """
    region = ndb.StringProperty()
    name = ndb.StringProperty()
    value = ndb.StringProperty()


def get_env_var(var_name, region_code):
    """Retrieve environment variable from memcache or datastore

    Helper function for scrapers to get environment variables. Tries to pull
    first from memcache, if not found will pull from datastore and repopulate
    in memcache.

    Args:
        var_name: Variable name to retrieve
        region_code: (string) Region code, or 'None' if global var

    Returns:
        Variable value if found
        None if not
    """
    region_code = "all" if not region_code else region_code
    memcache_name = region_code + "_" + var_name

    value = memcache.get(memcache_name)

    if not value:

        datastore_result = EnvironmentVariable.query(ndb.AND(
            EnvironmentVariable.region == region_code,
            EnvironmentVariable.name == var_name)).get()

        if datastore_result:
            value = str(datastore_result.value)
            memcache.set(key=memcache_name, value=value, time=3600)

        else:
            logging.error("Couldn't retrieve env var: %s for %s region." %
                (var_name, region_code))
            return None

    return value
