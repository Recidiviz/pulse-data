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

"""Environment variables for use at runtime.

Whereas traditional environment variables are simple key-value pairs, we have
an extra dimension for region, to allow us to define variables that vary
between regional scraping and//or calculation.
"""

import logging

from google.appengine.ext import ndb


class EnvironmentVariable(ndb.Model):
    """Environment variables loaded at runtime by the application (not from
    source)

    Environment variables pulled in at runtime. This is where sensitive info
    such as usernames and passwords to third-party services are kept. They're
    added manually in the gcloud console.

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

LOCAL_VARS = {}

def get_env_var(var_name):
    """Retrieve environment variable from local dict or datastore

    Helper function for scrapers to get environment variables. First checks
    local environment variables, if not found will pull from datastore and
    populate local environment variable.

    Args:
        var_name: Variable name to retrieve

    Returns:
        Variable value if found
        None if not
    """
    value = LOCAL_VARS.get(var_name)

    if not value:
        datastore_result = EnvironmentVariable.query(ndb.AND(
            EnvironmentVariable.region == 'all',
            EnvironmentVariable.name == var_name)).get()

        if datastore_result:
            value = str(datastore_result.value)
            LOCAL_VARS[var_name] = value

        else:
            logging.error("Couldn't retrieve env var: %s." % var_name)

    return value
