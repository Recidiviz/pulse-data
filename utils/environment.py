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

"""Tools for working with environment variables.

Environment variables in Google App Engine are parsed via .yml files, which are
static and cannot be trivially made environment-specific. So this includes
functionality for determining which environment we are in and loading the
appropriate variables.
"""


import os
import logging
import yaml


def in_prod():
    """ Check whether we're currently running on local dev machine or in prod

    Checks whether the current instance is running hosted on GAE (if not, likely
    running on local devserver).

    Args:
        N/A

    Returns:
        True if on hosted GAE instance
        False if not
    """
    current_environment = os.getenv('SERVER_SOFTWARE', '')
    return current_environment.startswith('Google App Engine/')


def local_only(func):
    """Decorator function to verify request only runs locally

    Decorator function to check run environment. If prod / served on GAE,
    exits before any work can be done.

    Args:
        N/A

    Returns:
        If running locally, results of decorated function.
        If not, nothing.
    """

    def check_env(request_handler, *args, **kwargs):
        """Decorator child-method to fail if runtime is in prod

        This is the function the decorator uses to test whether or not our
        runtime is in prod, and if so error out.

        Args:
            request_handler: The function being decorated (we expect a
                webapp2.RequestHandler instance)
            args, kwargs: Any arguments passed to that request handler

        Returns:
            Output of the decorated function, if running locally
            HTTP 500 and error logs, if running in prod
        """

        deployed = in_prod()

        if deployed:
            # Production environment - fail
            logging.error("This API call is not allowed in production.")
            request_handler.response.write('Not available, see service logs.')
            request_handler.response.set_status(500)
            return None

        # Local development server - continue
        logging.info("Test environment, proceeding.")

        return func(request_handler, *args, **kwargs)

    return check_env


def load_local_vars():
    """Load environmental variables from local file

    Load local.yaml and return dictionary of contents. For use in test / local
    environments.

    Args:
        N/A

    Returns:
        Dict of local environment variables
    """
    with open("local.yaml", 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

    env_vars = cfg['env_vars']

    return env_vars
