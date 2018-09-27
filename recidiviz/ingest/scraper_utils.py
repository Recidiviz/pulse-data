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


"""This file defines utility functions that support scraper functionality
that does not depend on member data.
"""

from datetime import date
import logging
import random
import string
import dateutil.parser as parser

from google.appengine.ext import ndb
from recidiviz.models import env_vars
from recidiviz.utils import environment


def parse_date_string(date_string, person_id):
    """Converts string describing date to Python date object

    Dates are expressed differently in different records,
    typically following one of these patterns:

        "07/2001",
        "12/21/1991",
        "06/14/13", etc.

    This function parses several common variants and returns a datetime.

    Args:
        date_string: (string) Scraped string containing a date
        person_id: (string) Person ID this date is for, for logging

    Returns:
        Python date object representing the date parsed from the string, or
        None if string wasn't one of our expected values (this is common,
        often NONE or LIFE are put in for these if life sentence).

    """
    if date_string:
        try:
            result = parser.parse(date_string)
            result = result.date()
        except ValueError:
            logging.debug("Couldn't parse date string '%s' for person: %s",
                          date_string, person_id)
            return None

        # If month-only date, manually force date to first of the month.
        if len(date_string.split("/")) == 2:
            result = result.replace(day=1)

    else:
        return None

    return result


def generate_id(entity_kind):
    """Generate unique, 10-digit alphanumeric ID for entity kind provided

    Generates a new 10-digit alphanumeric ID, checks it for uniqueness for
    the entity type provided, and retries if needed until unique before
    returning a new ID.

    Args:
        entity_kind: (ndb model class, e.g. us_ny.UsNyPerson) Entity kind to
            check uniqueness of generated id.

    Returns:
        The new ID / key name (string)
    """
    new_id = ''.join(random.choice(string.ascii_uppercase +
                                   string.ascii_lowercase +
                                   string.digits) for _ in range(10))

    test_key = ndb.Key(entity_kind, new_id)
    key_result = test_key.get()

    if key_result is not None:
        # Collision, try again
        return generate_id(entity_kind)

    return new_id


def normalize_key_value_row(row_data):
    """Removes extraneous whitespace from scraped data

    Removes extraneous (leading, trailing, internal) whitespace from scraped
    data.

    Args:
        row_data: (list) One row of data in a list (key/value pair)

    Returns:
        Tuple of cleaned strings, in the order provided.
    """
    key = ' '.join(row_data[0].text_content().split())
    value = ' '.join(row_data[1].text_content().split())
    return key, value


def calculate_age(birth_date):
    """Converts birth date to age during current scrape.

    Determines age of person based on her or his birth date. Note: We don't
    know the timezone of birth, so we use local time for us. Result may be
    off by up to a day.

    Args:
        birth_date: (date) Date of birth as reported by prison system

    Returns:
        (int) Age of person
    """
    today = date.today()
    age = today.year - birth_date.year - ((today.month, today.day) <
                                          (birth_date.month,
                                           birth_date.day))

    return age


def get_proxies(use_test=False):
    """Retrieves proxy username/pass from environment variables

    Retrieves proxy information to use in requests to third-party
    services. If not in production environment, defaults to test proxy
    credentials (so problems during test runs don't risk our main proxy
    IP's reputation).

    Args:
        use_test: (bool) Use test proxy credentials, not prod

    Returns:
        Proxies dict for requests library, in the form:
            {'<protocol>': '<http://<proxy creds>@<proxy url>'}

    Raises:
        Exception: General exception, since scraper cannot
        proceed without this

    """
    in_prod = environment.in_prod()

    if not in_prod or use_test:
        user_var = "test_proxy_user"
        pass_var = "test_proxy_password"
    else:
        user_var = "proxy_user"
        pass_var = "proxy_password"

    proxy_url = env_vars.get_env_var("proxy_url", None)

    proxy_user = env_vars.get_env_var(user_var, None)
    proxy_password = env_vars.get_env_var(pass_var, None)

    if (proxy_user is None) or (proxy_password is None):
        raise Exception("No proxy user/pass")

    proxy_credentials = proxy_user + ":" + proxy_password
    proxy_request_url = 'http://' + proxy_credentials + "@" + proxy_url

    proxies = {'http': proxy_request_url}

    return proxies


def get_headers():
    """Retrieves headers (e.g., user agent string) from environment
    variables

    Retrieves user agent string information to use in requests to
    third-party services.

    Args:
        N/A

    Returns:
        Headers dict for the requests library, in the form:
            {'User-Agent': '<user agent string>'}

    Raises:
        Exception: General exception, since scraper cannot
        proceed without this

    """
    user_agent_string = env_vars.get_env_var("user_agent", None)

    if not user_agent_string:
        raise Exception("No user agent string")

    headers = {'User-Agent': (user_agent_string)}
    return headers
