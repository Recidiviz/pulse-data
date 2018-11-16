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

from datetime import date, datetime
import logging
import random
import string
import zlib
from lxml.html import HtmlElement

import dateutil.parser as parser

from google.appengine.ext import ndb
from recidiviz.models import env_vars
from recidiviz.utils import environment


def convert_key_to_cells(content, key):
    """Searches for elements in |content| that match a |key| and converts those
    elements, along with their adjacent text, as table cells.

    Args:
        content: (HtmlElement) to be modified
        key: (string) to search for
    """
    matches = content.xpath('.//*[starts-with(normalize-space(text()),"%s")]'
                            % key)
    for match in matches:
        key_element_to_cell(key, match)

def key_element_to_cell(key, key_element):
    """Converts a |key_element| Element to a table cell and tries to modify the
    corresponding value to a cell.

    Args:
        key: (string) the key that |key_element| represents
        key_element: (HtmlElement) the element to be modified
    Returns:
        True if a modification was made and False otherwise.
    """

    # <foo><bar>key</bar>value</foo>
    following_siblings = key_element.xpath('following-sibling::text()')
    if following_siblings:
        following_text = following_siblings[0].strip()
        if following_text:
            key_element.tag = 'td'
            following_cell = HtmlElement(following_text)
            following_cell.tag = 'td'
            key_element.addnext(following_cell)
            return True

    # <foo>key</foo><bar>value</bar>
    if key_element.getnext() is not None:
        key_element.tag = 'td'
        key_element.getnext().tag = 'td'
        return True

    # <foo>key<bar>value</bar></foo>
    if len(key_element) == 1:
        key_cell = HtmlElement(key)
        key_cell.tag = 'td'
        value_cell = key_element[0]
        value_cell.tag = 'td'
        value_cell.addprevious(key_cell)
        return True

    # <foo>key : value</foo>
    text = key_element.text.strip()
    if text.startswith(key):
        text = text[len(key):].strip().strip(':').strip()
        if text != '':
            key_cell = HtmlElement(key)
            key_cell.tag = 'td'
            value_cell = HtmlElement(text)
            value_cell.tag = 'td'
            key_element.insert(0, key_cell)
            key_element.insert(1, value_cell)
            return True

    return False

def parse_date_string(date_string, person_id=None):
    """Converts string describing date to Python date object

    Dates are expressed differently in different records,
    typically following one of these patterns:

        "07/2001",
        "12/21/1991",
        "06/14/13",
        "02/15/1990 4:32", etc.

    This function parses several common variants and returns a datetime.

    Args:
        date_string: (string) Scraped string containing a date
        person_id: (string) Person ID this date is for, for logging

    Returns:
        Python date object representing the date parsed from the string, or
        None if string wasn't one of our expected values (this is common,
        often NONE or LIFE are put in for these if life sentence).

    """
    if date_string and not date_string.isspace():
        try:
            # The year and month in the `default` do not matter. This protects
            # against an esoteric bug: parsing a 2-integer date with a month of
            # February while the local machine date is the 29th or higher (or
            # 30th or higher if the year of the string is a leap year).
            # Without a default day of `01` to fall back on, it falls back to
            # the local machine day, which will fail because February does not
            # have that many days.
            result = parser.parse(date_string, default=datetime(2018, 1, 1))
            result = result.date()
        except ValueError, e:
            if person_id:
                logging.debug("Couldn't parse date string '%s' for person: %s",
                              date_string, person_id)
            else:
                logging.debug("Couldn't parse date string '%s'",
                              date_string)

            logging.debug(str(e))
            return None

        # If month-only date, manually force date to first of the month.
        if len(date_string.split("/")) == 2 or len(date_string.split("-")) == 2:
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


def calculate_age(birthdate, check_date=None):
    """Converts birth date to age during current scrape.

    Determines age of person based on her or his birth date. Note: We don't
    know the timezone of birth, so we use local time for us. Result may be
    off by up to a day.

    Args:
        birthdate: (date) Date of birth as reported by prison system
        check_date: (date) The date to compare against, defaults to today

    Returns:
        (int) Age of person
    """
    if check_date is None:
        check_date = date.today()

    return None if birthdate is None else \
        check_date.year - birthdate.year - \
        ((check_date.month, check_date.day) < (birthdate.month, birthdate.day))


def get_id_value_from_html_tree(html_tree, html_id):
    """Retrieves the value of the given id from the given html tree.

    Args:
        html_tree: (string) html of the scraped page.
        html_id: (string) the html id we are trying to retrieve.

    Returns:
        A string representing the value of the id from the html page.
    """
    html_obj = html_tree.cssselect('[id={}]'.format(html_id))
    if html_obj:
        return html_obj[0].get('value')
    return None


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

    if proxy_url is None:
        raise Exception("No proxy url")

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

    headers = {'User-Agent': user_agent_string}
    return headers


def currency_to_float(currency):
    """Converts a given currency string value to a float.  This function has
    limited uses currently as it only works when the given currency starts with
    a currency symbol.

    Args:
        currency: A string representing the dollar currency dollar amount.

    Returns:
        A float representing the currency.
    """
    try:
        return float(currency[1:].replace(',', ''))
    except ValueError:
        logging.debug("Could not convert '%s' to float", currency)
        return None


def compress_string(s, level=1):
    """Uses the built in DEFLATE algorithm to compress the string.

    Args:
        s: The string to be compressed
        level: The level of compression, 1 is almost always the right answer
            here as the multi-level compression is much slower and provides
            very little benefit.

    Returns:
        The compressed string
    """
    return zlib.compress(s, level)


def decompress_string(s):
    """Uses the built in DEFLATE algorithm to decompress the string.

    Args:
        s: The string to be decompressed

    Returns:
        The decompressed string
    """
    return zlib.decompress(s)
