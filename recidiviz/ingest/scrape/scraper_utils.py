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


"""This file defines utility functions that support scraper functionality
that does not depend on member data.
"""

import base64
import random
import zlib
from typing import Optional, Union

from recidiviz.ingest.models.ingest_info import PLURALS, IngestInfo, IngestObject
from recidiviz.ingest.models.ingest_object_hierarchy import get_ancestor_class_sequence
from recidiviz.ingest.scrape.task_params import ScrapedData
from recidiviz.utils import environment, secrets

# We add a random session in order to rotate the IPs from luminati.
PROXY_USER_TEMPLATE = "{}-session-{}"


def get_value_from_html_tree(html_tree, attribute_value, attribute_name="id"):
    """Retrieves the value of an html tag from the given html tree. The
    tag is chosen based on a given attribute value on an attribute
    name that can optionally be set, but is 'id' by default.. If there
    are multiple matches, only the first is returned.

    Args:
        html_tree: (string) html of the scraped page.
        attribute_value: (string) the attribute value of the tag we are trying
            to retrieve.
        attribute_name: (string) the attribute name of the tag we are trying to
            retrieve [default: 'id'].

    Returns:
        A string representing the value of the id from the html page.

    """
    html_obj = html_tree.cssselect(f"[{attribute_name}={attribute_value}]")
    if html_obj:
        return html_obj[0].get("value")
    return None


def _one(ingest_object: str, parent: Optional[IngestObject]):
    none_found = ValueError(
        f"IngestInfo did not have a single {ingest_object} " f"as expected."
    )
    if parent is None:
        raise none_found
    if ingest_object in PLURALS:
        lst = getattr(parent, PLURALS[ingest_object])
        if not lst:
            raise none_found
        if len(lst) > 1:
            raise ValueError(
                f"IngestInfo had {len(lst)} {ingest_object}s, " f"expected one."
            )
        return lst[0]
    elt = getattr(parent, ingest_object)
    if elt is None:
        raise none_found
    return elt


def one(
    ingest_object: str, ingest_info_or_scraped_data: Union[IngestInfo, ScrapedData]
):
    """Convenience function to return the single descendant of an IngestInfo
    object. For example, |one('arrest', ingest_info)| returns the single arrest
    of the single booking of the single person in |ingest_info| and raises an
    error if there are zero or multiple people, bookings, or arrests."""
    if ingest_info_or_scraped_data is None:
        raise ValueError("No ScrapedData or IngestInfo was found.")
    if isinstance(ingest_info_or_scraped_data, ScrapedData):
        ingest_info = ingest_info_or_scraped_data.ingest_info
    else:
        ingest_info = ingest_info_or_scraped_data

    hierarchy_sequence = get_ancestor_class_sequence(ingest_object)
    parent = ingest_info
    for hier_class in hierarchy_sequence:
        parent = _one(hier_class, parent)

    return _one(ingest_object, parent)


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
    if not environment.in_gcp() or use_test:
        return None

    user_var = "proxy_user"
    pass_var = "proxy_password"

    proxy_url = secrets.get_secret("proxy_url")

    if proxy_url is None:
        raise Exception("No proxy url")

    # On the proxy side, a random ip is chosen for a session it has not seen
    # so collisions can still happen so we increase the integer to reduce the
    # odds.
    base_proxy_user = secrets.get_secret(user_var)
    proxy_user = PROXY_USER_TEMPLATE.format(base_proxy_user, random.random())
    proxy_password = secrets.get_secret(pass_var)

    if (base_proxy_user is None) or (proxy_password is None):
        raise Exception("No proxy user/pass")

    proxy_credentials = proxy_user + ":" + proxy_password
    proxy_request_url = "http://" + proxy_credentials + "@" + proxy_url

    proxies = {"http": proxy_request_url, "https": proxy_request_url}

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
    in_prod = environment.in_gcp()
    if not in_prod:
        user_agent_string = (
            "For any issues, concerns, or rate constraints,"
            "e-mail alerts@recidiviz.com"
        )
    else:
        user_agent_string = secrets.get_secret("user_agent")

    if not user_agent_string:
        raise Exception("No user agent string")

    headers = {"User-Agent": user_agent_string}
    return headers


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
    return base64.b64encode(zlib.compress(s.encode(), level)).decode()


def decompress_string(s):
    """Uses the built in DEFLATE algorithm to decompress the string.

    Args:
        s: The string to be decompressed

    Returns:
        The decompressed string
    """
    return zlib.decompress(base64.b64decode(s.encode())).decode()
