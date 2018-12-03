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

"""Tests for ingest/scraper_utils.py."""


from datetime import date
from lxml import html
from mock import patch
import pytest

from google.appengine.ext import ndb
from google.appengine.ext import testbed
from recidiviz.ingest import scraper_utils
from recidiviz.models import env_vars
from recidiviz.models.person import Person


def test_parse_date_string_american_date():
    date_string = "11/20/1991"

    result = scraper_utils.parse_date_string(date_string, "12345")
    assert result is not None
    assert result == date(1991, 11, 20)


def test_parse_date_string_american_date_two_digit_year():
    date_string = "11/20/91"

    result = scraper_utils.parse_date_string(date_string, "12345")
    assert result is not None
    assert result == date(1991, 11, 20)


def test_parse_date_string_american_date_no_day():
    date_string = "11/1991"

    result = scraper_utils.parse_date_string(date_string, "12345")
    assert result is not None
    assert result == date(1991, 11, 1)


def test_parse_date_string_american_date_no_day_two_digit_year():
    date_string = "11/91"

    result = scraper_utils.parse_date_string(date_string, "12345")
    assert result is not None
    assert result == date(1991, 11, 1)


def test_parse_date_string_international_date():
    date_string = "2018-05-24"

    result = scraper_utils.parse_date_string(date_string, "12345")
    assert result is not None
    assert result == date(2018, 5, 24)


def test_parse_date_string_international_date_no_day():
    date_string = "2018-03"

    result = scraper_utils.parse_date_string(date_string, "12345")
    assert result is not None
    assert result == date(2018, 3, 1)


def test_parse_date_string_none():
    assert scraper_utils.parse_date_string(None, "12345") is None


def test_parse_date_string_garbage():
    assert scraper_utils.parse_date_string("whatever", "12345") is None


def test_parse_date_string_empty():
    assert scraper_utils.parse_date_string("", "12345") is None


def test_parse_date_string_space():
    assert scraper_utils.parse_date_string("    ", "12345") is None


class TestGenerateId(object):
    """Tests for the generate_id method in the module."""

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        context = ndb.get_context()
        context.set_memcache_policy(False)
        context.clear_cache()

    def teardown_method(self, _test_method):
        self.testbed.deactivate()

    def test_generate_id(self):
        new_id = scraper_utils.generate_id(Person)
        assert new_id is not None
        assert new_id.isalnum()


def test_normalize_key_value_row():
    key_html = '<td headers="crime"> BURGLARY 2ND                   &nbsp;</td>'
    value_html = '<td headers="class">C  &nbsp;</td>'

    key = html.fromstring(key_html)
    value = html.fromstring(value_html)

    normalized = scraper_utils.normalize_key_value_row([key, value])
    assert normalized == ("BURGLARY 2ND", "C")


def test_normalize_key_value_row_with_nesting():
    key_html = '''
        <td scope="row" id="t3f">
            <a href="http://www.doccs.ny.gov/univinq/fpmsdoc.htm#pht" 
            title="Definition of Parole Hearing Type">
            Parole Hearing Type</a></td>
        '''
    value_html = '<td headers="t3f">' \
                 'APPROVED OPEN DATE/6 MO AFT INIT APPEAR   &nbsp;</td>'

    key = html.fromstring(key_html)
    value = html.fromstring(value_html)

    normalized = scraper_utils.normalize_key_value_row([key, value])
    assert normalized == ("Parole Hearing Type",
                          "APPROVED OPEN DATE/6 MO AFT INIT APPEAR")


def test_normalize_key_value_row_with_nesting_empty_value():
    key_html = '''
        <td scope="row" id="t3e">
             <a href="http://www.doccs.ny.gov/univinq/fpmsdoc.htm#phd" 
             title="Definition of Parole Hearing Date">
             Parole Hearing Date</a></td>
        '''
    value_html = '<td headers="t3e"> &nbsp;</td>'

    key = html.fromstring(key_html)
    value = html.fromstring(value_html)

    normalized = scraper_utils.normalize_key_value_row([key, value])
    assert normalized == ("Parole Hearing Date", "")


def test_calculate_age_earlier_month():
    birthdate = date(1989, 6, 17)
    check_date = date(2014, 4, 15)

    assert scraper_utils.calculate_age(birthdate, check_date) == 24


def test_calculate_age_same_month_earlier_date():
    birthdate = date(1989, 6, 17)
    check_date = date(2014, 6, 16)

    assert scraper_utils.calculate_age(birthdate, check_date) == 24


def test_calculate_age_same_month_same_date():
    birthdate = date(1989, 6, 17)
    check_date = date(2014, 6, 17)

    assert scraper_utils.calculate_age(birthdate, check_date) == 25


def test_calculate_age_same_month_later_date():
    birthdate = date(1989, 6, 17)
    check_date = date(2014, 6, 18)

    assert scraper_utils.calculate_age(birthdate, check_date) == 25


def test_calculate_age_later_month():
    birthdate = date(1989, 6, 17)
    check_date = date(2014, 7, 11)

    assert scraper_utils.calculate_age(birthdate, check_date) == 25


def test_calculate_age_birthdate_unknown():
    assert scraper_utils.calculate_age(None) is None


class TestGetProxies(object):
    """Tests for the get_proxies method in the module."""

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        context = ndb.get_context()
        context.set_memcache_policy(False)
        context.clear_cache()

    def teardown_method(self, _test_method):
        self.testbed.deactivate()
        env_vars.LOCAL_VARS.clear()

    @patch('recidiviz.utils.environment.in_prod')
    def test_get_proxies_local(self, mock_in_prod):
        mock_in_prod.return_value = False

        write_env_var('proxy_url', 'proxy.biz/')
        write_env_var('test_proxy_user', 'user')
        write_env_var('test_proxy_password', 'password')

        proxies = scraper_utils.get_proxies()
        assert proxies == {'http': 'http://user:password@proxy.biz/'}

    @patch('recidiviz.utils.environment.in_prod')
    def test_get_proxies_prod(self, mock_in_prod):
        mock_in_prod.return_value = True

        write_env_var('proxy_url', 'proxy.net/')
        write_env_var('proxy_user', 'real_user')
        write_env_var('proxy_password', 'real_password')

        proxies = scraper_utils.get_proxies()
        assert proxies == {'http': 'http://real_user:real_password@proxy.net/'}

    @patch('recidiviz.utils.environment.in_prod')
    def test_get_proxies_local_no_user(self, mock_in_prod):
        mock_in_prod.return_value = True

        write_env_var('proxy_url', 'proxy.net/')
        write_env_var('proxy_password', 'real_password')

        with pytest.raises(Exception) as exception:
            scraper_utils.get_proxies()
        assert exception.value.message == 'No proxy user/pass'

    @patch('recidiviz.utils.environment.in_prod')
    def test_get_proxies_local_no_password(self, mock_in_prod):
        mock_in_prod.return_value = False

        write_env_var('proxy_url', 'proxy.biz/')
        write_env_var('test_proxy_user', 'user')

        with pytest.raises(Exception) as exception:
            scraper_utils.get_proxies()
        assert exception.value.message == 'No proxy user/pass'

    @patch('recidiviz.utils.environment.in_prod')
    def test_get_proxies_local_no_url(self, mock_in_prod):
        mock_in_prod.return_value = False

        write_env_var('test_proxy_user', 'user')
        write_env_var('test_proxy_password', 'password')

        with pytest.raises(Exception) as exception:
            scraper_utils.get_proxies()
        assert exception.value.message == 'No proxy url'


class TestGetHeaders(object):
    """Tests for the get_headers method in the module."""

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        context = ndb.get_context()
        context.set_memcache_policy(False)
        context.clear_cache()

    def teardown_method(self, _test_method):
        self.testbed.deactivate()
        env_vars.LOCAL_VARS.clear()

    def test_get_headers(self):
        user_agent = 'test_user_agent'
        write_env_var('user_agent', user_agent)

        headers = scraper_utils.get_headers()
        assert headers == {'User-Agent': user_agent}

    def test_get_headers_missing_user_agent(self):
        with pytest.raises(Exception) as exception:
            scraper_utils.get_headers()
        assert exception.value.message == 'No user agent string'


def write_env_var(name, value):
    env_vars.LOCAL_VARS[name] = value
