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

# pylint: disable=unused-import,wrong-import-order

"""Tests for utils/regions.py."""


import pytest

from ..context import utils
from google.appengine.api import memcache
from google.appengine.ext import ndb
from google.appengine.ext import testbed
from mock import patch, mock_open
from recidiviz.utils import regions


MANIFEST_CONTENTS = """
    regions:
      us_ny:
        agency_name: Department of Corrections and Community Supervision
        agency_type: prison
        base_url: http://nysdoccslookup.doccs.ny.gov
        entity_kinds:
          person: UsNyPerson
          record: UsNyRecord
        names_file: us_ny_names.csv
        queues:
        - us-ny-scraper
        region_code: us_ny
        region_name: New York State
        scraper_package: us_ny
        timezone: America/New_York
      us_fl:
        agency_name: Department of Corrections
        agency_type: prison
        base_url: http://www.dc.state.fl.us/OffenderSearch/Search.aspx
        entity_kinds:
          person: UsFlPerson
          record: UsFlRecord
          snapshot: UsFlSnapshot
        names_file: us_fl_names.csv
        params:
          foo: bar
          sha: baz
        queues:
        - us-fl-scraper
        - a-different-queue
        region_code: us_fl
        region_name: Florida State
        scraper_class: a_different_scraper
        scraper_package: us_fl
        timezone: America/New_York
    """

FULL_MANIFEST = {
    'regions': {
        'us_ny': {
            'agency_name': 'Department of Corrections and '
                           'Community Supervision',
            'agency_type': 'prison',
            'base_url': 'http://nysdoccslookup.doccs.ny.gov',
            'entity_kinds': {
                'person': 'UsNyPerson',
                'record': 'UsNyRecord'
            },
            'names_file': 'us_ny_names.csv',
            'queues': ['us-ny-scraper'],
            'region_code': 'us_ny',
            'region_name': 'New York State',
            'scraper_package': 'us_ny',
            'timezone': 'America/New_York'
        },
        'us_fl': {
            'agency_name': 'Department of Corrections',
            'agency_type': 'prison',
            'base_url': 'http://www.dc.state.fl.us/OffenderSearch/Search.aspx',
            'entity_kinds': {
                'person': 'UsFlPerson',
                'record': 'UsFlRecord',
                'snapshot': 'UsFlSnapshot'
            },
            'names_file': 'us_fl_names.csv',
            'params': {
                'foo': 'bar',
                'sha': 'baz'
            },
            'queues': ['us-fl-scraper', 'a-different-queue'],
            'region_code': 'us_fl',
            'region_name': 'Florida State',
            'scraper_class': 'a_different_scraper',
            'scraper_package': 'us_fl',
            'timezone': 'America/New_York'
        }
    }
}


def test_load_region_manifest():
    manifest = with_manifest(regions.load_region_manifest)
    assert manifest == FULL_MANIFEST


def test_load_region_manifest_specific():
    manifest = with_manifest(regions.load_region_manifest, 'us_ny')
    assert manifest == FULL_MANIFEST['regions']['us_ny']


def test_load_region_manifest_not_found():
    with pytest.raises(Exception) as exception:
        with patch("__builtin__.open",
                   mock_open(read_data=MANIFEST_CONTENTS)) \
                as mock_file:
            regions.load_region_manifest('us_az')

        assert exception.value.message == "Region 'us_az' not " \
                                          "found in manifest."
        mock_file.assert_called_with('region_manifest.yaml', 'r')


def test_get_supported_regions():
    supported_regions = with_manifest(regions.get_supported_regions)
    assert supported_regions == ['us_ny', 'us_fl']


def test_get_supported_regions_full_manifest():
    supported_regions = with_manifest(regions.get_supported_regions,
                                      full_manifest=True)
    assert supported_regions == FULL_MANIFEST['regions']


def test_validate_region_code_valid():
    assert with_manifest(regions.validate_region_code, 'us_fl')


def test_validate_region_code_invalid():
    assert not with_manifest(regions.validate_region_code, 'us_az')


def test_get_subkind():
    person = with_manifest(regions.get_subkind, 'us_ny', 'Person')
    assert person.__name__ == 'UsNyPerson'


def test_get_name_list_file():
    filename = with_manifest(regions.get_name_list_file, 'us_fl')
    assert filename == 'us_fl_names.csv'


def test_get_scraper_module():
    module = regions.get_scraper_module('us_ny')
    assert module.__name__ == 'recidiviz.ingest.us_ny'


def test_get_scraper():
    scraper = regions.get_scraper('us_ny', 'us_ny_scraper')
    assert type(scraper).__name__ == 'UsNyScraper'


def test_region_class():
    region = with_manifest(regions.Region, 'us_ny')
    assert region.scraper().__name__ == 'recidiviz.ingest.us_ny'
    assert region.get_person_kind().__name__ == 'UsNyPerson'
    assert region.get_record_kind().__name__ == 'UsNyRecord'
    assert region.get_snapshot_kind().__name__ == 'Snapshot'
    assert not region.params
    assert region.queues == ['us-ny-scraper']
    assert region.scraper_class == 'us_ny_scraper'


def test_region_class_with_scraper_class_and_multiple_queues():
    region = with_manifest(regions.Region, 'us_fl')
    assert region.params == {'foo': 'bar', 'sha': 'baz'}
    assert region.queues == ['us-fl-scraper', 'a-different-queue']
    assert region.scraper_class == 'a_different_scraper'


def with_manifest(func, *args, **kwargs):
    with patch("__builtin__.open",
               mock_open(read_data=MANIFEST_CONTENTS)) \
            as mock_file:
        value = func(*args, **kwargs)
        mock_file.assert_called_with('region_manifest.yaml', 'r')
        return value


class TestRegionsCache(object):
    """Tests for caching methods in the module."""

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_memcache_stub()
        ndb.get_context().clear_cache()

    def teardown_method(self, _test_method):
        self.testbed.deactivate()

    def test_get_scraper_from_cache(self):
        scraper = regions.get_scraper_from_cache('us_ny')
        assert type(scraper).__name__ == 'UsNyScraper'

        assert memcache.get('us_ny_scraper_package') == 'us_ny'
        assert memcache.get('us_ny_scraper_class') == 'us_ny_scraper'
