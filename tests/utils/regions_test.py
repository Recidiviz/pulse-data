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
from mock import patch, mock_open
from utils import regions


MANIFEST_CONTENTS = """
    regions:
      us_ny:
        region_name: New York State
        agency_name: Department of Corrections and Community Supervision (DOCCS)
        region_code: us_ny
        agency_type: prison
        scrape_queue_name: us-ny-scraper
        base_url: http://nysdoccslookup.doccs.ny.gov
        names_file: us_ny_names.csv
        entity_kinds:
          inmate: UsNyInmate
          record: UsNyRecord
          snapshot: UsNySnapshot
      us_fl:
        region_name: Florida State
        agency_name: Department of Corrections
        region_code: us_fl
        agency_type: prison
        scrape_queue_name: us-fl-scraper
        base_url: http://www.dc.state.fl.us/OffenderSearch/Search.aspx
        names_file: us_fl_names.csv
        entity_kinds:
          inmate: UsFlInmate
          record: UsFlRecord
          snapshot: UsFlSnapshot
    """

FULL_MANIFEST = {
    'regions': {
        'us_ny': {
            'region_name': 'New York State',
            'agency_name': 'Department of Corrections and '
                           'Community Supervision (DOCCS)',
            'region_code': 'us_ny',
            'agency_type': 'prison',
            'scrape_queue_name': 'us-ny-scraper',
            'base_url': 'http://nysdoccslookup.doccs.ny.gov',
            'names_file': 'us_ny_names.csv',
            'entity_kinds': {
                'inmate': 'UsNyInmate',
                'record': 'UsNyRecord',
                'snapshot': 'UsNySnapshot'
            }
        },
        'us_fl': {
            'region_name': 'Florida State',
            'agency_name': 'Department of Corrections',
            'region_code': 'us_fl',
            'agency_type': 'prison',
            'scrape_queue_name': 'us-fl-scraper',
            'base_url': 'http://www.dc.state.fl.us/OffenderSearch/Search.aspx',
            'names_file': 'us_fl_names.csv',
            'entity_kinds': {
                'inmate': 'UsFlInmate',
                'record': 'UsFlRecord',
                'snapshot': 'UsFlSnapshot'
            }
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
    inmate = with_manifest(regions.get_subkind, 'us_ny', 'Inmate')
    assert inmate.__name__ == 'UsNyInmate'


def test_get_name_list_file():
    filename = with_manifest(regions.get_name_list_file, 'us_fl')
    assert filename == 'us_fl_names.csv'


def test_get_scraper_module():
    module = regions.get_scraper_module('us_ny')
    assert module.__name__ == 'scraper.us_ny'


def test_get_scraper():
    scraper = regions.get_scraper('us_ny')
    assert scraper.__name__ == 'scraper.us_ny.us_ny_scraper'


def test_region_class():
    region = with_manifest(regions.Region, 'us_ny')
    assert region.scraper().__name__ == 'scraper.us_ny.us_ny_scraper'
    assert region.get_inmate_kind().__name__ == 'UsNyInmate'
    assert region.get_record_kind().__name__ == 'UsNyRecord'
    assert region.get_snapshot_kind().__name__ == 'UsNySnapshot'


def with_manifest(func, *args, **kwargs):
    with patch("__builtin__.open",
               mock_open(read_data=MANIFEST_CONTENTS)) \
            as mock_file:
        value = func(*args, **kwargs)
        mock_file.assert_called_with('region_manifest.yaml', 'r')
        return value
