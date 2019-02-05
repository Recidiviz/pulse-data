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
"""Tests for tx_aggregate_ingest.py."""
from unittest import TestCase
import builtins
import datetime
import os
import tempfile
from flask import Flask
from mock import patch, Mock, call
import requests
import gcsfs
import pytz

from recidiviz.ingest.aggregate import scrape_aggregate_reports
from recidiviz.ingest.aggregate.regions.ny import ny_aggregate_site_scraper
from recidiviz.ingest.aggregate.regions.tx import tx_aggregate_site_scraper
from recidiviz.tests.ingest import fixtures
from recidiviz.utils import metadata

REPORTS_HTML = fixtures.as_string('aggregate/regions/tx', 'reports.html')

APP_ID = "recidiviz-scraper-aggregate-report-test"

app = Flask(__name__)
app.register_blueprint(
    scrape_aggregate_reports.scrape_aggregate_reports_blueprint)
app.config['TESTING'] = True

SERVER_MODIFIED_TIME = datetime.datetime(
    year=2019, month=1, day=1, tzinfo=pytz.UTC)
EXISTING_TEST_URL = 'http://test.com/url_test/Existing.pdf'
EXISTING_TEST_URL2 = 'http://test.com/url_test/Existing2.pdf'
NONEXISTING_TEST_URL = 'url_test/nonexisting.pdf'
EXISTING_PDF_NAME = '_url_test_existing.pdf'
EXISTING_PDF_NAME2 = '_url_test_existing2.pdf'
NONEXISTING_PDF_NAME = '_url_test_nonexisting.pdf'
TEST_CONTENT = 'test_content'
TEST_ENV = 'recidiviz-test'


def _MockGet(url, **_):
    ret = Mock()
    if url in (EXISTING_TEST_URL, EXISTING_TEST_URL2):
        ret.status_code = 200
        ret.content = TEST_CONTENT
    else:
        ret.status_code = 500
    return ret


class TestScraperAggregateReports(TestCase):
    """Test that tx_aggregate_site_scraper correctly scrapes urls."""

    # noinspection PyAttributeOutsideInit
    def setup_method(self, _test_method):
        self.client = app.test_client()
        self.historical_bucket = \
            scrape_aggregate_reports.HISTORICAL_BUCKET.format(TEST_ENV)
        self.upload_bucket = \
            scrape_aggregate_reports.UPLOAD_BUCKET.format(TEST_ENV)

    @patch.object(metadata, 'project_id')
    @patch.object(gcsfs, 'GCSFileSystem')
    @patch.object(requests, 'get')
    @patch.object(builtins, 'open')
    @patch.object(tx_aggregate_site_scraper, 'get_urls_to_download')
    def testExistsNoUpload(
            self, mock_get_all_tx, mock_open, mock_get, mock_fs, mock_env):
        mock_env.return_value = TEST_ENV
        # Make the info call return an older modified time than the server time.
        mock_fs_return = Mock()
        mock_fs.return_value = mock_fs_return
        mock_fs_return.exists.return_value = True
        mock_get_all_tx.return_value = {EXISTING_TEST_URL}
        mock_get.side_effect = _MockGet

        headers = {'X-Appengine-Cron': "test-cron"}
        response = self.client.get(
            '/scrape_state?state=texas', headers=headers)
        self.assertEqual(response.status_code, 200)

        mock_fs.assert_called_with(project=TEST_ENV)
        mock_fs_return.exists.assert_called_with(
            os.path.join(self.historical_bucket, 'texas', EXISTING_PDF_NAME))
        self.assertEqual(mock_fs_return.put.called, False)
        self.assertEqual(mock_open.called, False)

    @patch.object(metadata, 'project_id')
    @patch.object(gcsfs, 'GCSFileSystem')
    @patch.object(requests, 'get')
    @patch.object(builtins, 'open')
    @patch.object(ny_aggregate_site_scraper, 'get_urls_to_download')
    def testExistsIsNyUpload(
            self, mock_get_all_tx, mock_open, mock_get, mock_fs, mock_env):
        upload_bucket = os.path.join(
            self.upload_bucket, 'new_york', EXISTING_PDF_NAME)
        temploc = os.path.join(tempfile.gettempdir(), EXISTING_PDF_NAME)
        mock_env.return_value = TEST_ENV
        # Make the info call return an older modified time than the server time.
        mock_fs_return = Mock()
        mock_fs.return_value = mock_fs_return
        mock_fs_return.exists.return_value = True
        mock_get_all_tx.return_value = {EXISTING_TEST_URL}
        mock_get.side_effect = _MockGet

        headers = {'X-Appengine-Cron': "test-cron"}
        response = self.client.get(
            '/scrape_state?state=new_york', headers=headers)
        self.assertEqual(response.status_code, 200)

        mock_fs.assert_called_with(project=TEST_ENV)
        self.assertFalse(mock_fs_return.exists.called)
        mock_fs_return.put.assert_called_with(temploc, upload_bucket)
        mock_open.assert_called_with(temploc, 'wb')
        mock_get.assert_called_with(EXISTING_TEST_URL)

    @patch.object(metadata, 'project_id')
    @patch.object(gcsfs, 'GCSFileSystem')
    @patch.object(requests, 'get')
    @patch.object(builtins, 'open')
    @patch.object(tx_aggregate_site_scraper, 'get_urls_to_download')
    def testNoExistsUpload200(
            self, mock_get_all_tx, mock_open, mock_get, mock_fs, mock_env):
        upload_bucket = os.path.join(
            self.upload_bucket, 'texas', EXISTING_PDF_NAME)
        temploc = os.path.join(tempfile.gettempdir(), EXISTING_PDF_NAME)
        mock_env.return_value = TEST_ENV
        # Make the info call return an older modified time than the server time.
        mock_fs_return = Mock()
        mock_fs.return_value = mock_fs_return
        mock_fs_return.exists.return_value = False
        mock_get_all_tx.return_value = {EXISTING_TEST_URL}
        mock_get.side_effect = _MockGet

        headers = {'X-Appengine-Cron': "test-cron"}
        response = self.client.get(
            '/scrape_state?state=texas', headers=headers)
        self.assertEqual(response.status_code, 200)

        mock_fs.assert_called_with(project=TEST_ENV)
        mock_fs_return.exists.assert_called_with(
            os.path.join(self.historical_bucket, 'texas', EXISTING_PDF_NAME))
        mock_fs_return.put.assert_called_with(temploc, upload_bucket)
        mock_open.assert_called_with(temploc, 'wb')
        mock_get.assert_called_with(EXISTING_TEST_URL)

    @patch.object(metadata, 'project_id')
    @patch.object(gcsfs, 'GCSFileSystem')
    @patch.object(requests, 'get')
    @patch.object(builtins, 'open')
    @patch.object(tx_aggregate_site_scraper, 'get_urls_to_download')
    def testMultipleUrlsAll200(
            self, mock_get_all_tx, mock_open, mock_get, mock_fs, mock_env):
        upload_bucket1 = os.path.join(
            self.upload_bucket, 'texas', EXISTING_PDF_NAME)
        upload_bucket2 = os.path.join(
            self.upload_bucket, 'texas', EXISTING_PDF_NAME2)
        temploc1 = os.path.join(tempfile.gettempdir(), EXISTING_PDF_NAME)
        temploc2 = os.path.join(tempfile.gettempdir(), EXISTING_PDF_NAME2)
        mock_env.return_value = TEST_ENV
        # Make the info call return an older modified time than the server time.
        mock_fs_return = Mock()
        mock_fs.return_value = mock_fs_return
        mock_fs_return.exists.return_value = False
        mock_get_all_tx.return_value = {EXISTING_TEST_URL, EXISTING_TEST_URL2}
        mock_get.side_effect = _MockGet

        headers = {'X-Appengine-Cron': "test-cron"}
        response = self.client.get(
            '/scrape_state?state=texas', headers=headers)
        self.assertEqual(response.status_code, 200)

        mock_fs.assert_called_with(project=TEST_ENV)
        self.assertEqual(mock_fs.call_count, 1)
        expected_exists_calls = [
            call(os.path.join(
                self.historical_bucket, 'texas', EXISTING_PDF_NAME)),
            call(os.path.join(
                self.historical_bucket, 'texas', EXISTING_PDF_NAME2))
        ]
        self.assertCountEqual(
            mock_fs_return.exists.call_args_list, expected_exists_calls)
        expected_put_calls = [call(temploc1, upload_bucket1),
                              call(temploc2, upload_bucket2)]
        self.assertCountEqual(
            mock_fs_return.put.call_args_list, expected_put_calls)
        expected_open_calls = [call(temploc1, 'wb'),
                               call(temploc2, 'wb')]
        self.assertCountEqual(mock_open.call_args_list, expected_open_calls)

    @patch.object(metadata, 'project_id')
    @patch.object(gcsfs, 'GCSFileSystem')
    @patch.object(requests, 'get')
    @patch.object(builtins, 'open')
    @patch.object(tx_aggregate_site_scraper, 'get_urls_to_download')
    def testMultipleUrlsOne200OneNoExists(
            self, mock_get_all_tx, mock_open, mock_get, mock_fs, mock_env):
        historical_path1 = os.path.join(
            self.historical_bucket, 'texas', EXISTING_PDF_NAME)
        historical_path2 = os.path.join(
            self.historical_bucket, 'texas', EXISTING_PDF_NAME2)

        def _exists_return(path):
            ret_bool = False
            if path == historical_path1:
                ret_bool = True
            elif path == historical_path2:
                ret_bool = False
            return ret_bool
        upload_bucket2 = os.path.join(
            self.upload_bucket, 'texas', EXISTING_PDF_NAME2)
        temploc2 = os.path.join(tempfile.gettempdir(), EXISTING_PDF_NAME2)
        mock_env.return_value = TEST_ENV
        # Make the info call return an older modified time than the server time.
        mock_fs_return = Mock()
        mock_fs.return_value = mock_fs_return
        mock_fs_return.exists.side_effect = _exists_return
        mock_get_all_tx.return_value = {EXISTING_TEST_URL, EXISTING_TEST_URL2}
        mock_get.side_effect = _MockGet

        headers = {'X-Appengine-Cron': "test-cron"}
        response = self.client.get(
            '/scrape_state?state=texas', headers=headers)
        self.assertEqual(response.status_code, 200)

        mock_fs.assert_called_with(project=TEST_ENV)
        self.assertEqual(mock_fs.call_count, 1)
        expected_exists_calls = [
            call(historical_path1),
            call(historical_path2)
        ]
        self.assertCountEqual(
            mock_fs_return.exists.call_args_list, expected_exists_calls)
        self.assertEqual(mock_fs_return.put.call_count, 1)
        mock_fs_return.put.assert_called_with(temploc2, upload_bucket2)
        mock_open.assert_called_with(temploc2, 'wb')
        self.assertEqual(mock_open.call_count, 1)
