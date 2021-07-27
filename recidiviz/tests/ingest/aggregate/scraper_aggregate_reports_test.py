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
"""Tests for tx_aggregate_ingest.py."""
import builtins
import datetime
from typing import Any
from unittest import TestCase

import pytz
import requests
from flask import Flask
from mock import ANY, Mock, call, patch

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.aggregate.regions.ca import ca_aggregate_site_scraper
from recidiviz.ingest.aggregate.regions.ny import ny_aggregate_site_scraper
from recidiviz.ingest.aggregate.regions.tx import tx_aggregate_site_scraper
from recidiviz.ingest.aggregate.scrape_aggregate_reports import (
    HISTORICAL_BUCKET,
    UPLOAD_BUCKET,
    build_path,
    scrape_aggregate_reports_blueprint,
)
from recidiviz.tests.cloud_storage.fake_gcs_file_system import (
    FakeGCSFileSystem,
    FakeGCSFileSystemDelegate,
)
from recidiviz.utils import metadata

APP_ID = "recidiviz-scraper-aggregate-report-test"

app = Flask(__name__)
app.register_blueprint(scrape_aggregate_reports_blueprint)
app.config["TESTING"] = True

SERVER_MODIFIED_TIME = datetime.datetime(year=2019, month=1, day=1, tzinfo=pytz.UTC)
EXISTING_TEST_URL = "http://test.com/url_test/Existing.pdf"
EXISTING_TEST_URL2 = "http://test.com/url_test/Existing2.pdf"
EXISTING_TEST_URL_CA = "http://test.com"
CA_POST_DATA = {"year": 1996, "testing": "1"}
NONEXISTING_TEST_URL = "url_test/nonexisting.pdf"
EXISTING_PDF_NAME = "_url_test_existing.pdf"
EXISTING_PDF_NAME2 = "_url_test_existing2.pdf"
EXISTING_CA_NAME = "california1996"
NONEXISTING_PDF_NAME = "_url_test_nonexisting.pdf"
TEST_CONTENT = "test_content"
TEST_ENV = "recidiviz-test"


def _MockGet(url: str, **_: Any) -> Mock:
    ret = Mock()
    if url in (EXISTING_TEST_URL, EXISTING_TEST_URL2, EXISTING_TEST_URL_CA):
        ret.status_code = 200
        ret.content = TEST_CONTENT
    else:
        ret.status_code = 500
    return ret


class FailOnUploadDelegate(FakeGCSFileSystemDelegate):
    def on_file_added(self, path: GcsfsFilePath) -> None:
        raise ValueError(f"{path.uri()} was added.")


@patch.object(metadata, "project_id", Mock(return_value=TEST_ENV))
@patch.object(metadata, "project_number", Mock(return_value=TEST_ENV))
class TestScraperAggregateReports(TestCase):
    """Test that tx_aggregate_site_scraper correctly scrapes urls."""

    def setUp(self) -> None:
        self.client = app.test_client()

        self.fs = FakeGCSFileSystem()
        self.gcs_factory_patcher = patch(
            "recidiviz.ingest.aggregate.scrape_aggregate_reports.GcsfsFactory.build"
        )
        self.gcs_factory_patcher.start().return_value = self.fs

    @patch.object(requests, "get")
    @patch.object(builtins, "open")
    @patch.object(tx_aggregate_site_scraper, "get_urls_to_download")
    def testExistsNoUpload(
        self, mock_get_all_tx: Mock, mock_open: Mock, mock_get: Mock
    ) -> None:
        historical_path = build_path(HISTORICAL_BUCKET, "texas", EXISTING_PDF_NAME)
        # Make the info call return an older modified time than the server time.
        self.fs.test_add_path(historical_path, local_path=None)
        self.fs.test_set_delegate(FailOnUploadDelegate())
        mock_get_all_tx.return_value = {EXISTING_TEST_URL}
        mock_get.side_effect = _MockGet

        headers = {"X-Appengine-Cron": "test-cron"}
        response = self.client.get("/scrape_state?state=texas", headers=headers)
        self.assertEqual(response.status_code, 200)

        self.assertListEqual(self.fs.all_paths, [historical_path])
        self.assertEqual(mock_open.called, False)

    @patch.object(requests, "get")
    @patch.object(builtins, "open")
    @patch.object(ny_aggregate_site_scraper, "get_urls_to_download")
    def testExistsIsNyUpload(
        self, mock_get_all_tx: Mock, mock_open: Mock, mock_get: Mock
    ) -> None:
        historical_path = build_path(HISTORICAL_BUCKET, "new_york", EXISTING_PDF_NAME)
        upload_path = build_path(UPLOAD_BUCKET, "new_york", EXISTING_PDF_NAME)
        # Make the info call return an older modified time than the server time.
        self.fs.test_add_path(historical_path, local_path=None)
        mock_get_all_tx.return_value = {EXISTING_TEST_URL}
        mock_get.side_effect = _MockGet

        headers = {"X-Appengine-Cron": "test-cron"}
        response = self.client.get("/scrape_state?state=new_york", headers=headers)
        self.assertEqual(response.status_code, 200)

        self.assertListEqual(self.fs.all_paths, [historical_path, upload_path])
        mock_open.assert_called_with(ANY, "wb")
        mock_get.assert_called_with(EXISTING_TEST_URL, verify=True)

    @patch.object(requests, "get")
    @patch.object(builtins, "open")
    @patch.object(tx_aggregate_site_scraper, "get_urls_to_download")
    def testNoExistsUpload200(
        self, mock_get_all_tx: Mock, mock_open: Mock, mock_get: Mock
    ) -> None:
        upload_path = build_path(UPLOAD_BUCKET, "texas", EXISTING_PDF_NAME)
        # Make the info call return an older modified time than the server time.
        mock_get_all_tx.return_value = {EXISTING_TEST_URL}
        mock_get.side_effect = _MockGet

        headers = {"X-Appengine-Cron": "test-cron"}
        response = self.client.get("/scrape_state?state=texas", headers=headers)
        self.assertEqual(response.status_code, 200)

        self.assertListEqual(self.fs.all_paths, [upload_path])
        mock_open.assert_called_with(ANY, "wb")
        mock_get.assert_called_with(EXISTING_TEST_URL, verify=True)

    @patch.object(requests, "post")
    @patch.object(builtins, "open")
    @patch.object(ca_aggregate_site_scraper, "get_urls_to_download")
    def testCaNoExistsUpload200(
        self, mock_get_all_ca: Mock, mock_open: Mock, mock_post: Mock
    ) -> None:
        upload_path = build_path(UPLOAD_BUCKET, "california", EXISTING_CA_NAME)
        # Make the info call return an older modified time than the server time.
        mock_get_all_ca.return_value = [(EXISTING_TEST_URL_CA, CA_POST_DATA)]
        mock_post.side_effect = _MockGet

        headers = {"X-Appengine-Cron": "test-cron"}
        response = self.client.get("/scrape_state?state=california", headers=headers)
        self.assertEqual(response.status_code, 200)

        self.assertListEqual(self.fs.all_paths, [upload_path])
        mock_open.assert_called_with(ANY, "wb")
        mock_post.assert_called_with(
            EXISTING_TEST_URL_CA, data=CA_POST_DATA, verify=True
        )

    @patch.object(requests, "get")
    @patch.object(builtins, "open")
    @patch.object(tx_aggregate_site_scraper, "get_urls_to_download")
    def testMultipleUrlsAll200(
        self, mock_get_all_tx: Mock, mock_open: Mock, mock_get: Mock
    ) -> None:
        upload_path1 = build_path(UPLOAD_BUCKET, "texas", EXISTING_PDF_NAME)
        upload_path2 = build_path(UPLOAD_BUCKET, "texas", EXISTING_PDF_NAME2)
        # Make the info call return an older modified time than the server time.
        mock_get_all_tx.return_value = {EXISTING_TEST_URL, EXISTING_TEST_URL2}
        mock_get.side_effect = _MockGet

        headers = {"X-Appengine-Cron": "test-cron"}
        response = self.client.get("/scrape_state?state=texas", headers=headers)
        self.assertEqual(response.status_code, 200)

        self.assertCountEqual(self.fs.all_paths, [upload_path1, upload_path2])
        self.assertListEqual(mock_open.call_args_list, [call(ANY, "wb")] * 2)
        self.assertCountEqual(
            mock_get.call_args_list,
            [
                call(EXISTING_TEST_URL, verify=True),
                call(EXISTING_TEST_URL2, verify=True),
            ],
        )

    @patch.object(requests, "get")
    @patch.object(builtins, "open")
    @patch.object(tx_aggregate_site_scraper, "get_urls_to_download")
    def testMultipleUrlsOne200OneNoExists(
        self, mock_get_all_tx: Mock, mock_open: Mock, mock_get: Mock
    ) -> None:
        historical_path1 = build_path(HISTORICAL_BUCKET, "texas", EXISTING_PDF_NAME)
        upload_path2 = build_path(UPLOAD_BUCKET, "texas", EXISTING_PDF_NAME2)

        # Make the info call return an older modified time than the server time.
        self.fs.test_add_path(historical_path1, local_path=None)
        mock_get_all_tx.return_value = {EXISTING_TEST_URL, EXISTING_TEST_URL2}
        mock_get.side_effect = _MockGet

        headers = {"X-Appengine-Cron": "test-cron"}
        response = self.client.get("/scrape_state?state=texas", headers=headers)
        self.assertEqual(response.status_code, 200)

        self.assertCountEqual(self.fs.all_paths, [historical_path1, upload_path2])
        self.assertListEqual(mock_open.call_args_list, [call(ANY, "wb")])
        self.assertCountEqual(
            mock_get.call_args_list, [call(EXISTING_TEST_URL2, verify=True)]
        )
