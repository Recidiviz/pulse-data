# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

"""Tests for email_reporting_utils.py."""

#pylint: disable=import-outside-toplevel

from unittest import TestCase
from unittest.mock import patch, Mock
import recidiviz.reporting.email_reporting_utils as utils

_MOCK_PROJECT_ID = 'RECIDIVIZ_TEST'


class EmailReportingUtilsTests(TestCase):
    """Tests for email_reporting_utils.py."""

    @classmethod
    def setUpClass(cls):
        cls.eru = utils

    @patch('os.environ.get')
    def test_get_env_var_happy_path(self, mock_environ_get):
        expected = 'ANALYSIS'
        mock_environ_get.return_value = expected

        actual = self.eru.get_env_var('WILDLIFE')
        self.assertEqual(expected, actual)

    @patch('os.environ.get')
    def test_get_env_var_not_found(self, mock_environ_get):
        mock_environ_get.return_value = None

        with self.assertRaises(KeyError):
            self.eru.get_env_var('SIXTYTEN')

    @patch('recidiviz.utils.metadata.project_id', Mock(return_value=_MOCK_PROJECT_ID))
    def test_get_project_id(self):
        actual = self.eru.get_project_id()
        self.assertEqual(_MOCK_PROJECT_ID, actual)

    @patch('recidiviz.utils.secrets.get_secret')
    def test_get_cdn_static_ip(self, mock_secret):
        expected = '123.456.7.8'
        test_secrets = {
            'po_report_cdn_static_IP': expected
        }
        mock_secret.side_effect = test_secrets.get

        actual = self.eru.get_cdn_static_ip()
        self.assertEqual(expected, actual)

    @patch('recidiviz.utils.metadata.project_id', Mock(return_value=_MOCK_PROJECT_ID))
    def test_get_data_storage_bucket_name(self):
        expected = f'{_MOCK_PROJECT_ID}-report-data'
        actual = self.eru.get_data_storage_bucket_name()
        self.assertEqual(expected, actual)

    @patch('recidiviz.utils.metadata.project_id', Mock(return_value=_MOCK_PROJECT_ID))
    def test_get_data_archive_bucket_name(self):
        expected = f'{_MOCK_PROJECT_ID}-report-data-archive'
        actual = self.eru.get_data_archive_bucket_name()
        self.assertEqual(expected, actual)

    @patch('recidiviz.utils.metadata.project_id', Mock(return_value=_MOCK_PROJECT_ID))
    def test_get_html_bucket_name(self):
        expected = f'{_MOCK_PROJECT_ID}-report-html'
        actual = self.eru.get_html_bucket_name()
        self.assertEqual(expected, actual)

    @patch('recidiviz.utils.secrets.get_secret')
    def test_get_static_image_path(self, mock_secret):
        expected = '123.456.7.8'
        test_secrets = {
            'po_report_cdn_static_IP': expected
        }
        mock_secret.side_effect = test_secrets.get

        expected = 'http://123.456.7.8/us_va/anything/static'
        actual = self.eru.get_static_image_path('us_va', 'anything')
        self.assertEqual(expected, actual)

    def test_get_data_filename(self):
        expected = 'anything/us_va/anything_data.json'
        actual = self.eru.get_data_filename('us_va', 'anything')
        self.assertEqual(expected, actual)

    def test_get_data_archive_filename(self):
        expected = 'batch-1.json'
        actual = self.eru.get_data_archive_filename('batch-1')
        self.assertEqual(expected, actual)

    def test_get_properties_filename(self):
        expected = 'anything/us_va/properties.json'
        actual = self.eru.get_properties_filename('us_va', 'anything')
        self.assertEqual(expected, actual)

    def test_get_html_filename(self):
        expected = 'batch-1/boards@canada.ca.html'
        actual = self.eru.get_html_filename('batch-1', 'boards@canada.ca')
        self.assertEqual(expected, actual)

    def test_get_template_filename(self):
        expected = 'anything/us_va/template.html'
        actual = self.eru.get_template_filename('us_va', 'anything')
        self.assertEqual(expected, actual)
