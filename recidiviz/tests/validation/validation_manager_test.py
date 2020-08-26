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

"""Tests for validation/validation_manager.py."""

from typing import List
from unittest import TestCase

from flask import Flask
from mock import patch, call

from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.big_query.view_update_manager import BigQueryViewNamespace
from recidiviz.tests.utils.matchers import UnorderedCollection
from recidiviz.utils.environment import GaeEnvironment
from recidiviz.validation.checks.existence_check import ExistenceDataValidationCheck
from recidiviz.validation.configured_validations import get_all_validations, get_state_codes_to_validate
from recidiviz.validation.validation_manager import validation_manager_blueprint, _fetch_validation_jobs_to_perform
from recidiviz.validation.validation_models import DataValidationJob, DataValidationJobResult
from recidiviz.calculator.query.county import view_config as county_view_config
from recidiviz.calculator.query.state import view_config as state_view_config
from recidiviz.validation.views import view_config as validation_view_config


def get_test_validations() -> List[DataValidationJob]:
    return [
        DataValidationJob(region_code='US_UT',
                          validation=ExistenceDataValidationCheck(
                              view=BigQueryView(dataset_id='my_dataset',
                                                view_id='test_1',
                                                view_query_template='select * from literally_anything')
                          )),
        DataValidationJob(region_code='US_UT',
                          validation=ExistenceDataValidationCheck(
                              view=BigQueryView(dataset_id='my_dataset',
                                                view_id='test_2',
                                                view_query_template='select * from literally_anything')
                          )),
        DataValidationJob(region_code='US_VA',
                          validation=ExistenceDataValidationCheck(
                              view=BigQueryView(dataset_id='my_dataset',
                                                view_id='test_1',
                                                view_query_template='select * from literally_anything')
                          )),
        DataValidationJob(region_code='US_VA',
                          validation=ExistenceDataValidationCheck(
                              view=BigQueryView(dataset_id='my_dataset',
                                                view_id='test_2',
                                                view_query_template='select * from literally_anything')
                          )),
    ]


_API_RESPONSE_IF_NO_FAILURES = "Failed validations:\n"


class TestHandleRequest(TestCase):
    """Tests for the Validation Manager API endpoint."""

    def setUp(self) -> None:
        self.metadata_patcher = patch('recidiviz.utils.metadata.project_id')
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = 'recidiviz-456'

        app = Flask(__name__)
        app.register_blueprint(validation_manager_blueprint)
        app.config['TESTING'] = True
        self.client = app.test_client()

        self._TEST_VALIDATIONS = get_test_validations()

    def tearDown(self):
        self.metadata_patcher.stop()

    @patch("recidiviz.validation.validation_manager._emit_failures")
    @patch("recidiviz.validation.validation_manager._run_job")
    @patch("recidiviz.validation.validation_manager._fetch_validation_jobs_to_perform")
    def test_handle_request_happy_path_no_failures(self, mock_fetch_validations, mock_run_job, mock_emit_failures):
        mock_fetch_validations.return_value = self._TEST_VALIDATIONS
        mock_run_job.return_value = DataValidationJobResult(
            validation_job=self._TEST_VALIDATIONS[0], was_successful=True, failure_description=None)

        headers = {'X-Appengine-Cron': 'test-cron'}
        response = self.client.get('/validate', headers=headers)

        self.assertEqual(200, response.status_code)
        self.assertEqual(_API_RESPONSE_IF_NO_FAILURES, response.get_data().decode())

        self.assertEqual(4, mock_run_job.call_count)
        for job in self._TEST_VALIDATIONS:
            mock_run_job.assert_any_call(job)

        mock_emit_failures.assert_not_called()

    @patch("recidiviz.validation.validation_manager._emit_failures")
    @patch("recidiviz.validation.validation_manager._run_job")
    @patch("recidiviz.validation.validation_manager._fetch_validation_jobs_to_perform")
    def test_handle_request_with_job_failures_and_validation_failures(
            self, mock_fetch_validations, mock_run_job, mock_emit_failures):
        mock_fetch_validations.return_value = self._TEST_VALIDATIONS
        first_failure = DataValidationJobResult(
            validation_job=self._TEST_VALIDATIONS[1], was_successful=False, failure_description='Oh no')
        second_failure = DataValidationJobResult(
            validation_job=self._TEST_VALIDATIONS[2], was_successful=False, failure_description='How awful')
        mock_run_job.side_effect = [
            DataValidationJobResult(
                validation_job=self._TEST_VALIDATIONS[0], was_successful=True, failure_description=None),
            first_failure,
            second_failure,
            ValueError('Job failed to run!')
        ]
        headers = {'X-Appengine-Cron': 'test-cron'}
        response = self.client.get('/validate', headers=headers)

        self.assertEqual(200, response.status_code)
        self.assertNotEqual(_API_RESPONSE_IF_NO_FAILURES, response.get_data().decode())

        self.assertEqual(4, mock_run_job.call_count)
        for job in self._TEST_VALIDATIONS:
            mock_run_job.assert_any_call(job)

        mock_emit_failures.assert_called_with(UnorderedCollection([self._TEST_VALIDATIONS[3]]),
                                              UnorderedCollection([first_failure, second_failure]))

    @patch("recidiviz.validation.validation_manager._emit_failures")
    @patch("recidiviz.validation.validation_manager._run_job")
    @patch("recidiviz.validation.validation_manager._fetch_validation_jobs_to_perform")
    def test_handle_request_happy_path_some_failures(self, mock_fetch_validations, mock_run_job, mock_emit_failures):
        mock_fetch_validations.return_value = self._TEST_VALIDATIONS

        first_failure = DataValidationJobResult(
            validation_job=self._TEST_VALIDATIONS[1], was_successful=False, failure_description='Oh no')
        second_failure = DataValidationJobResult(
            validation_job=self._TEST_VALIDATIONS[2], was_successful=False, failure_description='How awful')
        mock_run_job.side_effect = [
            DataValidationJobResult(
                validation_job=self._TEST_VALIDATIONS[0], was_successful=True, failure_description=None),
            first_failure,
            second_failure,
            DataValidationJobResult(
                validation_job=self._TEST_VALIDATIONS[3], was_successful=True, failure_description=None),
        ]

        headers = {'X-Appengine-Cron': 'test-cron'}
        response = self.client.get('/validate', headers=headers)

        self.assertEqual(200, response.status_code)
        self.assertNotEqual(_API_RESPONSE_IF_NO_FAILURES, response.get_data().decode())

        self.assertEqual(4, mock_run_job.call_count)
        for job in self._TEST_VALIDATIONS:
            mock_run_job.assert_any_call(job)

        mock_emit_failures.assert_called_with([], UnorderedCollection([first_failure, second_failure]))

    @patch("recidiviz.validation.validation_manager._emit_failures")
    @patch("recidiviz.validation.validation_manager._run_job")
    @patch("recidiviz.validation.validation_manager._fetch_validation_jobs_to_perform")
    def test_handle_request_happy_path_nothing_configured(self,
                                                          mock_fetch_validations,
                                                          mock_run_job,
                                                          mock_emit_failures):
        mock_fetch_validations.return_value = []

        headers = {'X-Appengine-Cron': 'test-cron'}
        response = self.client.get('/validate', headers=headers)

        self.assertEqual(200, response.status_code)
        self.assertEqual(_API_RESPONSE_IF_NO_FAILURES, response.get_data().decode())

        mock_run_job.assert_not_called()
        mock_emit_failures.assert_not_called()

    @patch("recidiviz.big_query.view_update_manager.create_dataset_and_update_views_for_view_builders")
    @patch("recidiviz.validation.validation_manager._emit_failures")
    @patch("recidiviz.validation.validation_manager._run_job")
    @patch("recidiviz.validation.validation_manager._fetch_validation_jobs_to_perform")
    def test_handle_request_happy_path_should_update_views(self,
                                                           mock_fetch_validations,
                                                           mock_run_job,
                                                           mock_emit_failures,
                                                           mock_update_views):
        mock_fetch_validations.return_value = []

        headers = {'X-Appengine-Cron': 'test-cron'}
        response = self.client.get('/validate?should_update_views=true', headers=headers)

        self.assertEqual(200, response.status_code)
        self.assertEqual(_API_RESPONSE_IF_NO_FAILURES, response.get_data().decode())

        mock_run_job.assert_not_called()
        mock_emit_failures.assert_not_called()

        self.maxDiff = None
        expected_update_calls = [
            call(BigQueryViewNamespace.COUNTY, county_view_config.VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE, False),
            call(BigQueryViewNamespace.STATE, state_view_config.VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE, False),
            call(BigQueryViewNamespace.VALIDATION, validation_view_config.VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE, False)
        ]
        self.assertCountEqual(mock_update_views.call_args_list, expected_update_calls)


class TestFetchValidations(TestCase):
    """Tests the _fetch_validation_jobs_to_perform function."""

    def setUp(self) -> None:
        self.metadata_patcher = patch('recidiviz.utils.metadata.project_id')
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = 'recidiviz-456'

    def tearDown(self):
        self.metadata_patcher.stop()

    @patch("recidiviz.utils.environment.get_gae_environment", return_value=GaeEnvironment.STAGING.value)
    def test_cross_product_states_and_checks_staging(self, _mock_get_environment):
        all_validations = get_all_validations()
        all_states = get_state_codes_to_validate()
        expected_length = len(all_validations) * len(all_states)

        result = _fetch_validation_jobs_to_perform()
        self.assertEqual(expected_length, len(result))

    @patch("recidiviz.utils.environment.get_gae_environment", return_value=GaeEnvironment.PRODUCTION.value)
    def test_cross_product_states_and_checks_production(self, _mock_get_environment):
        all_validations = get_all_validations()
        state_codes_to_validate = get_state_codes_to_validate()

        # When you promote a state to production, we will start running validations against that state - add it to this
        # list to confirm you've updated all relevant external data validation tables in production to include
        # validation data for the newly promoted region.
        self.assertEqual(state_codes_to_validate, [
            'US_ID',
            'US_MO',
            'US_ND'
        ])

        expected_length = len(all_validations) * len(state_codes_to_validate)

        result = _fetch_validation_jobs_to_perform()
        self.assertEqual(expected_length, len(result))
