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

"""Tests for validation/checks/sameness_check.py."""

from unittest import TestCase

from mock import patch

from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.validation.checks.sameness_check import SamenessValidationChecker, SamenessDataValidationCheck
from recidiviz.validation.validation_models import ValidationCheckType, DataValidationJob, \
    DataValidationJobResult


class TestSamenessValidationChecker(TestCase):
    """Tests for the SamenessValidationChecker."""

    def setUp(self) -> None:
        self.metadata_patcher = patch('recidiviz.utils.metadata.project_id')
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = 'project-id'

        self.client_patcher = patch(
            'recidiviz.validation.checks.sameness_check.BigQueryClientImpl')
        self.mock_client = self.client_patcher.start().return_value

    def tearDown(self):
        self.client_patcher.stop()
        self.metadata_patcher.stop()

    def test_sameness_check_same_values(self):
        self.mock_client.run_query_async.return_value = [{'a': 10, 'b': 10, 'c': 10}]

        job = DataValidationJob(region_code='US_VA',
                                validation=SamenessDataValidationCheck(
                                    validation_type=ValidationCheckType.SAMENESS,
                                    comparison_columns=['a', 'b', 'c'],
                                    view=BigQueryView(dataset_id='my_dataset',
                                                      view_id='test_view',
                                                      view_query_template='select * from literally_anything')
                                ))
        result = SamenessValidationChecker.run_check(job)

        self.assertEqual(result,
                         DataValidationJobResult(validation_job=job, was_successful=True, failure_description=None))

    def test_sameness_check_different_values_no_allowed_error(self):
        self.mock_client.run_query_async.return_value = [{'a': 98, 'b': 100, 'c': 99}]

        job = DataValidationJob(region_code='US_VA',
                                validation=SamenessDataValidationCheck(
                                    validation_type=ValidationCheckType.SAMENESS,
                                    comparison_columns=['a', 'b', 'c'],
                                    view=BigQueryView(dataset_id='my_dataset',
                                                      view_id='test_view',
                                                      view_query_template='select * from literally_anything')
                                ))
        result = SamenessValidationChecker.run_check(job)

        self.assertEqual(result,
                         DataValidationJobResult(
                             validation_job=job,
                             was_successful=False,
                             failure_description='1 row(s) had unacceptable margins of error. The acceptable margin '
                                                 'of error is only 0.0, but the validation returned rows with '
                                                 'errors as high as 0.02.',
                         ))

    def test_sameness_check_different_values_within_margin(self):
        self.mock_client.run_query_async.return_value = [{'a': 98, 'b': 100, 'c': 99}]

        job = DataValidationJob(region_code='US_VA',
                                validation=SamenessDataValidationCheck(
                                    validation_type=ValidationCheckType.SAMENESS,
                                    comparison_columns=['a', 'b', 'c'],
                                    max_allowed_error=0.02,
                                    view=BigQueryView(dataset_id='my_dataset',
                                                      view_id='test_view',
                                                      view_query_template='select * from literally_anything')
                                ))
        result = SamenessValidationChecker.run_check(job)

        self.assertEqual(result,
                         DataValidationJobResult(validation_job=job, was_successful=True, failure_description=None))

    def test_sameness_check_different_values_above_margin(self):
        self.mock_client.run_query_async.return_value = [{'a': 97, 'b': 100, 'c': 99}]

        job = DataValidationJob(region_code='US_VA',
                                validation=SamenessDataValidationCheck(
                                    validation_type=ValidationCheckType.SAMENESS,
                                    comparison_columns=['a', 'b', 'c'],
                                    max_allowed_error=0.02,
                                    view=BigQueryView(dataset_id='my_dataset',
                                                      view_id='test_view',
                                                      view_query_template='select * from literally_anything')
                                ))
        result = SamenessValidationChecker.run_check(job)

        self.assertEqual(result,
                         DataValidationJobResult(
                             validation_job=job,
                             was_successful=False,
                             failure_description='1 row(s) had unacceptable margins of error. The acceptable margin '
                                                 'of error is only 0.02, but the validation returned rows with '
                                                 'errors as high as 0.03.',
                         ))

    def test_sameness_check_multiple_rows_above_margin(self):
        self.mock_client.run_query_async.return_value = [{'a': 97, 'b': 100, 'c': 99}, {'a': 14, 'b': 21, 'c': 14}]

        job = DataValidationJob(region_code='US_VA',
                                validation=SamenessDataValidationCheck(
                                    validation_type=ValidationCheckType.SAMENESS,
                                    comparison_columns=['a', 'b', 'c'],
                                    max_allowed_error=0.02,
                                    view=BigQueryView(dataset_id='my_dataset',
                                                      view_id='test_view',
                                                      view_query_template='select * from literally_anything')
                                ))
        result = SamenessValidationChecker.run_check(job)

        self.assertEqual(result,
                         DataValidationJobResult(
                             validation_job=job,
                             was_successful=False,
                             failure_description='2 row(s) had unacceptable margins of error. The acceptable margin '
                                                 'of error is only 0.02, but the validation returned rows with '
                                                 'errors as high as 0.33.',
                         ))
