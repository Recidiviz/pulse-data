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
from typing import List, Dict
from unittest import TestCase

from mock import patch

from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.validation.checks.sameness_check import SamenessValidationChecker, SamenessDataValidationCheck, \
    SamenessDataValidationCheckType
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

        self.good_string_row = {'a': 'same', 'b': 'same', 'c': 'same'}
        self.bad_string_row = {'a': 'a_value', 'b': 'b_value', 'c': 'c_value'}

    def tearDown(self):
        self.client_patcher.stop()
        self.metadata_patcher.stop()

    def return_string_values_with_num_bad_rows(self, num_bad_rows) -> List[Dict[str, str]]:
        return_values = [self.good_string_row] * (100 - num_bad_rows)
        return_values.extend([self.bad_string_row] * num_bad_rows)

        return return_values

    def test_samneness_check_no_comparison_columns(self):
        with self.assertRaises(ValueError) as e:
            _ = SamenessDataValidationCheck(
                validation_type=ValidationCheckType.SAMENESS,
                sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
                view=BigQueryView(dataset_id='my_dataset',
                                  view_id='test_view',
                                  view_query_template='select * from literally_anything')
            )
        self.assertEqual(str(e.exception), 'Found only [0] comparison columns, expected at least 2.')

    def test_samneness_check_bad_max_error(self):
        with self.assertRaises(ValueError) as e:
            _ = SamenessDataValidationCheck(
                validation_type=ValidationCheckType.SAMENESS,
                sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
                comparison_columns=['a', 'b', 'c'],
                view=BigQueryView(dataset_id='my_dataset',
                                  view_id='test_view',
                                  view_query_template='select * from literally_anything'),
                max_allowed_error=1.5
            )
        self.assertEqual(str(e.exception), 'Allowed error value must be between 0.0 and 1.0. Found instead: [1.5]')

    def test_samneness_check_validation_name(self):
        check = SamenessDataValidationCheck(
            validation_type=ValidationCheckType.SAMENESS,
            sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
            comparison_columns=['a', 'b', 'c'],
            view=BigQueryView(dataset_id='my_dataset',
                              view_id='test_view',
                              view_query_template='select * from literally_anything')
        )
        self.assertEqual(check.validation_name, 'test_view')

        check_with_name_suffix = SamenessDataValidationCheck(
            validation_type=ValidationCheckType.SAMENESS,
            sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
            validation_name_suffix='b_c_only',
            comparison_columns=['b', 'c'],
            view=BigQueryView(dataset_id='my_dataset',
                              view_id='test_view',
                              view_query_template='select * from literally_anything')
        )
        self.assertEqual(check_with_name_suffix.validation_name, 'test_view_b_c_only')

    def test_sameness_check_same_values_numbers(self):
        self.mock_client.run_query_async.return_value = [{'a': 10, 'b': 10, 'c': 10}]

        job = DataValidationJob(region_code='US_VA',
                                validation=SamenessDataValidationCheck(
                                    validation_type=ValidationCheckType.SAMENESS,
                                    comparison_columns=['a', 'b', 'c'],
                                    sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
                                    view=BigQueryView(dataset_id='my_dataset',
                                                      view_id='test_view',
                                                      view_query_template='select * from literally_anything')
                                ))
        result = SamenessValidationChecker.run_check(job)

        self.assertEqual(result,
                         DataValidationJobResult(validation_job=job, was_successful=True, failure_description=None))

    def test_sameness_check_different_values_numbers_no_allowed_error(self):
        self.mock_client.run_query_async.return_value = [{'a': 98, 'b': 100, 'c': 99}]

        job = DataValidationJob(region_code='US_VA',
                                validation=SamenessDataValidationCheck(
                                    validation_type=ValidationCheckType.SAMENESS,
                                    comparison_columns=['a', 'b', 'c'],
                                    sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
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

    def test_sameness_check_numbers_different_values_within_margin(self):
        self.mock_client.run_query_async.return_value = [{'a': 98, 'b': 100, 'c': 99}]

        job = DataValidationJob(region_code='US_VA',
                                validation=SamenessDataValidationCheck(
                                    validation_type=ValidationCheckType.SAMENESS,
                                    comparison_columns=['a', 'b', 'c'],
                                    sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
                                    max_allowed_error=0.02,
                                    view=BigQueryView(dataset_id='my_dataset',
                                                      view_id='test_view',
                                                      view_query_template='select * from literally_anything')
                                ))
        result = SamenessValidationChecker.run_check(job)

        self.assertEqual(result,
                         DataValidationJobResult(validation_job=job, was_successful=True, failure_description=None))

    def test_sameness_check_numbers_different_values_above_margin(self):
        self.mock_client.run_query_async.return_value = [{'a': 97, 'b': 100, 'c': 99}]

        job = DataValidationJob(region_code='US_VA',
                                validation=SamenessDataValidationCheck(
                                    validation_type=ValidationCheckType.SAMENESS,
                                    comparison_columns=['a', 'b', 'c'],
                                    sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
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

    def test_sameness_check_numbers_multiple_rows_above_margin(self):
        self.mock_client.run_query_async.return_value = [{'a': 97, 'b': 100, 'c': 99}, {'a': 14, 'b': 21, 'c': 14}]

        job = DataValidationJob(region_code='US_VA',
                                validation=SamenessDataValidationCheck(
                                    validation_type=ValidationCheckType.SAMENESS,
                                    comparison_columns=['a', 'b', 'c'],
                                    sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
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
                                                 'errors as high as 0.3333.',
                         ))

    def test_string_sameness_check_same_values(self):
        self.mock_client.run_query_async.return_value = [{'a': '10', 'b': '10', 'c': '10'}]

        job = DataValidationJob(region_code='US_VA',
                                validation=SamenessDataValidationCheck(
                                    validation_type=ValidationCheckType.SAMENESS,
                                    comparison_columns=['a', 'b', 'c'],
                                    sameness_check_type=SamenessDataValidationCheckType.STRINGS,
                                    view=BigQueryView(dataset_id='my_dataset',
                                                      view_id='test_view',
                                                      view_query_template='select * from literally_anything')
                                ))
        result = SamenessValidationChecker.run_check(job)

        self.assertEqual(result,
                         DataValidationJobResult(validation_job=job, was_successful=True, failure_description=None))

    def test_string_sameness_check_strings_values_all_none(self):
        self.mock_client.run_query_async.return_value = [{'a': None, 'b': None, 'c': None}]

        job = DataValidationJob(region_code='US_VA',
                                validation=SamenessDataValidationCheck(
                                    validation_type=ValidationCheckType.SAMENESS,
                                    comparison_columns=['a', 'b', 'c'],
                                    sameness_check_type=SamenessDataValidationCheckType.STRINGS,
                                    view=BigQueryView(dataset_id='my_dataset',
                                                      view_id='test_view',
                                                      view_query_template='select * from literally_anything')
                                ))
        result = SamenessValidationChecker.run_check(job)

        self.assertEqual(result,
                         DataValidationJobResult(validation_job=job, was_successful=True, failure_description=None))

    def test_string_sameness_check_numbers_values_all_none(self):
        self.mock_client.run_query_async.return_value = [{'a': None, 'b': None, 'c': None}]

        job = DataValidationJob(region_code='US_VA',
                                validation=SamenessDataValidationCheck(
                                    validation_type=ValidationCheckType.SAMENESS,
                                    comparison_columns=['a', 'b', 'c'],
                                    sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
                                    view=BigQueryView(dataset_id='my_dataset',
                                                      view_id='test_view',
                                                      view_query_template='select * from literally_anything')
                                ))

        with self.assertRaises(ValueError) as e:
            _ = SamenessValidationChecker.run_check(job)

        self.assertEqual(str(e.exception), 'Unexpected None value for column [a] in validation [test_view].')


    def test_string_sameness_check_numbers_one_none(self):
        self.mock_client.run_query_async.return_value = [{'a': 3, 'b': 3, 'c': None}]

        job = DataValidationJob(region_code='US_VA',
                                validation=SamenessDataValidationCheck(
                                    validation_type=ValidationCheckType.SAMENESS,
                                    comparison_columns=['a', 'b', 'c'],
                                    sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
                                    view=BigQueryView(dataset_id='my_dataset',
                                                      view_id='test_view',
                                                      view_query_template='select * from literally_anything')
                                ))

        with self.assertRaises(ValueError) as e:
            _ = SamenessValidationChecker.run_check(job)

        self.assertEqual(str(e.exception), 'Unexpected None value for column [c] in validation [test_view].')

    def test_string_sameness_check_different_values_no_allowed_error(self):
        self.mock_client.run_query_async.return_value = [{'a': 'a', 'b': 'b', 'c': 'c'}]

        job = DataValidationJob(region_code='US_VA',
                                validation=SamenessDataValidationCheck(
                                    validation_type=ValidationCheckType.SAMENESS,
                                    comparison_columns=['a', 'b', 'c'],
                                    sameness_check_type=SamenessDataValidationCheckType.STRINGS,
                                    view=BigQueryView(dataset_id='my_dataset',
                                                      view_id='test_view',
                                                      view_query_template='select * from literally_anything')
                                ))
        result = SamenessValidationChecker.run_check(job)

        self.assertEqual(result,
                         DataValidationJobResult(
                             validation_job=job,
                             was_successful=False,
                             failure_description='1 out of 1 row(s) did not contain matching strings. '
                                                 'The acceptable margin of error is only 0.0, but the '
                                                 'validation returned an error rate of 1.0.',
                         ))

    def test_string_sameness_check_different_values_handle_empty_string(self):
        self.mock_client.run_query_async.return_value = [{'a': 'same', 'b': 'same', 'c': None}]

        job = DataValidationJob(region_code='US_VA',
                                validation=SamenessDataValidationCheck(
                                    validation_type=ValidationCheckType.SAMENESS,
                                    comparison_columns=['a', 'b', 'c'],
                                    sameness_check_type=SamenessDataValidationCheckType.STRINGS,
                                    view=BigQueryView(dataset_id='my_dataset',
                                                      view_id='test_view',
                                                      view_query_template='select * from literally_anything')
                                ))
        result = SamenessValidationChecker.run_check(job)

        self.assertEqual(result,
                         DataValidationJobResult(
                             validation_job=job,
                             was_successful=False,
                             failure_description='1 out of 1 row(s) did not contain matching strings. '
                                                 'The acceptable margin of error is only 0.0, but the '
                                                 'validation returned an error rate of 1.0.',
                         ))

    def test_string_sameness_check_different_values_handle_non_string_type(self):
        self.mock_client.run_query_async.return_value = [{'a': 'same', 'b': 'same', 'c': 1245}]

        job = DataValidationJob(region_code='US_VA',
                                validation=SamenessDataValidationCheck(
                                    validation_type=ValidationCheckType.SAMENESS,
                                    comparison_columns=['a', 'b', 'c'],
                                    sameness_check_type=SamenessDataValidationCheckType.STRINGS,
                                    view=BigQueryView(dataset_id='my_dataset',
                                                      view_id='test_view',
                                                      view_query_template='select * from literally_anything')
                                ))
        with self.assertRaises(ValueError) as e:
            _ = SamenessValidationChecker.run_check(job)

        self.assertEqual(str(e.exception),
                         'Unexpected type [<class \'int\'>] for value [1245] in STRING validation [test_view].')

    def test_string_sameness_check_different_values_within_margin(self):
        num_bad_rows = 2
        max_allowed_error = (num_bad_rows / 100)

        self.mock_client.run_query_async.return_value = self.return_string_values_with_num_bad_rows(num_bad_rows)

        job = DataValidationJob(region_code='US_VA',
                                validation=SamenessDataValidationCheck(
                                    validation_type=ValidationCheckType.SAMENESS,
                                    comparison_columns=['a', 'b', 'c'],
                                    sameness_check_type=SamenessDataValidationCheckType.STRINGS,
                                    max_allowed_error=max_allowed_error,
                                    view=BigQueryView(dataset_id='my_dataset',
                                                      view_id='test_view',
                                                      view_query_template='select * from literally_anything')
                                ))
        result = SamenessValidationChecker.run_check(job)

        self.assertEqual(result,
                         DataValidationJobResult(validation_job=job, was_successful=True, failure_description=None))

    def test_string_sameness_check_different_values_above_margin(self):
        num_bad_rows = 5
        max_allowed_error = ((num_bad_rows - 1) / 100)  # Below the number of bad rows

        self.mock_client.run_query_async.return_value = self.return_string_values_with_num_bad_rows(num_bad_rows)
        job = DataValidationJob(region_code='US_VA',
                                validation=SamenessDataValidationCheck(
                                    validation_type=ValidationCheckType.SAMENESS,
                                    comparison_columns=['a', 'b', 'c'],
                                    sameness_check_type=SamenessDataValidationCheckType.STRINGS,
                                    max_allowed_error=max_allowed_error,
                                    view=BigQueryView(dataset_id='my_dataset',
                                                      view_id='test_view',
                                                      view_query_template='select * from literally_anything')
                                ))
        result = SamenessValidationChecker.run_check(job)

        actual_expected_error = (num_bad_rows / 100)

        self.assertEqual(result,
                         DataValidationJobResult(
                             validation_job=job,
                             was_successful=False,
                             failure_description=f'{num_bad_rows} out of 100 row(s) did not contain matching strings. '
                                                 f'The acceptable margin of error is only {max_allowed_error}, but the '
                                                 f'validation returned an error rate of {actual_expected_error}.',
                         ))
