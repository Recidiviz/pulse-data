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

"""Tests for validation/checks/existence_check.py."""

from unittest import TestCase

from mock import patch

from recidiviz.calculator.query.bqview import BigQueryView
from recidiviz.validation.checks.existence_check import ExistenceValidationChecker, ExistenceDataValidationCheck
from recidiviz.validation.validation_models import ValidationCheckType, DataValidationJob, DataValidationJobResult


class TestExistenceValidationChecker(TestCase):
    """Tests for the ExistenceValidationChecker."""

    @patch("recidiviz.validation.validation_queries.run_query")
    def test_existence_check_no_failures(self, mock_query):
        mock_query.return_value = []

        job = DataValidationJob(region_code='US_VA',
                                validation=ExistenceDataValidationCheck(
                                    validation_type=ValidationCheckType.EXISTENCE,
                                    view=BigQueryView('test_view', 'select * from literally_anything')
                                ))
        result = ExistenceValidationChecker.run_check(job)

        self.assertEqual(result,
                         DataValidationJobResult(validation_job=job, was_successful=True, failure_description=None))

    @patch("recidiviz.validation.validation_queries.run_query")
    def test_existence_check_failures(self, mock_query):
        mock_query.return_value = ['some result row', 'some other result row']

        job = DataValidationJob(region_code='US_VA',
                                validation=ExistenceDataValidationCheck(
                                    validation_type=ValidationCheckType.EXISTENCE,
                                    view=BigQueryView('test_view', 'select * from literally_anything')
                                ))
        result = ExistenceValidationChecker.run_check(job)

        self.assertEqual(result,
                         DataValidationJobResult(validation_job=job,
                                                 was_successful=False,
                                                 failure_description='Found 2 invalid rows, though 0 were expected'))
