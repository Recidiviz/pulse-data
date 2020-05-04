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

"""Tests for validation/checks/check_resolver.py."""

from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.validation.checks import check_resolver
from recidiviz.validation.checks.existence_check import ExistenceValidationChecker
from recidiviz.validation.validation_models import DataValidationJob, DataValidationCheck, ValidationCheckType


def test_check_happy_path_existence():
    job = DataValidationJob(region_code='US_VA',
                            validation=DataValidationCheck(
                                validation_type=ValidationCheckType.EXISTENCE,
                                view=BigQueryView('test_view', 'select * from literally_anything')
                            ))
    check_class = check_resolver.checker_for_validation(job)

    assert isinstance(check_class, ExistenceValidationChecker)
