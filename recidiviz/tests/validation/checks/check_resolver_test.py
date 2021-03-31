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
import unittest

from mock import patch

from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.validation.checks import check_resolver
from recidiviz.validation.checks.existence_check import (
    ExistenceValidationChecker,
    ExistenceDataValidationCheck,
)
from recidiviz.validation.validation_models import (
    DataValidationJob,
    ValidationCheckType,
)


class ValidationCheckResolverTest(unittest.TestCase):
    """Tests for the DirectIngestIngestViewExportManager class"""

    def setUp(self) -> None:
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = "recidiviz-456"

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    def test_check_happy_path_existence(self) -> None:
        job = DataValidationJob(
            region_code="US_VA",
            validation=ExistenceDataValidationCheck(
                validation_type=ValidationCheckType.EXISTENCE,
                view=BigQueryView(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )
        check_class = check_resolver.checker_for_validation(job)

        assert isinstance(check_class, ExistenceValidationChecker)
