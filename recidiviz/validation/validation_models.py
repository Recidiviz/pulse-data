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

"""Models representing data validation."""

from enum import Enum
from typing import Optional, TypeVar, Generic

import attr

from recidiviz.calculator.query.bqview import BigQueryView
from recidiviz.common.attr_mixins import BuildableAttr
from recidiviz.utils import metadata


VALIDATION_VIEWS_DATASET = 'validation_views'


class ValidationCheckType(Enum):
    EXISTENCE = 'EXISTENCE'


@attr.s(frozen=True)
class DataValidationCheck(BuildableAttr):
    """Models a type of validation check that can be performed."""

    # The BigQuery view for this type of check
    view: BigQueryView = attr.ib()

    # The type of validation to be performed for this type of check
    validation_type: ValidationCheckType = attr.ib()

    def query_str_for_region_code(self, region_code: str) -> str:
        return "SELECT * FROM `{project_id}.{dataset}.{table}` " \
               " WHERE region_code = '{region_code}'" \
            .format(project_id=metadata.project_id(),
                    dataset=VALIDATION_VIEWS_DATASET,
                    table=self.view.view_id,
                    region_code=region_code)


DataValidationType = TypeVar('DataValidationType', bound=DataValidationCheck)


@attr.s(frozen=True)
class DataValidationJob(Generic[DataValidationType], BuildableAttr):
    """Models a specific data validation that is to be performed for a specific region."""

    # The config for the validation to run (what we're going to check for)
    validation: DataValidationCheck = attr.ib()

    # The region we're going to validate (who we're going to check)
    region_code: str = attr.ib()

    def query_str(self) -> str:
        return self.validation.query_str_for_region_code(region_code=self.region_code)


@attr.s(frozen=True)
class DataValidationJobResult(BuildableAttr):
    """Models a data validation result that is to be reviewed."""

    # The validation which was evaluated
    validation_job: DataValidationJob = attr.ib()

    # Whether or not the validation was successful
    was_successful: bool = attr.ib()

    # Description of failure, if there was a failure
    failure_description: Optional[str] = attr.ib()
