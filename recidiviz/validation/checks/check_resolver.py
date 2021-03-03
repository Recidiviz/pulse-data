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

"""Utilities for resolving the appropriate validation checker to use."""
from typing import Dict

from recidiviz.validation.checks.existence_check import ExistenceValidationChecker
from recidiviz.validation.checks.sameness_check import SamenessValidationChecker
from recidiviz.validation.checks.validation_checker import ValidationChecker
from recidiviz.validation.validation_models import (
    ValidationCheckType,
    DataValidationJob,
)


_CHECKER_FOR_TYPE: Dict[ValidationCheckType, ValidationChecker] = {
    ValidationCheckType.EXISTENCE: ExistenceValidationChecker(),
    ValidationCheckType.SAMENESS: SamenessValidationChecker(),
}


def _checker_for_type(check_type: ValidationCheckType) -> ValidationChecker:
    checker = _CHECKER_FOR_TYPE.get(check_type, None)
    if checker:
        return checker

    raise ValueError(f"No checker implementation enabled for check type {check_type}")


def checker_for_validation(validation_job: DataValidationJob) -> ValidationChecker:
    """Retrieves the checker type associated with the given validation table.

    Raises a ValueError if the check type on the validation job is not associated with any
    validation checker implementation.
    """
    check_type = validation_job.validation.validation_type
    return _checker_for_type(check_type)
