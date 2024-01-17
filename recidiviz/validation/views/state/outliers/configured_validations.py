# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Contains configured data validations to perform on outliers related entities and views."""
from typing import List

from recidiviz.validation.checks.existence_check import ExistenceDataValidationCheck
from recidiviz.validation.validation_models import (
    DataValidationCheck,
    ValidationCategory,
)
from recidiviz.validation.views.state.outliers.current_supervision_staff_missing_district import (
    CURRENT_SUPERVISION_STAFF_MISSING_DISTRICT_VIEW_BUILDER,
)
from recidiviz.validation.views.state.outliers.current_supervision_staff_missing_email import (
    CURRENT_SUPERVISION_STAFF_MISSING_EMAIL_VIEW_BUILDER,
)
from recidiviz.validation.views.state.outliers.unidentified_supervision_districts_for_staff import (
    UNIDENTIFIED_SUPERVISION_DISTRICTS_FOR_STAFF_VIEW_BUILDER,
)
from recidiviz.validation.views.state.outliers.unidentified_supervision_officer_supervisors import (
    UNIDENTIFIED_SUPERVISION_OFFICER_SUPERVISORS_VIEW_BUILDER,
)


def get_all_outliers_validations() -> List[DataValidationCheck]:
    """Returns the full list of configured validations to perform on the
    outliers related entities and views.
    """
    return [
        ExistenceDataValidationCheck(
            view_builder=CURRENT_SUPERVISION_STAFF_MISSING_DISTRICT_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=CURRENT_SUPERVISION_STAFF_MISSING_EMAIL_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=UNIDENTIFIED_SUPERVISION_OFFICER_SUPERVISORS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=UNIDENTIFIED_SUPERVISION_DISTRICTS_FOR_STAFF_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
    ]
