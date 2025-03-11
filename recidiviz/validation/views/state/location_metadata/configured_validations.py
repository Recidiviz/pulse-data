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
"""Contains configured data validations to perform on the location_metadata view."""
from typing import List

from recidiviz.validation.checks.existence_check import ExistenceDataValidationCheck
from recidiviz.validation.validation_models import (
    DataValidationCheck,
    ValidationCategory,
)
from recidiviz.validation.views.state.location_metadata.location_metadata_human_readable_location_name import (
    LOCATION_METADATA_HUMAN_READABLE_LOCATION_NAME_VIEW_BUILDER,
)
from recidiviz.validation.views.state.location_metadata.location_metadata_human_readable_metadata_name import (
    LOCATION_METADATA_HUMAN_READABLE_METADATA_NAME_VIEW_BUILDER,
)
from recidiviz.validation.views.state.location_metadata.location_metadata_invalid_location_type import (
    LOCATION_METADATA_INVALID_LOCATION_TYPE_VIEW_BUILDER,
)
from recidiviz.validation.views.state.location_metadata.location_metadata_invalid_metadata_key import (
    LOCATION_METADATA_INVALID_METADATA_KEY_VIEW_BUILDER,
)
from recidiviz.validation.views.state.location_metadata.location_metadata_missing_person_staff_relationship_period_locations import (
    LOCATION_METADATA_MISSING_PERSON_STAFF_RELATIONSHIP_PERIOD_LOCATIONS_VIEW_BUILDER,
)
from recidiviz.validation.views.state.location_metadata.location_metadata_missing_staff_location_period_locations import (
    LOCATION_METADATA_MISSING_STAFF_LOCATION_PERIOD_LOCATIONS_VIEW_BUILDER,
)


def get_all_location_metadata_validations() -> List[DataValidationCheck]:
    """Returns the full list of configured validations to perform on the
    location_metadata view.
    """
    return [
        ExistenceDataValidationCheck(
            view_builder=LOCATION_METADATA_HUMAN_READABLE_LOCATION_NAME_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=LOCATION_METADATA_HUMAN_READABLE_METADATA_NAME_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=LOCATION_METADATA_INVALID_LOCATION_TYPE_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=LOCATION_METADATA_INVALID_METADATA_KEY_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=LOCATION_METADATA_MISSING_STAFF_LOCATION_PERIOD_LOCATIONS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=LOCATION_METADATA_MISSING_PERSON_STAFF_RELATIONSHIP_PERIOD_LOCATIONS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
    ]
