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
"""Configured validations for all of the Dataflow metrics that check for all rows having
a field with an invalid value."""
from typing import List

from recidiviz.validation.checks.existence_check import ExistenceDataValidationCheck
from recidiviz.validation.validation_models import (
    DataValidationCheck,
    ValidationCategory,
)
from recidiviz.validation.views.state.dataflow_metrics.dataflow_metric_fields_all_internal_unknown import (
    VALIDATION_VIEW_BUILDERS as INTERNAL_UNKNOWN_BUILDERS,
)
from recidiviz.validation.views.state.dataflow_metrics.dataflow_metric_fields_all_null import (
    VALIDATION_VIEW_BUILDERS as NULL_BUILDERS,
)


def get_all_dataflow_metrics_validations() -> List[DataValidationCheck]:
    """Returns all validations for checking Dataflow metrics for all rows having invalid
    values."""
    return [
        ExistenceDataValidationCheck(
            view_builder=view_builder, validation_category=ValidationCategory.INVARIANT
        )
        for view_builder in NULL_BUILDERS + INTERNAL_UNKNOWN_BUILDERS
    ]
