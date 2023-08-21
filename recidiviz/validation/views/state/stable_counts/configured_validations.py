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
"""Configured validations for stable counts."""
from typing import Dict, List

from recidiviz.validation.checks.sameness_check import (
    SamenessDataValidationCheck,
    SamenessDataValidationCheckType,
)
from recidiviz.validation.validation_config import ValidationRegionConfig
from recidiviz.validation.validation_models import (
    DataValidationCheck,
    ValidationCategory,
)
from recidiviz.validation.views.state.stable_counts.entity_by_column_count_stable_counts import (
    VALIDATION_VIEW_BUILDERS_BY_ENTITY_AND_DATE_COL,
)


def get_all_stable_counts_validations(
    region_configs: Dict[str, ValidationRegionConfig]
) -> List[DataValidationCheck]:
    """Returns all validations for entity stable counts by specified date"""
    return [
        SamenessDataValidationCheck(
            view_builder=builder,
            comparison_columns=[
                f"previous_month_{date_col}_count",
                f"{date_col}_count",
            ],
            validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
            region_configs=region_configs,
            hard_max_allowed_error=0.25,
            soft_max_allowed_error=0.25,
            sameness_check_type=SamenessDataValidationCheckType.PER_ROW,
        )
        for (
            entity,
            date_col,
        ), builder in VALIDATION_VIEW_BUILDERS_BY_ENTITY_AND_DATE_COL.items()
    ]
