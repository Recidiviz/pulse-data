# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
""""Configured validations related to raw data tables."""
from typing import Dict, List

from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION
from recidiviz.validation.checks.existence_check import ExistenceDataValidationCheck
from recidiviz.validation.checks.sameness_check import SamenessDataValidationCheck
from recidiviz.validation.validation_config import ValidationRegionConfig
from recidiviz.validation.validation_models import (
    DataValidationCheck,
    ValidationCategory,
)
from recidiviz.validation.views.state.raw_data.stable_historical_counts import (
    collect_stable_historical_counts_view_builders,
)
from recidiviz.validation.views.state.raw_data.stale_critical_table_validation import (
    collect_stale_critical_raw_data_view_builders,
)


def get_all_raw_data_validations(
    region_configs: Dict[str, ValidationRegionConfig]
) -> List[DataValidationCheck]:
    return [
        *[
            ExistenceDataValidationCheck(
                view_builder=builder,
                validation_category=ValidationCategory.FRESHNESS,
                projects_to_deploy={GCP_PROJECT_PRODUCTION},
            )
            for builder in collect_stale_critical_raw_data_view_builders()
        ],
        *[
            SamenessDataValidationCheck(
                view_builder=builder,
                comparison_columns=["row_count", "prev_row_count"],
                region_configs=region_configs,
                validation_category=ValidationCategory.CONSISTENCY,
                projects_to_deploy={GCP_PROJECT_PRODUCTION},
            )
            for builder in collect_stable_historical_counts_view_builders()
        ],
    ]
