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
"""Generator to grab all validation jobs and generate the corresponding validation view builder that will output
only the validation errors"""
from typing import List

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.configured_validations import get_all_validations
from recidiviz.validation.validation_models import DataValidationCheck


def generate_validation_view_builders(
    validation_checks: List[DataValidationCheck],
) -> List[SimpleBigQueryViewBuilder]:
    # Creating set to remove possibility of duplicate view builders from validation checks list,
    # since some validation checks reuse the same view builder.
    return list(
        {
            view_builder
            for validation_check in validation_checks
            for view_builder in validation_check.managed_view_builders
        }
    )


def get_generated_validation_view_builders() -> List[SimpleBigQueryViewBuilder]:
    return generate_validation_view_builders(get_all_validations())


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        for builder in get_generated_validation_view_builders():
            builder.build_and_print()
