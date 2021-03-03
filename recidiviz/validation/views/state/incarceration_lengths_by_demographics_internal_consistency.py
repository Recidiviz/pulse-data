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
"""A view that can be used in validation to ensure internal consistency across breakdowns in the
incarceration_lengths_by_demographics view.
"""

# pylint: disable=trailing-whitespace

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config


from recidiviz.calculator.query.state.views.public_dashboard.incarceration.incarceration_lengths_by_demographics import (
    INCARCERATION_LENGTHS_BY_DEMOGRAPHICS_VIEW_NAME,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config
from recidiviz.validation.views.utils.internal_consistency_templates import (
    internal_consistency_query,
)

INCARCERATION_LENGTHS_BY_DEMOGRAPHICS_INTERNAL_CONSISTENCY_VIEW_NAME = (
    "incarceration_lengths_by_demographics_internal_consistency"
)

INCARCERATION_LENGTHS_BY_DEMOGRAPHICS_INTERNAL_CONSISTENCY_DESCRIPTION = """ Builds validation table to ensure
internal consistency across breakdowns in the incarceration_lengths_by_demographics view."""


PARTITION_COLUMNS = ["state_code", "metric_period_months"]
CALCULATED_COLUMNS_TO_VALIDATE = [
    "years_0_1",
    "years_1_2",
    "years_2_3",
    "years_3_5",
    "years_5_10",
    "years_10_20",
    "years_20_plus",
    "total_release_count",
]
MUTUALLY_EXCLUSIVE_BREAKDOWN_COLUMNS = ["age_bucket", "race_or_ethnicity", "gender"]


INCARCERATION_LENGTHS_BY_DEMOGRAPHICS_INTERNAL_CONSISTENCY_QUERY_TEMPLATE = f"""
/*{{description}}*/
{internal_consistency_query(partition_columns=PARTITION_COLUMNS, mutually_exclusive_breakdown_columns=MUTUALLY_EXCLUSIVE_BREAKDOWN_COLUMNS, calculated_columns_to_validate=CALCULATED_COLUMNS_TO_VALIDATE)}
"""

INCARCERATION_LENGTHS_BY_DEMOGRAPHICS_INTERNAL_CONSISTENCY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=INCARCERATION_LENGTHS_BY_DEMOGRAPHICS_INTERNAL_CONSISTENCY_VIEW_NAME,
    view_query_template=INCARCERATION_LENGTHS_BY_DEMOGRAPHICS_INTERNAL_CONSISTENCY_QUERY_TEMPLATE,
    description=INCARCERATION_LENGTHS_BY_DEMOGRAPHICS_INTERNAL_CONSISTENCY_DESCRIPTION,
    validated_table_dataset_id=state_dataset_config.PUBLIC_DASHBOARD_VIEWS_DATASET,
    validated_table_id=INCARCERATION_LENGTHS_BY_DEMOGRAPHICS_VIEW_NAME,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_LENGTHS_BY_DEMOGRAPHICS_INTERNAL_CONSISTENCY_VIEW_BUILDER.build_and_print()
