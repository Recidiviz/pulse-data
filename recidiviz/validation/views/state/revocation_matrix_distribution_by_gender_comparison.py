# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""A view which provides a comparison of counts summed across all genders with the expected total counts for those
sums for the revocations_matrix_distribution_by_gender view in the Revocation Analysis Matrix tool."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.views.dashboard.revocation_analysis.revocations_matrix_distribution_by_gender import (
    REVOCATIONS_MATRIX_DISTRIBUTION_BY_GENDER_VIEW_BUILDER,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config
from recidiviz.validation.views.utils.internal_consistency_templates import (
    sums_and_totals_consistency_query,
)

REVOCATION_MATRIX_DISTRIBUTION_BY_GENDER_COMPARISON_VIEW_NAME = (
    "revocation_matrix_distribution_by_gender_comparison"
)

REVOCATION_MATRIX_DISTRIBUTION_BY_GENDER_COMPARISON_DESCRIPTION = """
Revocation matrix comparison of summed counts across gender """


REVOCATION_MATRIX_DISTRIBUTION_BY_GENDER_COMPARISON_QUERY_TEMPLATE = f"""
{sums_and_totals_consistency_query(
    view_builder=REVOCATIONS_MATRIX_DISTRIBUTION_BY_GENDER_VIEW_BUILDER,
    breakdown_dimensions=['gender'],
    columns_with_totals=['revocation_count_all', 'supervision_count_all','recommended_for_revocation_count_all'],
    columns_with_breakdown_counts=
    ['revocation_count', 'supervision_population_count','recommended_for_revocation_count']
)}
"""


REVOCATION_MATRIX_DISTRIBUTION_BY_GENDER_COMPARISON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=REVOCATION_MATRIX_DISTRIBUTION_BY_GENDER_COMPARISON_VIEW_NAME,
    view_query_template=REVOCATION_MATRIX_DISTRIBUTION_BY_GENDER_COMPARISON_QUERY_TEMPLATE,
    description=REVOCATION_MATRIX_DISTRIBUTION_BY_GENDER_COMPARISON_DESCRIPTION,
    view_dataset=REVOCATIONS_MATRIX_DISTRIBUTION_BY_GENDER_VIEW_BUILDER.table_for_query.dataset_id,
    view=REVOCATIONS_MATRIX_DISTRIBUTION_BY_GENDER_VIEW_BUILDER.table_for_query.table_id,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATION_MATRIX_DISTRIBUTION_BY_GENDER_COMPARISON_VIEW_BUILDER.build_and_print()
