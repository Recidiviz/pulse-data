# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""View tracking characteristics/composition of the supervision population in each
district by month"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.calculator.query.state.views.analyst_data.supervision_population_attributes_template import (
    supervision_population_attributes_template,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_POPULATION_ATTRIBUTES_BY_SUPERVISION_OFFICE_BY_MONTH_VIEW_NAME = (
    "supervision_population_attributes_by_supervision_office_by_month"
)

SUPERVISION_POPULATION_ATTRIBUTES_BY_SUPERVISION_OFFICE_BY_MONTH_VIEW_DESCRIPTION = (
    "Captures demographic composition of supervision population for a given month by "
    "supervision office"
)

SUPERVISION_POPULATION_ATTRIBUTES_BY_SUPERVISION_OFFICE_BY_MONTH_QUERY_TEMPLATE = (
    f"""
    SELECT
        s.*,
        population_density
    FROM
    (
        {supervision_population_attributes_template("supervision_office")}
    ) AS s
    """
    + """
    LEFT JOIN
        `{project_id}.{analyst_dataset}.population_density_by_supervision_office` p
    USING
        (state_code, supervision_office)
    """
)

SUPERVISION_POPULATION_ATTRIBUTES_BY_SUPERVISION_OFFICE_BY_MONTH_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=SUPERVISION_POPULATION_ATTRIBUTES_BY_SUPERVISION_OFFICE_BY_MONTH_VIEW_NAME,
    view_query_template=SUPERVISION_POPULATION_ATTRIBUTES_BY_SUPERVISION_OFFICE_BY_MONTH_QUERY_TEMPLATE,
    description=SUPERVISION_POPULATION_ATTRIBUTES_BY_SUPERVISION_OFFICE_BY_MONTH_VIEW_DESCRIPTION,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_POPULATION_ATTRIBUTES_BY_SUPERVISION_OFFICE_BY_MONTH_VIEW_BUILDER.build_and_print()
