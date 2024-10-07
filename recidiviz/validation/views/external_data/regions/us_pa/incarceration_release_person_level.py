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
"""A view detailing incarceration releases at the person level for Pennsylvania."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config


def month_specific_table_query_template(
    year: int, month: int, date_col_name: str
) -> str:
    """query template for tables with mov_date_std col"""
    return f"""
SELECT
    'US_PA' AS state_code,
    control_number as person_external_id,
    'US_PA_CONT' as external_id_type,
    PARSE_DATE('%m/%d/%Y', {date_col_name}) as release_date
FROM `{{project_id}}.{{us_pa_validation_oneoff_dataset}}.{year}_{month:02}_incarceration_releases`
"""


MONTHS_WITH_AVAILABLE_DATA = {
    "mov_date_std": [
        (2021, 3),
        (2021, 4),
        (2021, 5),
        (2021, 6),
        (2021, 9),
        (2021, 10),
        (2021, 11),
    ],
    "move_dt_std": [
        (2021, 7),
        (2021, 8),
        (2021, 12),
        (2022, 1),
    ],
}


VIEW_QUERY_TEMPLATE = "UNION ALL\n".join(
    [
        month_specific_table_query_template(year, month, date_col)
        for date_col, months in MONTHS_WITH_AVAILABLE_DATA.items()
        for (year, month) in months
    ]
)


US_PA_INCARCERATION_RELEASE_PERSON_LEVEL_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.validation_dataset_for_state(StateCode.US_PA),
    view_id="incarceration_release_person_level",
    description="A view detailing incarceration releases at the person level for Pennsylvania",
    should_materialize=True,
    view_query_template=VIEW_QUERY_TEMPLATE,
    us_pa_validation_oneoff_dataset=dataset_config.validation_oneoff_dataset_for_state(
        StateCode.US_PA
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_PA_INCARCERATION_RELEASE_PERSON_LEVEL_VIEW_BUILDER.build_and_print()
