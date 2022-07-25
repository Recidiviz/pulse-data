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
"""Query joining all CB 971 reports from MI that report incarceration population numbers
for Michigan."""
from datetime import date

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

DATES_WITH_AVAILABLE_DATA = [
    date(2018, 1, 1),
    date(2018, 2, 1),
    date(2018, 3, 1),
    date(2018, 4, 1),
    date(2018, 5, 1),
    date(2018, 6, 1),
    date(2018, 7, 1),
    date(2018, 8, 1),
    date(2018, 9, 1),
    date(2018, 10, 1),
    date(2018, 11, 1),
    date(2018, 12, 1),
    date(2019, 1, 1),
    date(2019, 2, 1),
    date(2019, 3, 1),
    date(2019, 4, 1),
    date(2019, 5, 1),
    date(2019, 6, 1),
    date(2019, 7, 1),
    date(2019, 8, 1),
    date(2019, 9, 1),
    date(2019, 10, 1),
    date(2019, 11, 1),
    date(2019, 12, 1),
    date(2020, 2, 1),
    date(2020, 3, 1),
    date(2020, 4, 1),
    date(2020, 5, 1),
    date(2020, 6, 1),
    date(2020, 7, 1),
    date(2020, 8, 1),
    date(2020, 9, 1),
    date(2020, 10, 1),
    date(2020, 11, 1),
    date(2020, 12, 1),
    date(2021, 1, 1),
    date(2021, 2, 1),
    date(2021, 3, 1),
    date(2021, 4, 1),
    date(2021, 5, 1),
    date(2021, 6, 1),
    date(2021, 7, 1),
    date(2021, 8, 1),
    date(2021, 9, 1),
    date(2021, 10, 1),
    date(2021, 11, 1),
    date(2021, 12, 1),
    date(2022, 1, 1),
    date(2022, 2, 1),
    date(2022, 3, 1),
    date(2022, 4, 1),
    date(2022, 5, 1),
    date(2022, 7, 22),
]


def query_template(date_of_data: date) -> str:
    return f"SELECT *, DATE('{date_of_data.strftime('%Y-%m-%d')}') AS date_of_stay FROM `{{project_id}}.{{us_mi_validation_oneoff_dataset}}.cb_971_{date_of_data.strftime('%Y%m%d')}`\n"


VIEW_QUERY_TEMPLATE = "UNION ALL\n".join(
    [query_template(d) for d in DATES_WITH_AVAILABLE_DATA]
)


CB_971_REPORT_UNIFIED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.validation_dataset_for_state(StateCode.US_MI),
    view_id="cb_971_report_unified",
    description="A unified view of all CB 971 reports that report incarceration population numbers for MIDOC.",
    view_query_template=VIEW_QUERY_TEMPLATE,
    should_materialize=True,
    us_mi_validation_oneoff_dataset=dataset_config.validation_oneoff_dataset_for_state(
        StateCode.US_MI
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CB_971_REPORT_UNIFIED_VIEW_BUILDER.build_and_print()
