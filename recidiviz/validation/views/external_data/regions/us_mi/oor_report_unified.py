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
"""Query joining all OOR reports from MI that report incarceration per-person numbers
for Michigan."""
from datetime import date

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

DATES_WITH_AVAILABLE_DATA = [
    date(2020, 1, 8),
    date(2020, 1, 15),
    date(2020, 1, 22),
    date(2020, 1, 29),
    date(2020, 2, 5),
    date(2020, 2, 12),
    date(2020, 2, 19),
    date(2020, 2, 26),
    date(2020, 2, 27),
    date(2020, 3, 11),
    date(2020, 3, 18),
    date(2020, 3, 25),
    date(2020, 4, 1),
    date(2020, 4, 7),
    date(2020, 4, 8),
    date(2020, 4, 15),
    date(2020, 4, 22),
    date(2020, 5, 13),
    date(2020, 5, 14),
    date(2020, 5, 15),
    date(2020, 5, 27),
    date(2020, 6, 3),
    date(2020, 6, 10),
    date(2020, 6, 17),
    date(2020, 6, 24),
    date(2020, 7, 1),
    date(2020, 7, 8),
    date(2020, 7, 15),
    date(2020, 7, 22),
    date(2020, 7, 29),
    date(2020, 8, 5),
    date(2020, 8, 12),
    date(2020, 8, 19),
    date(2020, 8, 26),
    date(2020, 9, 2),
    date(2020, 9, 9),
    date(2020, 9, 16),
    date(2020, 9, 23),
    date(2020, 9, 30),
    date(2020, 10, 9),
    date(2020, 10, 14),
    date(2020, 10, 21),
    date(2020, 10, 28),
    date(2020, 11, 4),
    date(2020, 11, 13),
    date(2020, 11, 18),
    date(2020, 11, 25),
    date(2020, 12, 2),
    date(2020, 12, 4),
    date(2020, 12, 9),
    date(2020, 12, 16),
    date(2020, 12, 18),
    date(2020, 12, 23),
    date(2020, 12, 30),
    date(2021, 1, 6),
    date(2021, 1, 13),
    date(2021, 1, 20),
    date(2021, 1, 27),
    date(2021, 2, 3),
    date(2021, 2, 10),
    date(2021, 2, 11),
    date(2021, 2, 17),
    date(2021, 2, 24),
    date(2021, 3, 3),
    date(2021, 3, 17),
    date(2021, 3, 19),
    date(2021, 3, 24),
    date(2021, 3, 31),
    date(2021, 4, 7),
    date(2021, 4, 14),
    date(2021, 4, 21),
    date(2021, 4, 28),
    date(2021, 4, 29),
    date(2021, 5, 5),
    date(2021, 5, 12),
    date(2021, 5, 19),
    date(2021, 5, 26),
    date(2021, 6, 2),
    date(2021, 6, 11),
    date(2021, 6, 16),
    date(2021, 6, 23),
    date(2021, 7, 1),
    date(2021, 7, 7),
    date(2021, 7, 14),
    date(2021, 7, 21),
    date(2021, 7, 28),
    date(2021, 8, 4),
    date(2021, 8, 11),
    date(2021, 9, 9),
    date(2021, 9, 15),
    date(2021, 9, 16),
    date(2021, 9, 22),
    date(2021, 9, 29),
    date(2021, 10, 6),
    date(2021, 10, 13),
    date(2021, 10, 27),
    date(2021, 11, 10),
    date(2021, 11, 17),
    date(2021, 12, 1),
    date(2021, 12, 8),
    date(2021, 12, 15),
    date(2021, 12, 22),
    date(2022, 1, 5),
    date(2022, 1, 13),
    date(2022, 1, 19),
    date(2022, 1, 28),
    date(2022, 2, 3),
    date(2022, 2, 23),
    date(2022, 3, 2),
    date(2022, 3, 14),
    date(2022, 3, 16),
    date(2022, 3, 23),
    date(2022, 3, 30),
    date(2022, 4, 6),
    date(2022, 4, 20),
    date(2022, 4, 27),
    date(2022, 5, 4),
    date(2022, 5, 6),
    date(2022, 5, 10),
    date(2022, 5, 11),
    date(2022, 5, 13),
    date(2022, 5, 16),
    date(2022, 5, 18),
    date(2022, 5, 19),
    date(2022, 5, 25),
    date(2022, 5, 31),
    date(2022, 6, 1),
    date(2022, 6, 8),
    date(2022, 6, 15),
    date(2022, 6, 22),
    date(2022, 6, 29),
    date(2022, 7, 6),
]


def query_template(date_of_data: date) -> str:
    return f"SELECT Offender_Number AS person_external_id, DATE('{date_of_data.strftime('%Y-%m-%d')}') AS date_of_stay, Location AS facility FROM `{{project_id}}.{{us_mi_validation_oneoff_dataset}}.orc_report_{date_of_data.strftime('%Y%m%d')}` WHERE Unique = 'Yes'\n"


VIEW_QUERY_TEMPLATE = "UNION ALL\n".join(
    [query_template(d) for d in DATES_WITH_AVAILABLE_DATA]
)

OOR_REPORT_UNIFIED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.validation_dataset_for_state(StateCode.US_MI),
    view_id="oor_report_unified",
    description="A unified view of all OOR reports that report per person information for MIDOC.",
    view_query_template=VIEW_QUERY_TEMPLATE,
    should_materialize=True,
    us_mi_validation_oneoff_dataset=dataset_config.validation_oneoff_dataset_for_state(
        StateCode.US_MI
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        OOR_REPORT_UNIFIED_VIEW_BUILDER.build_and_print()
