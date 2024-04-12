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
    date(2022, 7, 22),
    date(2022, 7, 27),
    date(2022, 8, 3),
    date(2022, 8, 17),
    date(2022, 8, 24),
    date(2022, 9, 7),
    date(2022, 9, 15),
    date(2022, 9, 21),
    date(2022, 9, 28),
    date(2022, 10, 5),
    date(2022, 10, 19),
    date(2022, 10, 26),
    date(2022, 11, 2),
    date(2022, 11, 9),
    date(2022, 11, 16),
    date(2022, 12, 7),
    date(2022, 12, 14),
    date(2022, 12, 21),
    date(2022, 12, 28),
    date(2023, 1, 4),
    date(2023, 1, 11),
    date(2023, 1, 18),
    date(2023, 1, 25),
    date(2023, 3, 8),
    date(2023, 3, 15),
    date(2023, 3, 22),
    date(2023, 4, 5),
    date(2023, 4, 12),
    date(2023, 4, 19),
    date(2023, 4, 26),
    date(2023, 5, 3),
    date(2023, 5, 10),
    date(2023, 5, 24),
    date(2023, 6, 7),
    date(2023, 6, 16),
    date(2023, 6, 21),
    date(2023, 7, 5),
    date(2023, 7, 12),
    date(2023, 7, 19),
    date(2023, 7, 27),
    date(2023, 8, 4),
    date(2023, 8, 9),
    date(2023, 8, 17),
    date(2023, 8, 23),
    date(2023, 8, 31),
    date(2023, 9, 6),
    date(2023, 9, 13),
    date(2023, 9, 20),
    date(2023, 9, 27),
    date(2023, 10, 4),
    date(2023, 10, 11),
    date(2023, 10, 18),
    date(2023, 10, 25),
    date(2023, 10, 26),
    date(2023, 11, 2),
    date(2023, 11, 8),
    date(2023, 11, 15),
    date(2023, 11, 22),
    date(2023, 11, 30),
    date(2023, 12, 11),
    date(2023, 12, 13),
    date(2023, 12, 15),
    date(2023, 12, 20),
    date(2024, 1, 10),
    date(2024, 1, 19),
    date(2024, 1, 24),
    date(2024, 1, 31),
    date(2024, 2, 7),
    date(2024, 2, 14),
    date(2024, 2, 28),
    date(2024, 3, 4),
    date(2024, 3, 7),
    date(2024, 3, 11),
    date(2024, 3, 13),
    date(2024, 3, 18),
    date(2024, 3, 20),
    date(2024, 3, 25),
    date(2024, 4, 3),
]


def query_template(date_of_data: date) -> str:

    if date_of_data < date(2023, 7, 5):
        return f"SELECT Offender_Number AS person_external_id,DATE('{date_of_data.strftime('%Y-%m-%d')}') AS date_of_stay, Location AS facility, Confinement_Level, Management_Level, True_Sec_Level, Actual_Sec_Level, STG, NULL AS MH_Treatment FROM `{{project_id}}.{{us_mi_validation_oneoff_dataset}}.orc_report_{date_of_data.strftime('%Y%m%d')}` WHERE Unique = 'Yes'\n"

    return f"SELECT Offender_Number AS person_external_id,DATE('{date_of_data.strftime('%Y-%m-%d')}') AS date_of_stay, Location AS facility, Confinement_Level, Management_Level, True_Sec_Level, Actual_Sec_Level, STG, MH_Treatment FROM `{{project_id}}.{{us_mi_validation_oneoff_dataset}}.orc_report_{date_of_data.strftime('%Y%m%d')}` WHERE Unique = 'Yes'\n"


_SUBQUERY_TEMPLATE = "UNION ALL\n".join(
    [query_template(d) for d in DATES_WITH_AVAILABLE_DATA]
)

VIEW_QUERY_TEMPLATE = f"""
SELECT
  'US_MI' as region_code,
  person_external_id,
  'US_MI_DOC' as external_id_type,
  date_of_stay,
  facility,
  Confinement_Level, 
  Management_Level, 
  True_Sec_Level, 
  Actual_Sec_Level, 
  STG, 
  MH_Treatment
FROM (
    {_SUBQUERY_TEMPLATE}
)
"""

US_MI_INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.validation_dataset_for_state(StateCode.US_MI),
    view_id="incarceration_population_person_level",
    description="A unified view of all OOR reports that report per person information for MIDOC.",
    view_query_template=VIEW_QUERY_TEMPLATE,
    should_materialize=True,
    us_mi_validation_oneoff_dataset=dataset_config.validation_oneoff_dataset_for_state(
        StateCode.US_MI
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MI_INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER.build_and_print()
