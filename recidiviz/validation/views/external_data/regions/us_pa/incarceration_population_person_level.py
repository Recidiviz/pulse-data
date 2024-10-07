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
"""A view detailing the incarceration population at the person level for Pennsylvania."""
from datetime import date

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

MONTHS_WITH_AVAILABLE_DATA = [
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
]


def from_query_template(month: date) -> str:
    return f"""
SELECT 
    'US_PA' AS state_code,
    control_number as person_external_id, 
    'US_PA_CONT' as external_id_type,
    LAST_DAY(DATE '{month.strftime('%Y-%m-%d')}', MONTH) as date_of_stay, 
    CurrLoc_Cd as facility 
FROM `{{project_id}}.{{us_pa_validation_oneoff_dataset}}.{month.strftime('%Y_%m')}_incarceration_population`
"""


TIMESERIES_DATA = [from_query_template(month) for month in MONTHS_WITH_AVAILABLE_DATA]

SUPPLEMENTAL_DATA = """
SELECT 
    region_code AS state_code,
    person_external_id, 
    'US_PA_CONT' as external_id_type, 
    date_of_stay, 
    facility
FROM `{project_id}.{us_pa_validation_oneoff_dataset}.incarceration_population_person_level_raw`
"""

VIEW_QUERY_TEMPLATE = "UNION ALL\n".join([SUPPLEMENTAL_DATA, *TIMESERIES_DATA])


US_PA_INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.validation_dataset_for_state(StateCode.US_PA),
    view_id="incarceration_population_person_level",
    description="A view detailing the incarceration population at the person level for Pennsylvania",
    view_query_template=VIEW_QUERY_TEMPLATE,
    should_materialize=True,
    us_pa_validation_oneoff_dataset=dataset_config.validation_oneoff_dataset_for_state(
        StateCode.US_PA
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_PA_INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER.build_and_print()
