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
"""Query joining the supervision section of all CB 971 reports from MI that report population numbers
for Michigan."""
from datetime import date

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

DATES_WITH_AVAILABLE_DATA = [
    date(2022, 7, 22),
    date(2022, 8, 19),
    date(2022, 9, 23),
    date(2022, 10, 21),
    date(2022, 12, 23),
    date(2023, 1, 20),
    date(2023, 2, 24),
    date(2023, 3, 24),
    date(2023, 4, 21),
    date(2023, 5, 19),
    date(2023, 6, 30),
    date(2023, 7, 21),
    date(2023, 8, 18),
    date(2023, 9, 29),
    date(2023, 10, 27),
    date(2023, 11, 24),
]


def query_template(date_of_data: date) -> str:
    return f"SELECT *, DATE('{date_of_data.strftime('%Y-%m-%d')}') AS date_of_supervision FROM `{{project_id}}.{{us_mi_validation_oneoff_dataset}}.cb_971_supervision_{date_of_data.strftime('%Y%m%d')}`\n"


VIEW_QUERY_TEMPLATE = "UNION ALL\n".join(
    [query_template(d) for d in DATES_WITH_AVAILABLE_DATA]
)


CB_971_REPORT_SUPERVISION_UNIFIED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.validation_dataset_for_state(StateCode.US_MI),
    view_id="cb_971_report_supervision_unified",
    description="A unified view of the supervision section of all CB 971 reports that report population numbers for MIDOC.",
    view_query_template=VIEW_QUERY_TEMPLATE,
    should_materialize=True,
    us_mi_validation_oneoff_dataset=dataset_config.validation_oneoff_dataset_for_state(
        StateCode.US_MI
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CB_971_REPORT_SUPERVISION_UNIFIED_VIEW_BUILDER.build_and_print()
