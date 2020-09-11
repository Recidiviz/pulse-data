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
"""US_ND special releases due to COVID concerns"""
# pylint: disable=trailing-whitespace,line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import COVID_REPORT_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ND_COVID_SPECIAL_RELEASES_VIEW_NAME = 'us_nd_covid_special_releases'

US_ND_COVID_SPECIAL_RELEASES_DESCRIPTION = \
    """US_ND special releases due to COVID concerns"""

# TODO(3954): pull the ND COVID special release data directly from the us_nd_raw_data in BQ instead
US_ND_COVID_SPECIAL_RELEASES_QUERY_TEMPLATE = \
    """
    /*{description}*/
    -- Person_ids for those released specifically for COVID concerns
    -- identified in Docstars Contacts column C2 = 19
    SELECT
      state_code,
      person_id
    -- TODO(3954): pull this data from the US ND raw dataset instead
    FROM `{project_id}.{covid_report_dataset}.us_nd_covid_release_person_external_id`
    JOIN `{project_id}.{state_dataset}.state_person_external_id`
      USING (state_code, external_id)
    WHERE id_type = 'US_ND_SID'
    ORDER BY state_code, person_id
"""


US_ND_COVID_SPECIAL_RELEASES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.COVID_REPORT_DATASET,
    state_dataset=dataset_config.STATE_BASE_DATASET,
    view_id=US_ND_COVID_SPECIAL_RELEASES_VIEW_NAME,
    view_query_template=US_ND_COVID_SPECIAL_RELEASES_QUERY_TEMPLATE,
    description=US_ND_COVID_SPECIAL_RELEASES_DESCRIPTION,
    covid_report_dataset=COVID_REPORT_DATASET,
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ND_COVID_SPECIAL_RELEASES_VIEW_BUILDER.build_and_print()
