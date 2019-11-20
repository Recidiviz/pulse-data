# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Squashes race and ethnicity for all people into a single table."""
from recidiviz.calculator.query import export_config, bqview

from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
BASE_DATASET = export_config.STATE_BASE_TABLES_BQ_DATASET

STATE_PERSON_RACE_AND_ETHNICITY_VIEW_NAME = 'state_person_race_and_ethnicity'

STATE_PERSON_RACE_AND_ETHNICITY_DESCRIPTION = \
    """Race and Ethnicity information put into a single table."""

STATE_PERSON_RACE_AND_ETHNICITY_QUERY = \
    """
/*{description}*/

SELECT state_code, person_id, race_or_ethnicity
FROM (SELECT state_code, person_id, race as race_or_ethnicity
      FROM `{project_id}.{base_dataset}.state_person_race`
      UNION ALL
      SELECT state_code, person_id, ethnicity as race_or_ethnicity
      FROM `{project_id}.{base_dataset}.state_person_ethnicity`)
""".format(
        description=STATE_PERSON_RACE_AND_ETHNICITY_DESCRIPTION,
        project_id=PROJECT_ID,
        base_dataset=BASE_DATASET,
    )

STATE_PERSON_RACE_AND_ETHNICITY_VIEW = bqview.BigQueryView(
    view_id=STATE_PERSON_RACE_AND_ETHNICITY_VIEW_NAME,
    view_query=STATE_PERSON_RACE_AND_ETHNICITY_QUERY
)

if __name__ == '__main__':
    print(STATE_PERSON_RACE_AND_ETHNICITY_VIEW.view_id)
    print(STATE_PERSON_RACE_AND_ETHNICITY_VIEW.view_query)
