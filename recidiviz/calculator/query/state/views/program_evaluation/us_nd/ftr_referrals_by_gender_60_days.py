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
"""All individuals who have been referred to Free Through Recovery in the last
60 days, broken down by gender.
"""
# pylint: disable=line-too-long, trailing-whitespace

from recidiviz.calculator.query import export_config, bqview
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
BASE_DATASET = export_config.STATE_BASE_TABLES_BQ_DATASET
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET

FTR_REFERRALS_BY_GENDER_60_DAYS_VIEW_NAME = \
    'ftr_referrals_by_gender_60_days'

FTR_REFERRALS_BY_GENDER_60_DAYS_DESCRIPTION = """
 All individuals who have been referred to Free Through Recovery in the last
 60 days, broken down by gender.
"""

FTR_REFERRALS_BY_GENDER_60_DAYS_QUERY = \
    """
    /*{description}*/

    SELECT state_code, gender, count(*) AS count
    FROM `{project_id}.{views_dataset}.ftr_referrals_60_days` ftr_referrals
    JOIN `{project_id}.{base_dataset}.state_person` state_persons
    ON state_persons.person_id = ftr_referrals.person_id
    GROUP BY state_code, gender
    """.format(
        description=
        FTR_REFERRALS_BY_GENDER_60_DAYS_DESCRIPTION,
        project_id=PROJECT_ID,
        base_dataset=BASE_DATASET,
        views_dataset=VIEWS_DATASET,
    )

FTR_REFERRALS_BY_GENDER_60_DAYS_VIEW = bqview.BigQueryView(
    view_id=FTR_REFERRALS_BY_GENDER_60_DAYS_VIEW_NAME,
    view_query=FTR_REFERRALS_BY_GENDER_60_DAYS_QUERY
)

if __name__ == '__main__':
    print(FTR_REFERRALS_BY_GENDER_60_DAYS_VIEW.view_id)
    print(FTR_REFERRALS_BY_GENDER_60_DAYS_VIEW.view_query)
