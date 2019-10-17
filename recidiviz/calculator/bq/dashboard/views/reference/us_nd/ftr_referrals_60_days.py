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
"""All individuals who have been referred to Free Through Recovery within
the last 60 days.
"""
# pylint: disable=line-too-long, trailing-whitespace

from recidiviz.calculator.bq import bqview, export_config

from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
BASE_DATASET = export_config.STATE_BASE_TABLES_BQ_DATASET

FTR_REFERRAL_VIEW_NAME = 'ftr_referrals_60_days'

FTR_REFERRAL_DESCRIPTION = \
    """All individuals who have been referred to Free Through Recovery within
    the last 60 days.
    """

# TODO(2549): Filter by FTR specifically once the metadata exists.
FTR_REFERRAL_QUERY = \
    """
/*{description}*/
SELECT person_id, state_code
    FROM `{project_id}.{base_dataset}.state_program_assignment`
    WHERE referral_date IS NOT NULL
    AND referral_date BETWEEN (DATE_ADD(CURRENT_DATE(), INTERVAL -60 DAY)) AND CURRENT_DATE()
    AND start_date <= CURRENT_DATE()
    GROUP BY person_id, state_code
""".format(
        description=FTR_REFERRAL_DESCRIPTION,
        project_id=PROJECT_ID,
        base_dataset=BASE_DATASET,
    )

FTR_REFERRAL_VIEW = bqview.BigQueryView(
    view_id=FTR_REFERRAL_VIEW_NAME,
    view_query=FTR_REFERRAL_QUERY
)

if __name__ == '__main__':
    print(FTR_REFERRAL_VIEW.view_id)
    print(FTR_REFERRAL_VIEW.view_query)
