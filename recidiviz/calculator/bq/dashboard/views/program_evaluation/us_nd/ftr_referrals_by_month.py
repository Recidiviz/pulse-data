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
"""Month over month count for all new referrals to Free Through Recovery."""
# pylint: disable=line-too-long, trailing-whitespace

from recidiviz.calculator.bq import bqview, export_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
BASE_DATASET = export_config.STATE_BASE_TABLES_BQ_DATASET

FTR_REFERRALS_BY_MONTH_VIEW_NAME = \
    'ftr_referrals_by_month'

FTR_REFERRALS_BY_MONTH_DESCRIPTION = """
 Month over month count for all new referrals to Free Through Recovery.
"""

FTR_REFERRALS_BY_MONTH_QUERY = \
    """
    /*{description}*/

    SELECT state_code,
           EXTRACT(YEAR FROM referral_date) as year,
           EXTRACT(MONTH FROM referral_date) as month,
           count(*) as count
    FROM `{project_id}.{base_dataset}.state_program_assignment`
    GROUP BY state_code, year, month
    """.format(
        description=
        FTR_REFERRALS_BY_MONTH_DESCRIPTION,
        project_id=PROJECT_ID,
        base_dataset=BASE_DATASET,
    )

FTR_REFERRALS_BY_MONTH_VIEW = bqview.BigQueryView(
    view_id=FTR_REFERRALS_BY_MONTH_VIEW_NAME,
    view_query=FTR_REFERRALS_BY_MONTH_QUERY
)

if __name__ == '__main__':
    print(FTR_REFERRALS_BY_MONTH_VIEW.view_id)
    print(FTR_REFERRALS_BY_MONTH_VIEW.view_query)
