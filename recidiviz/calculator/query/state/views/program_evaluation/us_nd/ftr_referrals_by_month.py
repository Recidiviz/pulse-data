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
"""Month over month count for the unique number of people who were referred
to Free Through Recovery."""
# pylint: disable=trailing-whitespace

from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
REFERENCE_DATASET = view_config.REFERENCE_TABLES_DATASET

FTR_REFERRALS_BY_MONTH_VIEW_NAME = \
    'ftr_referrals_by_month'

FTR_REFERRALS_BY_MONTH_DESCRIPTION = """
 Month over month count for the unique number of people who were referred
 to Free Through Recovery.
"""

FTR_REFERRALS_BY_MONTH_QUERY = \
    """
    /*{description}*/
    SELECT
      state_code,
      year,
      month,
      IFNULL(ref.count, 0) AS count,
      total_supervision_count,
      supervision_type,
      district
    FROM (
      SELECT
        state_code, year, month,
        COUNT(DISTINCT person_id) AS total_supervision_count,
        supervision_type,
        district
      FROM `{project_id}.{reference_dataset}.event_based_supervision_populations`
      GROUP BY state_code, year, month, supervision_type, district
    ) pop
    LEFT JOIN (
      SELECT
        state_code, year, month,
        COUNT(DISTINCT person_id) AS count,
        supervision_type,
        district
      FROM `{project_id}.{reference_dataset}.event_based_program_referrals`
      GROUP BY state_code, year, month, supervision_type, district
    ) ref
    USING (state_code, year, month, supervision_type, district)
    WHERE supervision_type in ('ALL', 'PAROLE', 'PROBATION')
      AND district IS NOT NULL
      AND state_code = 'US_ND'
    ORDER BY state_code, year, month, district, supervision_type
    """.format(
        description=FTR_REFERRALS_BY_MONTH_DESCRIPTION,
        project_id=PROJECT_ID,
        reference_dataset=REFERENCE_DATASET,
    )

FTR_REFERRALS_BY_MONTH_VIEW = bqview.BigQueryView(
    view_id=FTR_REFERRALS_BY_MONTH_VIEW_NAME,
    view_query=FTR_REFERRALS_BY_MONTH_QUERY
)

if __name__ == '__main__':
    print(FTR_REFERRALS_BY_MONTH_VIEW.view_id)
    print(FTR_REFERRALS_BY_MONTH_VIEW.view_query)
