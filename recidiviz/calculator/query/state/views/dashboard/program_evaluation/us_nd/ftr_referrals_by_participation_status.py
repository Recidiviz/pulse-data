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
"""Month over month count of each participation status for the Free Through Recovery referrals."""
# pylint: disable=trailing-whitespace

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import view_config
from recidiviz.utils.environment import GAE_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

FTR_REFERRALS_BY_PARTICIPATION_STATUS_VIEW_NAME = 'ftr_referrals_by_participation_status'

FTR_REFERRALS_BY_PARTICIPATION_STATUS_DESCRIPTION = """
 Month over month count for the participation status of each Free Through Recovery program referral.
"""

FTR_REFERRALS_BY_PARTICIPATION_STATUS_QUERY_TEMPLATE = \
    """
    /*{description}*/
    SELECT
      state_code, year, month,
      supervision_type, district,
      COUNT(DISTINCT IF(participation_status = 'IN_PROGRESS', person_id, NULL)) AS in_progress,
      COUNT(DISTINCT IF(participation_status = 'DISCHARGED', person_id, NULL)) AS discharged,
      COUNT(DISTINCT IF(participation_status = 'DENIED', person_id, NULL)) AS denied,
      COUNT(DISTINCT IF(participation_status = 'PENDING', person_id, NULL)) AS pending
    FROM `{project_id}.{reference_dataset}.event_based_program_referrals`
    WHERE supervision_type in ('ALL', 'PAROLE', 'PROBATION')
      AND state_code = 'US_ND'
    GROUP BY state_code, year, month, district, supervision_type
    ORDER BY state_code, year, month, district, supervision_type
    """

FTR_REFERRALS_BY_PARTICIPATION_STATUS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=view_config.DASHBOARD_VIEWS_DATASET,
    view_id=FTR_REFERRALS_BY_PARTICIPATION_STATUS_VIEW_NAME,
    view_query_template=FTR_REFERRALS_BY_PARTICIPATION_STATUS_QUERY_TEMPLATE,
    description=FTR_REFERRALS_BY_PARTICIPATION_STATUS_DESCRIPTION,
    reference_dataset=view_config.REFERENCE_TABLES_DATASET,
)

if __name__ == '__main__':
    with local_project_id_override(GAE_PROJECT_STAGING):
        FTR_REFERRALS_BY_PARTICIPATION_STATUS_VIEW_BUILDER.build_and_print()
