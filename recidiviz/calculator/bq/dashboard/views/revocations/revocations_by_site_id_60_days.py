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
"""Revocations by site_id in the last 60 days."""
# pylint: disable=line-too-long, trailing-whitespace

from recidiviz.calculator.bq import bqview
from recidiviz.calculator.bq.dashboard.views import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET

REVOCATIONS_BY_SITE_ID_60_DAYS_VIEW_NAME = 'revocations_by_site_id_60_days'

REVOCATIONS_BY_SITE_ID_60_DAYS_DESCRIPTION = """
 Revocations by site_id in the last 60 days.
 This counts all individuals admitted to prison for a revocation
 of probation or parole, broken down by the site_id of the agent on the
 source_supervision_violation_response, and by the violation type of the
 supervision violation.
 """

REVOCATIONS_BY_SITE_ID_60_DAYS_QUERY = \
    """
    /*{description}*/

    SELECT state_code, site_id, SUM(absconsion_count) as absconsion_count, SUM(felony_count) as felony_count, SUM(technical_count) as technical_count, SUM(unknown_count) as unknown_count
    FROM `{project_id}.{views_dataset}.revocations_by_officer_60_days`
    GROUP BY state_code, site_id 
    ORDER BY site_id asc
    """.format(
        description=REVOCATIONS_BY_SITE_ID_60_DAYS_DESCRIPTION,
        project_id=PROJECT_ID,
        views_dataset=VIEWS_DATASET,
    )

REVOCATIONS_BY_SITE_ID_60_DAYS_VIEW = bqview.BigQueryView(
    view_id=REVOCATIONS_BY_SITE_ID_60_DAYS_VIEW_NAME,
    view_query=REVOCATIONS_BY_SITE_ID_60_DAYS_QUERY
)

if __name__ == '__main__':
    print(REVOCATIONS_BY_SITE_ID_60_DAYS_VIEW.view_id)
    print(REVOCATIONS_BY_SITE_ID_60_DAYS_VIEW.view_query)
