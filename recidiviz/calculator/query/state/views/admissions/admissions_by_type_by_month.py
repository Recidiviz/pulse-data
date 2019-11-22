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
"""Admissions by type by month"""
# pylint: disable=line-too-long, trailing-whitespace
from recidiviz.calculator.query import export_config, bqview
from recidiviz.calculator.query.state import view_config

from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
BASE_DATASET = export_config.STATE_BASE_TABLES_BQ_DATASET
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET

ADMISSIONS_BY_TYPE_BY_MONTH_VIEW_NAME = 'admissions_by_type_by_month'

ADMISSIONS_BY_TYPE_BY_MONTH_DESCRIPTION = """ Admissions by type by month """

ADMISSIONS_BY_TYPE_BY_MONTH_QUERY = \
    """
/*{description}*/

SELECT state_code, year, month, COUNTIF(admission_reason = 'NEW_ADMISSION') as new_admissions,
COUNTIF(admission_reason in ('PAROLE_REVOCATION', 'PROBATION_REVOCATION') AND violation_type = 'TECHNICAL') as technicals,
COUNTIF(admission_reason in ('PAROLE_REVOCATION', 'PROBATION_REVOCATION') AND violation_type != 'TECHNICAL') as non_technicals,
COUNTIF(admission_reason in ('PAROLE_REVOCATION', 'PROBATION_REVOCATION') AND violation_type is null) as unknown_revocations, count(*) as total_admissions
FROM
(SELECT inc.state_code, EXTRACT(YEAR FROM admission_date) as year, EXTRACT(MONTH FROM admission_date) as month, inc.admission_reason, viol.violation_type
FROM `{project_id}.{views_dataset}.incarceration_admissions_by_person_and_month` inc
LEFT JOIN `{project_id}.{base_dataset}.state_supervision_violation_response` resp
ON inc.source_supervision_violation_response_id = resp.supervision_violation_response_id
LEFT JOIN `{project_id}.{base_dataset}.state_supervision_violation` viol 
ON resp.supervision_violation_id = viol.supervision_violation_id)
GROUP BY state_code, year, month having year > EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE(), INTERVAL -3 YEAR))
ORDER BY year, month
""".format(
        description=ADMISSIONS_BY_TYPE_BY_MONTH_DESCRIPTION,
        project_id=PROJECT_ID,
        base_dataset=BASE_DATASET,
        views_dataset=VIEWS_DATASET,
    )

ADMISSIONS_BY_TYPE_BY_MONTH_VIEW = bqview.BigQueryView(
    view_id=ADMISSIONS_BY_TYPE_BY_MONTH_VIEW_NAME,
    view_query=ADMISSIONS_BY_TYPE_BY_MONTH_QUERY
)

if __name__ == '__main__':
    print(ADMISSIONS_BY_TYPE_BY_MONTH_VIEW.view_id)
    print(ADMISSIONS_BY_TYPE_BY_MONTH_VIEW.view_query)
