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
from recidiviz.calculator.bq import bqview, export_config

from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
BASE_DATASET = export_config.STATE_BASE_TABLES_BQ_DATASET

ADMISSIONS_BY_TYPE_BY_MONTH_VIEW_NAME = 'admissions_by_type_by_month'

ADMISSIONS_BY_TYPE_BY_MONTH_DESCRIPTION = """ Admissions by type by month """

ADMISSIONS_BY_TYPE_BY_MONTH_QUERY = \
    """
/*{description}*/

SELECT newadm.state_code, newadm.year, newadm.month, IFNULL(new_admissions, 0) as new_admissions, IFNULL(technicals, 0) as technicals, IFNULL(non_technicals, 0) as non_technicals, IFNULL(unknown_revocations, 0) as unknown_revocations FROM
-- Technical Revocations
(SELECT state_code, EXTRACT(YEAR FROM admission_date) as year, EXTRACT(MONTH FROM admission_date) as month, IFNULL(count(*), 0) as technicals
FROM
(SELECT inc.state_code, admission_date, violation_type as admission_type
FROM `{project_id}.{base_dataset}.state_incarceration_period` inc
JOIN `{project_id}.{base_dataset}.state_supervision_violation_response` resp
ON inc.source_supervision_violation_response_id = resp.supervision_violation_response_id
JOIN `{project_id}.{base_dataset}.state_supervision_violation` viol 
ON resp.supervision_violation_id = viol.supervision_violation_id
WHERE viol.violation_type = 'TECHNICAL') 
GROUP BY state_code, year, month having year > EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE(), INTERVAL -3 YEAR))) tech
FULL OUTER JOIN
-- New Admissions
(SELECT state_code, EXTRACT(YEAR FROM admission_date) as year, EXTRACT(MONTH FROM admission_date) as month, IFNULL(count(*), 0) as new_admissions
FROM `{project_id}.{base_dataset}.state_incarceration_period`
WHERE admission_reason = 'NEW_ADMISSION'
GROUP BY state_code, year, month having year > EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE(), INTERVAL -3 YEAR))) newadm
ON tech.year = newadm.year AND tech.month = newadm.month
FULL OUTER JOIN
-- Unknown Revocations
(SELECT state_code, EXTRACT(YEAR FROM admission_date) as year, EXTRACT(MONTH FROM admission_date) as month, IFNULL(count(*), 0) as unknown_revocations
FROM `{project_id}.{base_dataset}.state_incarceration_period`
WHERE admission_reason in ('PAROLE_REVOCATION', 'PROBATION_REVOCATION') and source_supervision_violation_response_id is null
GROUP BY state_code, year, month having year > EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE(), INTERVAL -3 YEAR))) unk_rev
ON newadm.year = unk_rev.year AND newadm.month = unk_rev.month
FULL OUTER JOIN
-- Non-Technical Revocations
(SELECT inc.state_code, EXTRACT(YEAR FROM admission_date) as year, EXTRACT(MONTH FROM admission_date) as month, IFNULL(count(*), 0) as non_technicals
FROM `{project_id}.{base_dataset}.state_incarceration_period` inc
JOIN `{project_id}.{base_dataset}.state_supervision_violation_response` resp
ON inc.source_supervision_violation_response_id = resp.supervision_violation_response_id
JOIN `{project_id}.{base_dataset}.state_supervision_violation` viol 
ON resp.supervision_violation_id = viol.supervision_violation_id
WHERE viol.violation_type != 'TECHNICAL'
GROUP BY state_code, year, month having year > EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE(), INTERVAL -3 YEAR))) non_tech
ON newadm.year = non_tech.year AND newadm.month = non_tech.month
ORDER BY year, month ASC
""".format(
        description=ADMISSIONS_BY_TYPE_BY_MONTH_DESCRIPTION,
        project_id=PROJECT_ID,
        base_dataset=BASE_DATASET,
    )

ADMISSIONS_BY_TYPE_BY_MONTH_VIEW = bqview.BigQueryView(
    view_id=ADMISSIONS_BY_TYPE_BY_MONTH_VIEW_NAME,
    view_query=ADMISSIONS_BY_TYPE_BY_MONTH_QUERY
)

if __name__ == '__main__':
    print(ADMISSIONS_BY_TYPE_BY_MONTH_VIEW.view_id)
    print(ADMISSIONS_BY_TYPE_BY_MONTH_VIEW.view_query)
