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
"""Admissions by type in the last 60 days"""
# pylint: disable=line-too-long, trailing-whitespace
from recidiviz.calculator.bq import bqview, export_config
from recidiviz.calculator.bq.dashboard.views import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
BASE_DATASET = export_config.STATE_BASE_TABLES_BQ_DATASET
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET

ADMISSIONS_BY_TYPE_60_DAYS_VIEW_NAME = 'admissions_by_type_60_days'

ADMISSIONS_BY_TYPE_60_DAYS_DESCRIPTION = """ Admissions by type in the last 60 days """

ADMISSIONS_BY_TYPE_60_DAYS_QUERY = \
    """
/*{description}*/

SELECT state_code, admission_type, count(*) as admission_count
FROM
-- Technical Revocations
(SELECT inc.state_code, violation_type as admission_type
FROM `{project_id}.{views_dataset}.incarceration_admissions_by_person_60_days` inc
JOIN `{project_id}.{base_dataset}.state_supervision_violation_response` resp
ON inc.source_supervision_violation_response_id = resp.supervision_violation_response_id
JOIN `{project_id}.{base_dataset}.state_supervision_violation` viol 
ON resp.supervision_violation_id = viol.supervision_violation_id
WHERE viol.violation_type = 'TECHNICAL') 
GROUP BY state_code, admission_type
UNION ALL
-- New Admissions
(SELECT state_code, admission_reason as admission_type, count(*) as revocation_count
FROM `{project_id}.{views_dataset}.incarceration_admissions_by_person_60_days`
WHERE admission_reason = 'NEW_ADMISSION'
GROUP BY state_code, admission_type)
UNION ALL
-- Unknown Revocations
(SELECT state_code, 'UNKNOWN_REVOCATION' as admission_type, count(*) as revocation_count
FROM `{project_id}.{views_dataset}.incarceration_admissions_by_person_60_days`
WHERE admission_reason in ('PAROLE_REVOCATION', 'PROBATION_REVOCATION') and source_supervision_violation_response_id is null
GROUP BY state_code, admission_type)
UNION ALL
-- Non-Technical Revocations
(SELECT inc.state_code, 'NON_TECHNICAL' as admission_type, count(*) as revocation_count
FROM `{project_id}.{views_dataset}.incarceration_admissions_by_person_60_days` inc
JOIN `{project_id}.{base_dataset}.state_supervision_violation_response` resp
ON inc.source_supervision_violation_response_id = resp.supervision_violation_response_id
JOIN `{project_id}.{base_dataset}.state_supervision_violation` viol 
ON resp.supervision_violation_id = viol.supervision_violation_id
WHERE viol.violation_type != 'TECHNICAL'
GROUP BY state_code, admission_type)
""".format(
        description=ADMISSIONS_BY_TYPE_60_DAYS_DESCRIPTION,
        project_id=PROJECT_ID,
        views_dataset=VIEWS_DATASET,
        base_dataset=BASE_DATASET
    )

ADMISSIONS_BY_TYPE_60_DAYS_VIEW = bqview.BigQueryView(
    view_id=ADMISSIONS_BY_TYPE_60_DAYS_VIEW_NAME,
    view_query=ADMISSIONS_BY_TYPE_60_DAYS_QUERY
)

if __name__ == '__main__':
    print(ADMISSIONS_BY_TYPE_60_DAYS_VIEW.view_id)
    print(ADMISSIONS_BY_TYPE_60_DAYS_VIEW.view_query)
