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
"""Revocations by violation type by month."""
# pylint: disable=line-too-long, trailing-whitespace
from recidiviz.calculator.bq import bqview, export_config
from recidiviz.calculator.bq.dashboard.views import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
BASE_DATASET = export_config.STATE_BASE_TABLES_BQ_DATASET
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET

REVOCATIONS_BY_VIOLATION_TYPE_BY_MONTH_VIEW_NAME = \
    'revocations_by_violation_type_by_month'

REVOCATIONS_BY_VIOLATION_TYPE_BY_MONTH_DESCRIPTION = \
    """ Revocations by violation type by month """

REVOCATIONS_BY_VIOLATION_TYPE_BY_MONTH_QUERY = \
    """
    /*{description}*/
    
    SELECT IFNULL(absc.state_code, IFNULL(fel.state_code, IFNULL(tech.state_code, unk.state_code))) as state_code,
    IFNULL(absc.year, IFNULL(fel.year, IFNULL(tech.year, unk.year))) as year,
    IFNULL(absc.month, IFNULL(fel.month, IFNULL(tech.month, unk.month))) as month,
    IFNULL(absconsion_count, 0) as absconsion_count, IFNULL(felony_count, 0) as felony_count, IFNULL(technical_count, 0) as technical_count, IFNULL(unknown_count, 0) as unknown_count
    FROM
    -- Absconsions
    ((SELECT sip.state_code, EXTRACT(YEAR FROM admission_date) as year, EXTRACT(MONTH FROM admission_date) as month, count(*) as absconsion_count FROM `{project_id}.{views_dataset}.incarceration_admissions_by_person_and_month` sip
    join `{project_id}.{base_dataset}.state_supervision_violation_response` resp on resp.supervision_violation_response_id = sip.source_supervision_violation_response_id 
    join `{project_id}.{base_dataset}.state_supervision_violation` viol on viol.supervision_violation_id = resp.supervision_violation_id 
    WHERE viol.violation_type = 'ABSCONDED' AND source_supervision_violation_response_id is not null
    GROUP BY state_code, year, month, violation_type having year > EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE(), INTERVAL -3 YEAR))) absc
    FULL OUTER JOIN
    -- Felonies (New offenses)
    (SELECT sip.state_code, EXTRACT(YEAR FROM admission_date) as year, EXTRACT(MONTH FROM admission_date) as month, count(*) as felony_count FROM `{project_id}.{views_dataset}.incarceration_admissions_by_person_and_month` sip
    join `{project_id}.{base_dataset}.state_supervision_violation_response` resp on resp.supervision_violation_response_id = sip.source_supervision_violation_response_id 
    join `{project_id}.{base_dataset}.state_supervision_violation` viol on viol.supervision_violation_id = resp.supervision_violation_id 
    WHERE viol.violation_type = 'FELONY' AND source_supervision_violation_response_id is not null
    GROUP BY state_code, year, month, violation_type having year > EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE(), INTERVAL -3 YEAR))) fel
    ON absc.state_code = fel.state_code AND absc.year = fel.year AND absc.month = fel.month
    FULL OUTER JOIN
    -- Technicals
    (SELECT sip.state_code, EXTRACT(YEAR FROM admission_date) as year, EXTRACT(MONTH FROM admission_date) as month, count(*) as technical_count FROM `{project_id}.{views_dataset}.incarceration_admissions_by_person_and_month` sip
    join `{project_id}.{base_dataset}.state_supervision_violation_response` resp on resp.supervision_violation_response_id = sip.source_supervision_violation_response_id 
    join `{project_id}.{base_dataset}.state_supervision_violation` viol on viol.supervision_violation_id = resp.supervision_violation_id 
    WHERE viol.violation_type = 'TECHNICAL' AND source_supervision_violation_response_id is not null
    GROUP BY state_code, year, month, violation_type having year > EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE(), INTERVAL -3 YEAR))) tech
    ON absc.state_code = tech.state_code AND absc.year = tech.year AND absc.month = tech.month
    FULL OUTER JOIN
    -- Unknown violation types
    (SELECT state_code, year, month, count(*) as unknown_count
    FROM
    ((SELECT state_code, EXTRACT(YEAR FROM admission_date) as year, EXTRACT(MONTH FROM admission_date) as month FROM
    `{project_id}.{views_dataset}.incarceration_admissions_by_person_and_month`
    WHERE admission_reason in ('PROBATION_REVOCATION', 'PAROLE_REVOCATION') and source_supervision_violation_response_id is null) 
    UNION ALL
    (SELECT sip.state_code, EXTRACT(YEAR FROM admission_date) as year, EXTRACT(MONTH FROM admission_date) as month
    FROM `{project_id}.{views_dataset}.incarceration_admissions_by_person_and_month` sip
    join `{project_id}.{base_dataset}.state_supervision_violation_response` resp on resp.supervision_violation_response_id = sip.source_supervision_violation_response_id 
    join `{project_id}.{base_dataset}.state_supervision_violation` viol on viol.supervision_violation_id = resp.supervision_violation_id 
    WHERE viol.violation_type IS NULL)) 
    GROUP BY state_code, year, month having year > EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE(), INTERVAL -3 YEAR))) unk
    ON absc.state_code = unk.state_code AND absc.year = unk.year AND absc.month = unk.month)
    ORDER BY year, month ASC
    """.format(
        description=REVOCATIONS_BY_VIOLATION_TYPE_BY_MONTH_DESCRIPTION,
        project_id=PROJECT_ID,
        base_dataset=BASE_DATASET,
        views_dataset=VIEWS_DATASET,
        )

REVOCATIONS_BY_VIOLATION_TYPE_BY_MONTH_VIEW = bqview.BigQueryView(
    view_id=REVOCATIONS_BY_VIOLATION_TYPE_BY_MONTH_VIEW_NAME,
    view_query=REVOCATIONS_BY_VIOLATION_TYPE_BY_MONTH_QUERY
)

if __name__ == '__main__':
    print(REVOCATIONS_BY_VIOLATION_TYPE_BY_MONTH_VIEW.view_id)
    print(REVOCATIONS_BY_VIOLATION_TYPE_BY_MONTH_VIEW.view_query)
