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
"""Revocations by officer in the last 60 days."""
# pylint: disable=line-too-long, trailing-whitespace

from recidiviz.calculator.bq import bqview, export_config
from recidiviz.calculator.bq.dashboard.views import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET
BASE_DATASET = export_config.STATE_BASE_TABLES_BQ_DATASET

REVOCATIONS_BY_OFFICER_60_DAYS_VIEW_NAME = 'revocations_by_officer_60_days'

REVOCATIONS_BY_OFFICER_60_DAYS_DESCRIPTION = """
 Revocations by officer in the last 60 days.
 This counts all individuals admitted to prison for a revocation
 of probation or parole, broken down by the agent on the
 source_supervision_violation_response.
 """

# TODO(2231): Join against state_agent instead of temp_officers once state_agent
#  is properly ingested and entity-matched
REVOCATIONS_BY_OFFICER_60_DAYS_QUERY = \
    """
    /*{description}*/
    SELECT state_code, officer_external_id, IFNULL(CAST(off.SITEID AS STRING), 'SITE_UNKNOWN') as site_id, rev.absconsion_count, rev.felony_count, rev.technical_count, rev.unknown_count
    FROM
    (SELECT state_code, officer_external_id, COUNTIF(violation_type = 'ABSCONDED') as absconsion_count, COUNTIF(violation_type = 'FELONY') as felony_count,
    COUNTIF(violation_type = 'TECHNICAL') as technical_count, COUNTIF(violation_type = 'UNKNOWN') as unknown_count
    FROM
    (SELECT sip.state_code, IFNULL(ag.external_id, 'OFFICER_UNKNOWN') as officer_external_id, IFNULL(viol.violation_type, 'UNKNOWN') as violation_type
    FROM
    `{project_id}.{views_dataset}.incarceration_admissions_by_person_60_days` sip
    LEFT JOIN `{project_id}.{base_dataset}.state_supervision_violation_response` resp on resp.supervision_violation_response_id = sip.source_supervision_violation_response_id 
    LEFT JOIN `{project_id}.{base_dataset}.state_supervision_violation_response_decision_agent_association` ref
    ON resp.supervision_violation_response_id = ref.supervision_violation_response_id 
    LEFT JOIN `{project_id}.{base_dataset}.state_agent` ag
    ON ag.agent_id = ref.agent_id
    LEFT JOIN `{project_id}.{base_dataset}.state_supervision_violation` viol
    ON viol.supervision_violation_id = resp.supervision_violation_id
    WHERE sip.admission_reason IN ('PAROLE_REVOCATION', 'PROBATION_REVOCATION')
    GROUP BY sip.state_code, sip.person_id, officer_external_id, violation_type
    ORDER BY officer_external_id)
    GROUP BY state_code, officer_external_id) rev
    LEFT JOIN `{project_id}.{base_dataset}.temp_officers` off
    ON rev.officer_external_id = CAST(off.OFFICER AS STRING)
    ORDER BY site_id asc
    """.format(
        description=REVOCATIONS_BY_OFFICER_60_DAYS_DESCRIPTION,
        project_id=PROJECT_ID,
        views_dataset=VIEWS_DATASET,
        base_dataset=BASE_DATASET,
        )

REVOCATIONS_BY_OFFICER_60_DAYS_VIEW = bqview.BigQueryView(
    view_id=REVOCATIONS_BY_OFFICER_60_DAYS_VIEW_NAME,
    view_query=REVOCATIONS_BY_OFFICER_60_DAYS_QUERY
)

if __name__ == '__main__':
    print(REVOCATIONS_BY_OFFICER_60_DAYS_VIEW.view_id)
    print(REVOCATIONS_BY_OFFICER_60_DAYS_VIEW.view_query)
