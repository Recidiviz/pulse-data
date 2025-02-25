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
"""Links between a StateSupervisionViolationResponse and the associated
agent.
"""
# pylint: disable=trailing-whitespace, line-too-long

from recidiviz.calculator.query import export_config, bqview
from recidiviz.calculator.query.state import view_config

from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
REFERENCE_TABLES_DATASET = view_config.REFERENCE_TABLES_DATASET
BASE_DATASET = export_config.STATE_BASE_TABLES_BQ_DATASET

SSVR_TO_AGENT_ASSOCIATION_VIEW_NAME = \
    'ssvr_to_agent_association'

SSVR_TO_AGENT_ASSOCIATION_DESCRIPTION = \
    """Links between a StateSupervisionViolationResponse and the associated 
    agent.
    """

SSVR_TO_AGENT_ASSOCIATION_QUERY = \
    """
    /*{description}*/
    SELECT 
      * EXCEPT(rownum) 
    FROM (
      SELECT 
        *, 
        row_number() OVER (PARTITION BY supervision_violation_response_id ORDER BY agent_external_id) as rownum 
      FROM (
        SELECT
          state_code, 
          response.supervision_violation_response_id, 
          agent.agent_id, 
          CONCAT(external_id, ': ', off.FNAME, ' ', off.LNAME) as agent_external_id, 
          CAST(off.SITEID as STRING) as district_external_id
        FROM `{project_id}.{base_dataset}.state_supervision_violation_response_decision_agent_association` response
        LEFT JOIN `{project_id}.{base_dataset}.state_agent` agent
          ON agent.agent_id = response.agent_id 
        LEFT JOIN `{project_id}.{reference_tables_dataset}.nd_officers_temp` off
          ON agent.external_id = CAST(off.OFFICER as STRING)
        WHERE agent.external_id IS NOT NULL 
          AND state_code = 'US_ND'
      )
    )
    WHERE rownum = 1

    UNION ALL

    SELECT 
      * EXCEPT(rownum) 
    FROM (
      SELECT 
        *, 
        row_number() OVER (PARTITION BY supervision_violation_response_id ORDER BY agent_external_id) as rownum 
      FROM (
        SELECT 
          state_code, 
          response.supervision_violation_response_id, 
          agent.agent_id,
          REPLACE(CONCAT(agent.external_id, ': ', JSON_EXTRACT(full_name, '$.given_names'), ' ', JSON_EXTRACT(full_name, '$.surname')), '"', '') as agent_external_id,
          off.work_location as district_external_id
        FROM `{project_id}.{base_dataset}.state_supervision_violation_response_decision_agent_association` response
        LEFT JOIN `{project_id}.{base_dataset}.state_agent` agent
          ON agent.agent_id = response.agent_id 
        LEFT JOIN `{project_id}.{reference_tables_dataset}.mo_officers_temp` off
          ON agent.external_id = CAST(off.badge_number as STRING)
        WHERE agent.external_id is not null AND state_code = 'US_MO'
      )
    )
    WHERE rownum = 1
""".format(
        description=SSVR_TO_AGENT_ASSOCIATION_DESCRIPTION,
        project_id=PROJECT_ID,
        base_dataset=BASE_DATASET,
        reference_tables_dataset=REFERENCE_TABLES_DATASET,
    )

SSVR_TO_AGENT_ASSOCIATION_VIEW = bqview.BigQueryView(
    view_id=SSVR_TO_AGENT_ASSOCIATION_VIEW_NAME,
    view_query=SSVR_TO_AGENT_ASSOCIATION_QUERY
)

if __name__ == '__main__':
    print(SSVR_TO_AGENT_ASSOCIATION_VIEW.view_id)
    print(SSVR_TO_AGENT_ASSOCIATION_VIEW.view_query)
