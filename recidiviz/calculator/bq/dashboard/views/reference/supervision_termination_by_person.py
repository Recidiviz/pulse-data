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
"""View for when and why a person's supervision was terminated."""
# pylint: disable=line-too-long, trailing-whitespace
from recidiviz.calculator.bq import bqview, export_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
BASE_DATASET = export_config.STATE_BASE_TABLES_BQ_DATASET

SUPERVISION_TERMINATION_BY_PERSON = 'supervision_termination_by_person'

SUPERVISION_TERMINATION_BY_PERSON_DESCRIPTION = \
    """ For people whose supervision was projected to end in a given month and
    whose supervision have ended by now, broken down by whether or not a
    revocation occurred. A "null" decision means that there was no documented
    revocation.
    """

SUPERVISION_TERMINATION_BY_PERSON_QUERY = \
    """
/*{description}*/

SELECT ss.state_code, EXTRACT(YEAR FROM TIMESTAMP(projected_completion_date)) as projected_year, EXTRACT(MONTH FROM TIMESTAMP(projected_completion_date)) as projected_month, ss.person_id, decision, count(*) as count
FROM `{project_id}.{base_dataset}.state_supervision_sentence` ss 
JOIN `{project_id}.{base_dataset}.state_supervision_sentence_supervision_period_association` assoc on ss.supervision_sentence_id = assoc.supervision_sentence_id
JOIN `{project_id}.{base_dataset}.state_supervision_period` sp on sp.supervision_period_id = assoc.supervision_period_id 
FULL OUTER JOIN `{project_id}.{base_dataset}.state_supervision_violation` sv on sp.supervision_period_id = sv.supervision_period_id 
FULL OUTER JOIN `{project_id}.{base_dataset}.state_supervision_violation_response` svr on sv.supervision_violation_id = svr.supervision_violation_id 
WHERE ss.projected_completion_date IS NOT NULL AND termination_date IS NOT NULL 
GROUP BY state_code, projected_year, projected_month, person_id, decision
HAVING projected_year <= EXTRACT(YEAR FROM CURRENT_DATE())
ORDER BY person_id desc
""".format(
        description=SUPERVISION_TERMINATION_BY_PERSON_DESCRIPTION,
        project_id=PROJECT_ID,
        base_dataset=BASE_DATASET,
    )

SUPERVISION_TERMINATION_BY_PERSON_VIEW = bqview.BigQueryView(
    view_id=SUPERVISION_TERMINATION_BY_PERSON,
    view_query=SUPERVISION_TERMINATION_BY_PERSON_QUERY
)

if __name__ == '__main__':
    print(SUPERVISION_TERMINATION_BY_PERSON_VIEW.view_id)
    print(SUPERVISION_TERMINATION_BY_PERSON_VIEW.view_query)
