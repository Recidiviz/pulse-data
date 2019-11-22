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
"""View for if a person's supervision was terminated successfully.
If a person has multiple supervision periods ending in the same month,
all must have terminated successfully for their supervision termination
to be considered a success.
"""
# pylint: disable=line-too-long, trailing-whitespace
from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET

SUPERVISION_SUCCESS_BY_PERSON = 'supervision_success_by_person'

SUPERVISION_SUCCESS_BY_PERSON_DESCRIPTION = \
    """ For people whose supervision was projected to end in a given month and
    whose supervision have ended by now, whether or not the supervision
    was terminated successfully. 
    """

SUPERVISION_SUCCESS_BY_PERSON_QUERY = \
    """
/*{description}*/

SELECT state_code, person_id, projected_year, projected_month, CASE
WHEN success_count > 0 AND failure_count = 0 THEN TRUE
ELSE FALSE
END AS termination_success
FROM
(SELECT state_code, projected_year, projected_month, person_id,
COUNTIF(termination_reason in ('DISCHARGE', 'EXPIRATION')) as success_count,
COUNTIF(termination_reason in ('ABSCONSION', 'REVOCATION', 'SUSPENSION')) as failure_count
FROM
(SELECT state_code, projected_year, projected_month, person_id, termination_reason
FROM `{project_id}.{views_dataset}.supervision_termination_by_person_and_projected_completion`)
GROUP BY state_code, projected_year, projected_month, person_id)
WHERE success_count > 0 OR failure_count > 0
ORDER BY PERSON_ID

""".format(
        description=SUPERVISION_SUCCESS_BY_PERSON_DESCRIPTION,
        project_id=PROJECT_ID,
        views_dataset=VIEWS_DATASET,
    )

SUPERVISION_SUCCESS_BY_PERSON_VIEW = bqview.BigQueryView(
    view_id=SUPERVISION_SUCCESS_BY_PERSON,
    view_query=SUPERVISION_SUCCESS_BY_PERSON_QUERY
)

if __name__ == '__main__':
    print(SUPERVISION_SUCCESS_BY_PERSON_VIEW.view_id)
    print(SUPERVISION_SUCCESS_BY_PERSON_VIEW.view_query)
