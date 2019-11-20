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
# pylint: disable=trailing-whitespace, line-too-long
"""Incarceration releases de-duped for any incarceration periods that share
state_code, person_id, release_date, release_reason, and facility.

Note: This is used instead of state_incarceration_period for calculations
because of rare data entry errors that duplicate the recording of an
individual's admission to a facility, which results in duplicate recording
of their release."""
from recidiviz.calculator.query import export_config, bqview

from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
BASE_DATASET = export_config.STATE_BASE_TABLES_BQ_DATASET

INCARCERATION_RELEASES_DEDUPED_VIEW_NAME = 'incarceration_releases_deduped'

INCARCERATION_RELEASES_DEDUPED_DESCRIPTION = \
    """ Incarceration releases de-duped for any incarceration periods that
    share state_code, person_id, release_date, release_reason, and facility.
    
    If there are duplicate revocation releases, only one of them will have
    a source supervision violation response attached to it. In these cases we 
    want to be sure to choose the release with the attached response,
    so we order these partitions by the source_supervision_violation_response_id
    in descending order and pick the top result.
    """

INCARCERATION_RELEASES_DEDUPED_QUERY = \
    """
/*{description}*/

SELECT * EXCEPT (rownum) FROM 
(SELECT *, row_number() OVER (PARTITION BY state_code, person_id, release_date, release_reason, facility ORDER BY source_supervision_violation_response_id) AS rownum
FROM `{project_id}.{base_dataset}.state_incarceration_period`
WHERE release_reason IN
('COMMUTED', 'CONDITIONAL_RELEASE', 'SENTENCE_SERVED'))
WHERE rownum = 1

""".format(
        description=INCARCERATION_RELEASES_DEDUPED_DESCRIPTION,
        project_id=PROJECT_ID,
        base_dataset=BASE_DATASET,
    )

INCARCERATION_RELEASES_DEDUPED_VIEW = bqview.BigQueryView(
    view_id=INCARCERATION_RELEASES_DEDUPED_VIEW_NAME,
    view_query=INCARCERATION_RELEASES_DEDUPED_QUERY
)

if __name__ == '__main__':
    print(INCARCERATION_RELEASES_DEDUPED_VIEW.view_id)
    print(INCARCERATION_RELEASES_DEDUPED_VIEW.view_query)
