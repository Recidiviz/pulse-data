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
"""All persons with their most recent LSIR score."""
# pylint: disable=line-too-long, trailing-whitespace

from recidiviz.calculator.query import export_config, bqview

from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
BASE_DATASET = export_config.STATE_BASE_TABLES_BQ_DATASET

PERSONS_WITH_MOST_RECENT_LSIR_VIEW_NAME = 'persons_with_most_recent_lsir'

PERSONS_WITH_MOST_RECENT_LSIR_DESCRIPTION = \
    """All persons with their most recent LSIR score."""

PERSONS_WITH_MOST_RECENT_LSIR_QUERY = \
    """
/*{description}*/
    SELECT person_id, state_code, assessment_score AS lsir_score FROM
        (SELECT person_id,
                state_code,
                assessment_score,
                assessment_date,
                row_number() OVER (PARTITION BY person_id ORDER BY assessment_date DESC) AS recency_rank
        FROM `{project_id}.{base_dataset}.state_assessment`
        WHERE assessment_type = 'LSIR') ordered_lsi_assessments
    WHERE ordered_lsi_assessments.recency_rank = 1
""".format(
        description=PERSONS_WITH_MOST_RECENT_LSIR_DESCRIPTION,
        project_id=PROJECT_ID,
        base_dataset=BASE_DATASET,
    )

PERSONS_WITH_MOST_RECENT_LSIR_VIEW = bqview.BigQueryView(
    view_id=PERSONS_WITH_MOST_RECENT_LSIR_VIEW_NAME,
    view_query=PERSONS_WITH_MOST_RECENT_LSIR_QUERY
)

if __name__ == '__main__':
    print(PERSONS_WITH_MOST_RECENT_LSIR_VIEW.view_id)
    print(PERSONS_WITH_MOST_RECENT_LSIR_VIEW.view_query)
