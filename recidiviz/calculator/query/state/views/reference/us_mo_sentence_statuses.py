# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""BQ View containing US_MO state statuses from TAK026"""
from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.state import view_config

from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
REFERENCE_TABLES_DATASET = view_config.REFERENCE_TABLES_DATASET

US_MO_SENTENCE_STATUSES_VIEW_NAME = \
    'us_mo_sentence_statuses'

US_MO_SENTENCE_STATUSES_DESCRIPTION = \
    """Provides time-based sentence status information for US_MO.
    """

US_MO_SENTENCE_STATUSES_QUERY = \
    """
    /*{description}*/
    SELECT
      CONCAT(DOC, '-', CYC, '-', SEO) as sentence_external_id,
      CONCAT(DOC, '-', CYC, '-', SEO, '-', SSO) as sentence_status_external_id,
      SCD AS status_code,
      SY AS status_date,
      SDE AS status_description
    FROM
      `{project_id}.{reference_tables_dataset}.us_mo_tak026_tak025_sentence_statuses`
    WHERE
      DOC IS NOT NULL AND
      CYC IS NOT NULL AND
      SEO IS NOT NULL AND
      SSO IS NOT NULL
""".format(
        description=US_MO_SENTENCE_STATUSES_DESCRIPTION,
        project_id=PROJECT_ID,
        reference_tables_dataset=REFERENCE_TABLES_DATASET,
    )

US_MO_SENTENCE_STATUSES_VIEW = bqview.BigQueryView(
    view_id=US_MO_SENTENCE_STATUSES_VIEW_NAME,
    view_query=US_MO_SENTENCE_STATUSES_QUERY
)

if __name__ == '__main__':
    print(US_MO_SENTENCE_STATUSES_VIEW.view_id)
    print(US_MO_SENTENCE_STATUSES_VIEW.view_query)
