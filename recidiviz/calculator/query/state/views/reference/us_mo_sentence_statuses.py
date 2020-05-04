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
from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.calculator.query import export_config
from recidiviz.calculator.query.state import view_config

from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
REFERENCE_TABLES_DATASET = view_config.REFERENCE_TABLES_DATASET
BASE_DATASET = export_config.STATE_BASE_TABLES_BQ_DATASET

US_MO_SENTENCE_STATUSES_VIEW_NAME = \
    'us_mo_sentence_statuses'

US_MO_SENTENCE_STATUSES_DESCRIPTION = \
    """Provides time-based sentence status information for US_MO.
    """

US_MO_SENTENCE_STATUSES_QUERY = \
    """
    /*{description}*/
    SELECT
        COALESCE(incarceration_sentences.person_id, supervision_sentences.person_id) AS person_id,
        all_statuses.*
    FROM (
        SELECT
          CONCAT(DOC, '-', CYC, '-', SEO) as sentence_external_id,
          CONCAT(DOC, '-', CYC, '-', SEO, '-', SSO) as sentence_status_external_id,
          SCD AS status_code,
          SY AS status_date,
          SDE AS status_description
        FROM
          `{project_id}.{reference_tables_dataset}.us_mo_tak026_sentence_status`
        LEFT OUTER JOIN
          `{project_id}.{reference_tables_dataset}.us_mo_tak025_sentence_status_xref`
        USING (DOC, CYC, SSO)
        LEFT OUTER JOIN
          `{project_id}.{reference_tables_dataset}.us_mo_tak146_status_code_descriptions`
        USING (SCD)
        WHERE
          DOC IS NOT NULL AND
          CYC IS NOT NULL AND
          SEO IS NOT NULL AND
          SSO IS NOT NULL
    ) all_statuses
    LEFT OUTER JOIN
      `{project_id}.{base_dataset}.state_supervision_sentence` supervision_sentences
    ON
        supervision_sentences.external_id = sentence_external_id
    LEFT OUTER JOIN
      `{project_id}.{base_dataset}.state_incarceration_sentence` incarceration_sentences
    ON
        incarceration_sentences.external_id = sentence_external_id
    WHERE (incarceration_sentences.person_id IS NOT NULL OR supervision_sentences.person_id IS NOT NULL);
""".format(
        description=US_MO_SENTENCE_STATUSES_DESCRIPTION,
        project_id=PROJECT_ID,
        base_dataset=BASE_DATASET,
        reference_tables_dataset=REFERENCE_TABLES_DATASET,
    )

US_MO_SENTENCE_STATUSES_VIEW = BigQueryView(
    view_id=US_MO_SENTENCE_STATUSES_VIEW_NAME,
    view_query=US_MO_SENTENCE_STATUSES_QUERY
)

if __name__ == '__main__':
    print(US_MO_SENTENCE_STATUSES_VIEW.view_id)
    print(US_MO_SENTENCE_STATUSES_VIEW.view_query)
