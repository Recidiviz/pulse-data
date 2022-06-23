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
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MO_SENTENCE_STATUSES_VIEW_NAME = "us_mo_sentence_statuses"

US_MO_SENTENCE_STATUSES_DESCRIPTION = """Provides time-based sentence status information for US_MO.
    """

US_MO_SENTENCE_STATUSES_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
        COALESCE(incarceration_sentences.person_id, supervision_sentences.person_id) AS person_id,
        'US_MO' AS state_code,
        all_statuses.*
    FROM (
        SELECT
          CONCAT(BW_DOC, '-', BW_CYC, '-', BV_SEO) as sentence_external_id,
          CONCAT(BW_DOC, '-', BW_CYC, '-', BV_SEO, '-', BW_SSO) as sentence_status_external_id,
          BW_SCD AS status_code,
          BW_SY AS status_date,
          FH_SDE AS status_description
        FROM
          `{project_id}.{us_mo_raw_data_up_to_date_dataset}.LBAKRDTA_TAK026_latest`
        LEFT OUTER JOIN
          `{project_id}.{us_mo_raw_data_up_to_date_dataset}.LBAKRDTA_TAK025_latest`
        ON
            BW_DOC = BV_DOC
            AND BW_CYC = BV_CYC
            AND BW_SSO = BV_SSO
        LEFT OUTER JOIN
          `{project_id}.{us_mo_raw_data_up_to_date_dataset}.LBAKRCOD_TAK146_latest`
        ON
            BW_SCD = FH_SCD
        WHERE
          BW_DOC IS NOT NULL AND
          BW_CYC IS NOT NULL AND
          BV_SEO IS NOT NULL AND
          BW_SSO IS NOT NULL
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
"""

US_MO_SENTENCE_STATUSES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=US_MO_SENTENCE_STATUSES_VIEW_NAME,
    view_query_template=US_MO_SENTENCE_STATUSES_QUERY_TEMPLATE,
    description=US_MO_SENTENCE_STATUSES_DESCRIPTION,
    base_dataset=dataset_config.STATE_BASE_DATASET,
    us_mo_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        StateCode.US_MO.value
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MO_SENTENCE_STATUSES_VIEW_BUILDER.build_and_print()
