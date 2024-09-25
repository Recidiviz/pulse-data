# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
""" This view has one record per term and person. It contains parole related information for residents in
IX. This will be used to create criteria queries for the CRC and XCRC workflows."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_IX_PAROLE_DATES_SPANS_PREPROCESSING_VIEW_NAME = (
    "us_ix_parole_dates_spans_preprocessing"
)

US_IX_PAROLE_DATES_SPANS_PREPROCESSING_VIEW_DESCRIPTION = """This view has one record per term and person. It contains parole related information for residents in
IX. This will be used to create criteria queries for the CRC and XCRC workflows."""

US_IX_PAROLE_DATES_SPANS_PREPROCESSING_QUERY_TEMPLATE = """
SELECT
  pei.person_id,
  pei.state_code,
  OffenderId,
  TermId,
  SAFE_CAST(LEFT(TermStartDate, 10) AS DATE) AS start_date,
  SAFE_CAST(LEFT(ReleaseDate, 10) AS DATE) AS end_date,
  SAFE_CAST(LEFT(TentativeParoleDate, 10) AS DATE) AS tentative_parole_date,
  SAFE_CAST(LEFT(NextHearingDate, 10) AS DATE) AS next_parole_hearing_date,
  SAFE_CAST(LEFT(InitialParoleHearingDate, 10) AS DATE) AS initial_parole_hearing_date,
FROM
  `{project_id}.{us_ix_raw_data_up_to_date_dataset}.scl_Term_latest`
INNER JOIN
  `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
ON
  pei.external_id = OffenderId
  AND pei.state_code = 'US_IX'
  AND pei.id_type = 'US_IX_DOC'
"""

US_IX_PAROLE_DATES_SPANS_PREPROCESSING_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_IX_PAROLE_DATES_SPANS_PREPROCESSING_VIEW_NAME,
    description=US_IX_PAROLE_DATES_SPANS_PREPROCESSING_VIEW_DESCRIPTION,
    view_query_template=US_IX_PAROLE_DATES_SPANS_PREPROCESSING_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    should_materialize=True,
    us_ix_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_IX, instance=DirectIngestInstance.PRIMARY
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_IX_PAROLE_DATES_SPANS_PREPROCESSING_VIEW_BUILDER.build_and_print()
