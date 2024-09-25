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
"""Materialized view for Contact Note Comments in TN to speed up processing"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_CONTACT_COMMENTS_PREPROCESSED_VIEW_NAME = "us_tn_contact_comments_preprocessed"

US_TN_CONTACT_COMMENTS_PREPROCESSED_VIEW_DESCRIPTION = (
    """Materialized view for Contact Note Comments in TN to speed up processing"""
)

US_TN_CONTACT_COMMENTS_PREPROCESSED_QUERY_TEMPLATE = """
    SELECT person_id,
           CAST(CAST(ContactNoteDateTime AS datetime) AS DATE) AS contact_date,
           comment.Comment AS contact_comment,
    FROM `{project_id}.{raw_data_up_to_date_views_dataset}.ContactNoteComment_latest` comment
    INNER JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
       ON comment.OffenderID = pei.external_id
       AND pei.state_code = 'US_TN'
"""

US_TN_CONTACT_COMMENTS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    view_id=US_TN_CONTACT_COMMENTS_PREPROCESSED_VIEW_NAME,
    description=US_TN_CONTACT_COMMENTS_PREPROCESSED_VIEW_DESCRIPTION,
    view_query_template=US_TN_CONTACT_COMMENTS_PREPROCESSED_QUERY_TEMPLATE,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_TN, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_CONTACT_COMMENTS_PREPROCESSED_VIEW_BUILDER.build_and_print()
