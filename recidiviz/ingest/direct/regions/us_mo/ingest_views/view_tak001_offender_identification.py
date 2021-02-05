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
"""Query containing offender identification information."""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import \
    DirectIngestPreProcessedIngestViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
    SELECT 
        EK_DOC,EK_CYC,EK_ALN,EK_AFN,EK_AMI,EK_AGS,EK_SID,EK_FBI,EK_OLN,EK_RAC,EK_ETH,EK_SEX,DOC_ID_DOB,DOB
    FROM
        {LBAKRDTA_TAK001} offender_identification_ek
    LEFT OUTER JOIN
        {LBAKRDTA_VAK003} dob_view
    ON EK_DOC = dob_view.DOC_ID_DOB
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region='us_mo',
    ingest_view_name='tak001_offender_identification',
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols='EK_DOC',
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
