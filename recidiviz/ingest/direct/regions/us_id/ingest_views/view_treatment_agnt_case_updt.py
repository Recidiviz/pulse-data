# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Query for case notes, from which treatment program assignments will be parsed."""
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
    SELECT
        agnt_case_updt_id,
        ofndr_num,
        create_by_usr_id,
        create_dt,
        agnt_note_title
    FROM {agnt_case_updt}
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_id",
    ingest_view_name="treatment_agnt_case_updt",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="ofndr_num, create_dt",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
