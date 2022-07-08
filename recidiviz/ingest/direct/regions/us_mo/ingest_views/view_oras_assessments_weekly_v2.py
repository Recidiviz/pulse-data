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
"""Query containing ORAS assessments information."""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
    SELECT
        OFFENDER_NAME,
        AGENCY_NAME,
        DATE_OF_BIRTH,
        GENDER,
        ETHNICITY,
        DOC_ID,
        ASSESSMENT_TYPE,
        RISK_LEVEL,
        OVERRIDE_RISK_LEVEL,
        OVERRIDE_RISK_REASON,
        ASSESSMENT_OUTCOME,
        ASSESSMENT_STATUS,
        SCORE,
        DATE_CREATED,
        USER_CREATED,
        RACE,
        BIRTH_DATE,
        CREATED_DATE
    FROM
        {ORAS_WEEKLY_SUMMARY_UPDATE}
    WHERE
        ASSESSMENT_STATUS = 'Complete'
    -- explicitly filter out any test data from UCCI
        AND OFFENDER_NAME NOT LIKE '%Test%'
        AND OFFENDER_NAME NOT LIKE '%test%';
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_mo",
    ingest_view_name="oras_assessments_weekly_v2",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="DOC_ID",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
