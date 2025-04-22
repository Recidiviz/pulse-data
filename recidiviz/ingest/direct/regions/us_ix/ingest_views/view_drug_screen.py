# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Query that generates drug screen information."""
from recidiviz.common.constants.reasonable_dates import (
    STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(#15329): Does this mean medical exception to a positive test?
# ExcusedException,
# TODO(#16038): Revisit after Atlas launch to see if this field is hydrated
# FacTestReasonId = 57 (Admitted Use)

VIEW_QUERY_TEMPLATE = f"""
SELECT
    DrugTestResultId,
    OffenderId,
    CollectionDate,
    TestingMethodDesc,
    -- Overwriting the 1/0 to make the raw text values more comprehensible
    -- See conversation in #17123 for an example of incomprehensible results
    CASE AllNegative
        WHEN '0' THEN 'Not All Negative Results'
        WHEN '1' THEN 'All Negative Results'
    END AS AllNegative,
FROM {{drg_DrugTestResult}}
LEFT JOIN {{drg_TestingMethod}}
    USING (TestingMethodId)
WHERE DATE(CollectionDate)
    BETWEEN '{STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND}' and @update_timestamp 
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix",
    ingest_view_name="drug_screen",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
