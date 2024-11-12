# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License AS published by
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
"""Query containing incarceration incident information from the ADH_OFFENDER_RESTRICTION table"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
    SELECT 
        res.offender_restriction_id,
        res.offender_id,
        loc.name as location_name,
        ref.description AS restriction_description,
        DATE(begin_date) AS begin_date,
        DATE(end_date) AS end_date
    FROM {ADH_OFFENDER_RESTRICTION} res
    LEFT JOIN {ADH_LOCATION} loc USING(location_id)
    LEFT JOIN {ADH_REFERENCE_CODE} ref
        ON res.offender_restriction_code_id = ref.reference_code_id
    WHERE 
        ref.description NOT LIKE 'Medical Restriction%' AND 
        ref.description NOT LIKE 'Quarantine%' AND
        ref.description NOT LIKE 'Sanction Break%' AND
        ref.description NOT LIKE 'Information%' AND
        ref.description NOT LIKE 'SSRTP%';
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mi",
    ingest_view_name="incarceration_incident_restrictions",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
