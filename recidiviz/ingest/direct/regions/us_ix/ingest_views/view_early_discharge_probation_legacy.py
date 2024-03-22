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
"""Query for probation early discharges using legacy Idaho data"""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- In a small handful of cases, a single early discharge record can be attached to multiple decisions where each decision is under a different sentence
-- In US_ID, we ingested early discharge on the sentence level and so it didn't matter when there were multiple decisions per early discharge record.
-- For US_IX, we won't be ingesting early discharge on the sentence level since early discharge information is on the individual level in Atlas.  Furthermore, there's no way to link sentences in US_ID to sentences in US_IX.
-- Therefore, we'll ingest each early discharge record as an early dicharge entity, so we'll concatenate the decisions here and then deal with conflicting decisions in the mapping.
early_discharge_decisions AS (
  SELECT 
    early_discharge_id,
    STRING_AGG(DISTINCT jurisdiction_decision_description, ',' ORDER BY jurisdiction_decision_description) as jurisdiction_decision_description
  FROM {early_discharge_sent}
  LEFT JOIN 
    {jurisdiction_decision_code}
  USING 
    (jurisdiction_decision_code_id)
  GROUP BY early_discharge_id
)
SELECT 
    e.ofndr_num,
    e.early_discharge_id,
    e.authority,
    (DATE(e.decision_official_dt)) as decision_official_dt,
    f.early_discharge_form_typ_desc,
    d.jurisdiction_decision_description,
    (DATE(e.created_by_dt)) as created_by_dt
FROM {early_discharge} e
LEFT JOIN 
    early_discharge_decisions d
USING
    (early_discharge_id)
LEFT JOIN 
    {early_discharge_form_typ} f
USING 
    (early_discharge_form_typ_id)
WHERE early_discharge_form_typ_desc like '%PROBATION%'
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix",
    ingest_view_name="early_discharge_probation_legacy",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
