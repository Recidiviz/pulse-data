# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Query that collects information about sentences and charges in UT.

Sentences in Utah can be suspended upon imposition, meaning that a person is sentenced
to a term of probation that, if they serve successfully, will take the place of their prison sentence.
If they are revoked from that term of probation, they must serve the entirety of their original
prison sentence. These types of sentences are ingested as probation sentences.

A majority of sentences are directly to probation; the next most prevalent sentence type is 
the suspended sentence described above. Sentences directly to prison follow suspended sentences
in prevalence."""

from recidiviz.ingest.direct.regions.us_ut.ingest_views.common_sentencing_views_and_utils import (
    VALID_PEOPLE_AND_SENTENCES,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
WITH 
-- Collect identifiers for sentences to ingest.
base_sentences AS ({VALID_PEOPLE_AND_SENTENCES})

-- Join details about each charge to the base sentence information.
SELECT DISTINCT
  crt.ofndr_num,
  crt.intr_case_num,
  sentence_type,
  ofnse.ofnse_id,
  ofnse_viol_cd AS statute,
  oc.ofnse_desc AS charge_description,
  ofnse.ofnse_svrty_cd AS offense_severity_code,
  ofnse.atmpt_flg AS attempt_flag,
  CAST(ofnse_dt AS DATETIME) AS offense_date,
  cd.crime_degree_desc AS charge_classification_type_raw_text,
  CAST(crt.sent_dt AS DATETIME) AS sent_dt,
  cl.crt_loc_desc AS sentencing_court,
FROM 
  base_sentences
JOIN 
  {{crt_case}} crt
USING
  (intr_case_num)
JOIN 
  {{rfrd_ofnse}} ofnse
USING
  (intr_case_num)
LEFT JOIN 
  {{ofnse_cd}} oc
USING
  (ofnse_viol_cd)
LEFT JOIN 
  {{crime_degree_cd}} cd
ON
  (cd.crime_degree_cd = ofnse.crime_degree_cd)
LEFT JOIN 
  {{crt_loc}} cl
USING
  (crt_loc_cd, crt_typ_cd)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ut",
    ingest_view_name="sentence",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
