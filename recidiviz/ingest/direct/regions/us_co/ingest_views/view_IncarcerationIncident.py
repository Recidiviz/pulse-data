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
"""Query containing incarceration incident information from the following tables:
    informix_intrac_incidents, informix_intrac_offender, informix_intrac_inc_type,
    informix_intrac_inc_loc, informix_displnry, informix_dispsanc, informix_dispcrim,
    informix_sanction
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH
incident_base AS (
    SELECT DISTINCT
        incident_num,
        inc_type_cd, 
        incident_date,
        fac_cd, 
        location_cd,
        date_created,
        summary,
    FROM {informix_intrac_incidents} 
), id AS (  
    SELECT DISTINCT
        DOCNO,
        INCIDENT_NUM,
    FROM {informix_intrac_offender}
), inc_type AS (
    SELECT DISTINCT
        inc_type_cd,
        inc_type_desc, 
    FROM  {informix_intrac_inc_type}
), loc AS (
    SELECT DISTINCT
        fac_cd, 
        location_cd,
        location_desc
    FROM {informix_intrac_inc_loc}
), outcome AS (
    SELECT DISTINCT
        docno,
        disp_srl,
        dispno,
        incident_num, 
        disp_hrg_dtd,
        d.sanc_strt_dtd, 
        formal_chrg,
        cntraband,
        s.sanct_ldesc, 
        d.days_lost, 
        disp_note, 
        CASE
            WHEN 
                conv_crim_cd LIKE 'II%' THEN 'COPD2: ' || conv_crim_cd
            WHEN 
                    conv_crim_cd LIKE 'I%' AND conv_crim_cd NOT LIKE 'II%'  THEN 'COPD1: ' || conv_crim_cd
        END AS copd_flag
    FROM {informix_displnry}
    LEFT JOIN {informix_dispcrim}
    USING(disp_srl)
    LEFT JOIN {informix_dispsanc} d
    USING (dispcrim_srl)
    LEFT JOIN {informix_sanction} s
    USING (sanct_cd)
), incidents AS (
    SELECT DISTINCT
        docno,
        incident_num,
        inc_type_desc, 
        incident_date,
        incident_base.fac_cd, 
        location_desc,
        summary,
        date_created
    FROM id
    LEFT JOIN incident_base USING (incident_num)
    LEFT JOIN inc_type 
       ON incident_base.inc_type_cd = inc_type.inc_type_cd
    LEFT JOIN loc
  ON incident_base.location_cd = loc.location_cd
      AND incident_base.fac_cd = loc.fac_cd
), inc_and_out AS (
    SELECT DISTINCT
        incidents.docno,
        incidents.incident_num,
        inc_type_desc, 
        incident_date, 
        fac_cd, 
        location_desc, 
        summary, 
        -- outcome --
        sanct_ldesc, 
        sanc_strt_dtd,
        disp_hrg_dtd, 
        date_created, 
        disp_note,
        days_lost,
        copd_flag,
        ROW_NUMBER() OVER (PARTITION BY incidents.docno, incidents.incident_num ORDER BY sanc_strt_dtd, date_created, disp_hrg_dtd, sanct_ldesc, days_lost, disp_note, copd_flag) AS inc_id
    FROM incidents
    LEFT JOIN outcome
        ON incidents.incident_num = outcome.incident_num
        AND incidents.docno = outcome.docno
)
SELECT * FROM inc_and_out
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_co",
    ingest_view_name="IncarcerationIncident",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
