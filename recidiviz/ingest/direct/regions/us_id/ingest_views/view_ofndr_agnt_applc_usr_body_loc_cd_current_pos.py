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
"""Query that associates a person with their current parole/probation officer."""

from recidiviz.ingest.direct.controllers.direct_ingest_big_query_view_types import \
    DirectIngestPreProcessedIngestViewBuilder
from recidiviz.utils.environment import GAE_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(3366): Integrate PO assignments into supervision query once we have a loss-less table with POs and their
#  assignments through history.
VIEW_QUERY_TEMPLATE = """SELECT
      ofndr_num, 
      agnt_id,
      usr_id,
      name,
      a.agcy_id AS ofndr_agent_agcy,
      u.agcy_id AS applc_usr_agcy,
      agnt_strt_dt,
      end_dt,
      usr_typ_cd,
      # updt_usr_id,
      # updt_dt,
      lan_id,
      st_id_num,
      body_loc_cd,
      body_loc_desc,
      loc_typ_cd,
      body_loc_cd_id
    FROM 
      {ofndr_agnt} a
    LEFT JOIN 
      {applc_usr} u
    ON 
      (agnt_id = usr_id)
    LEFT JOIN 
      {body_loc_cd}
    USING
      (body_loc_cd)
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region='us_id',
    ingest_view_name='ofndr_agnt_applc_usr_body_loc_cd_current_pos',
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == '__main__':
    with local_project_id_override(GAE_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
