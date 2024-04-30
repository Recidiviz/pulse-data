# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Validation view that returns one row for every supervision_level=IN_CUSTODY
supervision period whose time span is not fully covered by incarceration periods. When
this validation fails, it indicates that incarceration periods are missing information
about time we know someone spent in custody.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

IN_CUSTODY_SPS_HAVE_ASSOCIATED_IP_VIEW_NAME = "in_custody_sps_have_associated_ip"

IN_CUSTODY_SPS_HAVE_ASSOCIATED_IP_DESCRIPTION = """
Validation view that returns one row for every supervision_level=IN_CUSTODY 
supervision period whose time span is not fully covered by incarceration periods. When
this validation fails, it indicates that incarceration periods are missing information
about time we know someone spent in custody.
"""

IN_CUSTODY_SPS_HAVE_ASSOCIATED_IP_QUERY_TEMPLATE = f"""
WITH 
ips AS (
  SELECT state_code, person_id, admission_date AS start_date, release_date AS end_date
  FROM `{{project_id}}.normalized_state.state_incarceration_period`
),
ip_spans AS ({aggregate_adjacent_spans("ips")}),
in_custody_sps AS (
  SELECT *
  FROM `{{project_id}}.normalized_state.state_supervision_period`
  WHERE supervision_level = 'IN_CUSTODY'
)
SELECT sp.state_code AS region_code, sp.*
FROM in_custody_sps sp
LEFT OUTER JOIN
  ip_spans ip
ON 
  ip.person_id = sp.person_id
  AND ip.start_date <= sp.start_date 
  AND {nonnull_end_date_clause('ip.end_date')} >= {nonnull_end_date_clause('sp.termination_date')}
WHERE ip.session_id IS NULL
"""

IN_CUSTODY_SPS_HAVE_ASSOCIATED_IP_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=IN_CUSTODY_SPS_HAVE_ASSOCIATED_IP_VIEW_NAME,
    view_query_template=IN_CUSTODY_SPS_HAVE_ASSOCIATED_IP_QUERY_TEMPLATE,
    description=IN_CUSTODY_SPS_HAVE_ASSOCIATED_IP_DESCRIPTION,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        IN_CUSTODY_SPS_HAVE_ASSOCIATED_IP_VIEW_BUILDER.build_and_print()
