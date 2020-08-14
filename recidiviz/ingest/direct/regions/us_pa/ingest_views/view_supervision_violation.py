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
"""Query containing supervision sentence information."""

from recidiviz.ingest.direct.controllers.direct_ingest_big_query_view_types import \
    DirectIngestPreProcessedIngestViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT
  parole_number,
  parole_count_id,
  set_id,
  EXTRACT(DATE FROM MIN(parsed_violation_timestamp)) as violation_date,
  TO_JSON_STRING(ARRAY_AGG(STRUCT(sequence_id, violation_code))) AS violation_types,
FROM (
  SELECT
    ParoleNumber as parole_number,
    ParoleCountID as parole_count_id,
    SetID as set_id,
    SequenceID as sequence_id,
    SanctionCode as violation_code,
    PARSE_TIMESTAMP("%m/%d/%Y %T", ViolationDate) as parsed_violation_timestamp,
  FROM {dbo_SanctionTracking}
  WHERE Type = 'V'

  UNION ALL

  SELECT
    ParoleNumber as parole_number,
    ParoleCountID as parole_count_id,
    SetID as set_id,
    SequenceID as sequence_id,
    SanctionCode as violation_code,
    PARSE_TIMESTAMP("%m/%d/%Y %T", ViolationDate) as parsed_violation_timestamp,
  FROM {dbo_Hist_SanctionTracking}
  WHERE Type = 'V'
)
GROUP BY parole_number, parole_count_id, set_id
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region='us_pa',
    ingest_view_name='supervision_violation',
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols='parole_number ASC, parole_count_id ASC, set_id ASC'
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
