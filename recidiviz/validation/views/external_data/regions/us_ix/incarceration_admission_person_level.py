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
"""A view detailing the incarceration admissions at the person level for Idaho in 2020."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

VIEW_QUERY_TEMPLATE = """
WITH admissions AS (
    -- Entries in `idoc_prison_admissions_releases` represent a population flow from
    -- `stat_strt_typ` to `stat_rls_typ` on `stat_rls_dtd`. It also includes information
    -- about the flow into `stat_strt_typ` on `stat_strt_dtd` which is not always
    -- represented as a separate row, so we unnest the rows to make sure we include both
    -- and then deduplicate at the end.
    SELECT
        CAST(DOCNO AS string) as person_external_id,
        CASE direction
            WHEN 'START' then stat_strt_typ
            WHEN 'END' then stat_rls_typ
        END as to_status,
        EXTRACT(DATE FROM case direction
            WHEN 'START' then PARSE_DATETIME("%m/%d/%y %H:%M", stat_strt_dtd)
            WHEN 'END' then PARSE_DATETIME("%m/%d/%y %H:%M", stat_rls_dtd)
        END) as movement_date
    FROM `{project_id}.{us_ix_validation_oneoff_dataset}.idoc_prison_admissions_releases`, UNNEST(['START', 'END']) direction
)
SELECT
    'US_IX' AS state_code,
    person_external_id,
    'US_IX_DOC' AS external_id_type,
    movement_date as admission_date,
FROM admissions
WHERE EXTRACT(YEAR from movement_date) = 2020
AND to_status in ('PV', 'RJ', 'TM')
GROUP BY person_external_id, admission_date
"""

US_IX_INCARCERATION_ADMISSION_PERSON_LEVEL_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.validation_dataset_for_state(StateCode.US_IX),
    view_id="incarceration_admission_person_level",
    description="A view detailing the incarceration admissions at the person level for Idaho in 2020.",
    view_query_template=VIEW_QUERY_TEMPLATE,
    should_materialize=True,
    us_ix_validation_oneoff_dataset=dataset_config.validation_oneoff_dataset_for_state(
        StateCode.US_IX
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_IX_INCARCERATION_ADMISSION_PERSON_LEVEL_VIEW_BUILDER.build_and_print()
