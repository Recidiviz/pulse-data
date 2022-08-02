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
"""Overdue for Discharge Eligibility Criteria
    - Past projected end date
    - Case note exclusions (none in past year) (https://github.com/Recidiviz/recidiviz-research/pull/527)
        - violation = [('.*violat.*', 'regex'), 'pv', 'rov', 'report of violation']
        - sanction = [('sanction', 'partial_ratio')]
        - extend = [('extend', 'partial_ratio')]
        - abscond = [('abscond', 'partial_ratio'), 'absconsion']
        - custody = ['in custody', ('arrest', 'partial_ratio')]
        - agents_warning = ['aw', 'agents warrant', 'cw', 'bw', 'commission warrant', 'bench warrant', 'warrant']
        - revoke = [('.*revoke.*', 'regex'), ('.*revoc.*', 'regex'), 'rx']
        - new_investigation = ['psi', 'file_review', 'activation']
        - other = ['critical', 'detainer', 'positive', 'admission', ('ilet.*nco | nco.*ilet.*', 'regex')]

"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.pipeline.supplemental.dataset_config import (
    SUPPLEMENTAL_DATA_DATASET,
)
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_tables_dataset_for_region,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ID_DAY_0_OVERDUE_DISCHARGE_VIEW_NAME = "us_id_day_0_overdue_discharge"

US_ID_DAY_0_OVERDUE_DISCHARGE_VIEW_DESCRIPTION = """This view was created to simplify logic identified during the Day 0 trip to Idaho in September 2021"""
US_ID_DAY_0_OVERDUE_DISCHARGE_QUERY_TEMPLATE = """
WITH preliminary_eligibility AS (
    SELECT
        person_id,
        person_external_id,
        district_name,
        supervising_officer_external_id,
        projected_end_date,
        date_of_supervision,
        CONCAT(first_name, ' ', last_name) AS full_name,
    FROM `{project_id}.{analyst_dataset}.projected_discharges_materialized`
    WHERE state_code = 'US_ID'
        AND date_of_supervision >= projected_end_date
),
case_notes AS (
    SELECT
        CAST(person_external_id AS STRING) AS person_external_id,
        CAST(MAX(GREATEST(
            violation,
            sanction,
            extend,
            absconsion,
            in_custody,
            agents_warning,
            revocation,
            new_investigation,
            other
        )) AS INT64) AS case_notes_flag
    FROM `{project_id}.{supplemental_dataset}.us_id_case_note_matched_entities`
    WHERE SAFE_CAST(create_dt AS date) >= DATE_SUB(CURRENT_DATE("US/Mountain"), INTERVAL 1 YEAR)
    GROUP BY 1
),
sessions AS (
    SELECT
        person_id,
        compartment_level_2,
    FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized`
    WHERE state_code = 'US_ID'
        AND CURRENT_DATE("US/Mountain") BETWEEN start_date AND COALESCE(end_date, '9999-01-01')
),
summary_ofndr_ftrd_dt_recidiviz_latest AS (
  SELECT
    ofndr_num,
    sent_ftrd_dt,
    life_death_flag,
  FROM `{project_id}.{us_id_raw_data_dataset}.summary_ofndr_ftrd_dt_recidiviz`
  WHERE TRUE
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ofndr_num
    ORDER BY update_datetime DESC
  ) = 1
),
cis_ftrd AS (
  SELECT
    ofndr_num AS person_external_id,
    CAST(sent_ftrd_dt AS DATE) AS cis_ftrd,
    CASE
      WHEN life_death_flag IS NOT NULL THEN 1
      WHEN EXTRACT(YEAR FROM CAST(sent_ftrd_dt AS DATETIME)) = 9999 THEN 1
      ELSE 0
    END AS cis_life,
  FROM summary_ofndr_ftrd_dt_recidiviz_latest
)

SELECT
    pe.*,
FROM preliminary_eligibility pe
LEFT JOIN case_notes
    USING (person_external_id)
LEFT JOIN sessions
    USING (person_id)
LEFT JOIN cis_ftrd
    USING (person_external_id)
WHERE case_notes_flag = 0
    -- CIS FTRD Filters
    AND (cis_ftrd.cis_ftrd IS NULL OR date_of_supervision >= cis_ftrd.cis_ftrd)
    -- CIS Life Sentence Filter
    AND COALESCE(cis_ftrd.cis_life, 0) = 0
ORDER BY projected_end_date
     """

US_ID_DAY_0_OVERDUE_DISCHARGE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    view_id=US_ID_DAY_0_OVERDUE_DISCHARGE_VIEW_NAME,
    view_query_template=US_ID_DAY_0_OVERDUE_DISCHARGE_QUERY_TEMPLATE,
    description=US_ID_DAY_0_OVERDUE_DISCHARGE_VIEW_DESCRIPTION,
    supplemental_dataset=SUPPLEMENTAL_DATA_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    us_id_raw_data_dataset=raw_tables_dataset_for_region("us_id"),
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ID_DAY_0_OVERDUE_DISCHARGE_VIEW_BUILDER.build_and_print()
