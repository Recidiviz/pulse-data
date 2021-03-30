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
"""Creates the view builder and view for listing all clients."""

from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state.dataset_config import (
    CASE_TRIAGE_DATASET,
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
)
from recidiviz.case_triage.views.dataset_config import VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CLIENT_LIST_QUERY_TEMPLATE = """
WITH supervision_start_dates AS (
  SELECT
    person_id,
    state_code,
    supervision_type,
    -- This is MAX to handle the case where a person may have had multiple periods
    -- on supervision. We surmise that there should be no overlapping supervision
    -- periods for a single person under a single supervision type.
    MAX(start_date) AS supervision_start_date
  FROM
    `{project_id}.{dataflow_metrics_materialized_dataset}.most_recent_supervision_start_metrics_materialized`
  GROUP BY
    person_id,
    state_code,
    supervision_type
),
latest_face_to_face AS (
  SELECT
    person_id,
    state_code,
    MAX(most_recent_face_to_face_date) AS most_recent_face_to_face_date
  FROM
    `{project_id}.{dataflow_metrics_materialized_dataset}.most_recent_supervision_case_compliance_metrics_materialized`
  WHERE
    person_external_id IS NOT NULL
  GROUP BY person_id, state_code
),
latest_employment AS (
    SELECT * EXCEPT (row_num) FROM (
        SELECT 
            employment_periods.state_code,
            employment_periods.person_external_id,
            employment_periods.employer,
            ROW_NUMBER() over (PARTITION BY person_external_id ORDER BY recorded_start_date DESC) as row_num
         FROM
            `{project_id}.{case_triage_dataset}.employment_periods` employment_periods
        WHERE
          NOT is_unemployed
          AND (recorded_end_date IS NULL OR recorded_end_date > CURRENT_DATE())
    ) employments
    WHERE row_num = 1
),
-- HACK ALERT 
-- TODO(#6200): Until a full ID ingest re-run is a complete, we need to grab address data on the fly
-- HACK ALERT
latest_idaho_address AS (
   SELECT 
    'US_ID' as state_code,
    offendernumber as person_external_id,
    current_address
   FROM
        (SELECT
          cis_offender.offendernumber,
          ARRAY_TO_STRING([line1, city, state_abbreviation, cis_personaddress.zipcode], ', ') AS current_address,
          ROW_NUMBER() OVER (PARTITION BY personid ORDER BY startdate DESC) as row_num
        FROM
            `{project_id}.us_id_raw_data_up_to_date_views.cis_offenderaddress_latest` cis_offenderaddress
        FULL OUTER JOIN
            `{project_id}.us_id_raw_data_up_to_date_views.cis_personaddress_latest` cis_personaddress
        ON
            id = personaddressid
        LEFT JOIN
            `{project_id}.{static_reference_tables_dataset}.state_ids`
        ON
            codestateid = CAST(state_id AS STRING)
        LEFT JOIN
            `{project_id}.us_id_raw_data_up_to_date_views.cis_offender_latest` cis_offender
        ON 
            cis_offender.id = cis_personaddress.personid
        WHERE cis_personaddress.personid IS NOT NULL
            AND cis_offenderaddress.validaddress = 'T' -- Valid address
            AND cis_offenderaddress.enddate IS NULL -- Active address
            AND cis_personaddress.codeaddresstypeid IN ('1') -- Physical address
         ) latest_idaho_addresses
    WHERE row_num = 1
),
-- TODO(#5943): Make ideal_query the main query body.
ideal_query AS (
SELECT
    {columns}
FROM
  `{project_id}.{dataflow_metrics_materialized_dataset}.most_recent_daily_supervision_population_materialized`
LEFT JOIN
  `{project_id}.state.state_person`
USING (person_id, gender, state_code)
-- TODO(#5463): When we ingest employment info, we should replace this joined table with the correct table.
LEFT JOIN
  latest_employment
USING (person_external_id, state_code)
LEFT JOIN
  `{project_id}.{case_triage_dataset}.last_known_date_of_employment`
USING (person_external_id, state_code)
LEFT JOIN
  `{project_id}.{case_triage_dataset}.latest_assessments`
USING (person_id, state_code)
LEFT JOIN
  latest_face_to_face
USING (person_id, state_code)
LEFT JOIN
  supervision_start_dates
USING (person_id, state_code, supervision_type)
WHERE
  supervision_level IS NOT NULL
),
-- HACK ALERT HACK ALERT HACK ALERT HACK ALERT HACK ALERT HACK ALERT HACK ALERT HACK ALERT
--
-- HACK ALERT HACK ALERT HACK ALERT HACK ALERT
--
-- HACK ALERT HACK ALERT HACK ALERT HACK ALERT HACK ALERT HACK ALERT HACK ALERT HACK ALERT
--
-- TODO(#5943): We unfortunately have to pull straight from raw data from Idaho due to internal
-- inconsistencies in Idaho's data. Our ingest pipeline assumed that the historical record
-- was accurate, but unfortunately that no longer seems to be the case. The long-term solution
-- involves fetching an updates one-off historical dump of the casemgr table, re-running ingest,
-- and adding validation to ensure this doesn't happen, but the timescale of this is much
-- slower than we want to move for Case Triage.
--
-- Hence, the decision to add this very verbose warning to encourage future readers to decide
-- whether they should start trying to pay down this technical debt.
--
-- HACK ALERT HACK ALERT HACK ALERT HACK ALERT HACK ALERT HACK ALERT HACK ALERT HACK ALERT
--
-- HACK ALERT HACK ALERT HACK ALERT HACK ALERT
--
-- HACK ALERT HACK ALERT HACK ALERT HACK ALERT HACK ALERT HACK ALERT HACK ALERT HACK ALERT
with_derived_supervising_officer as (
    SELECT
      ideal_query.* EXCEPT (current_address, supervising_officer_external_id),
      IF(state_code != 'US_ID', ideal_query.supervising_officer_external_id, UPPER(ofndr_agnt.agnt_id)) AS supervising_officer_external_id,
      -- TODO(#6200): Until a full ID ingest re-run is a complete, we need to grab address data on the fly
      IF(state_code != 'US_ID', ideal_query.current_address, latest_idaho_address.current_address) as current_address,
  FROM
      ideal_query
    LEFT OUTER JOIN
      `{project_id}.us_id_raw_data_up_to_date_views.ofndr_agnt_latest` ofndr_agnt
    ON ideal_query.person_external_id = ofndr_agnt.ofndr_num
    LEFT OUTER JOIN 
        latest_idaho_address
    USING (person_external_id, state_code)
)
SELECT *
FROM with_derived_supervising_officer
WHERE with_derived_supervising_officer.supervising_officer_external_id IS NOT NULL;
"""

CLIENT_LIST_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=VIEWS_DATASET,
    view_id="etl_clients",
    view_query_template=CLIENT_LIST_QUERY_TEMPLATE,
    case_triage_dataset=CASE_TRIAGE_DATASET,
    dataflow_metrics_materialized_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    static_reference_tables_dataset=STATIC_REFERENCE_TABLES_DATASET,
    columns=[
        "state_code",
        "person_external_id",
        "full_name",
        "gender",
        "birthdate",
        "birthdate_inferred_from_age",
        "supervision_start_date",
        "projected_end_date",
        "supervision_type",
        "case_type",
        "supervision_level",
        "employer",
        "last_known_date_of_employment",
        "most_recent_assessment_date",
        "assessment_score",
        "most_recent_face_to_face_date",
        # TODO(#5943): supervising_officer_external_id must be at the end of
        # this list because of the way that we have to derive this result from
        # the ofndr_agnt table for Idaho.
        "supervising_officer_external_id",
        # TODO(#6200): Until a full ID ingest re-run is a complete, we need to grab address data on the fly
        "current_address",
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CLIENT_LIST_VIEW_BUILDER.build_and_print()
