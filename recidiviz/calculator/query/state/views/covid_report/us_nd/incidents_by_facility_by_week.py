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
"""US_ND counts of incidents (reprehensible offenses while in custody) by facility by week"""
# pylint: disable=trailing-whitespace,line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import COVID_REPORT_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

INCIDENTS_BY_FACILITY_BY_WEEK_VIEW_NAME = 'incidents_by_facility_by_week'

INCIDENTS_BY_FACILITY_BY_WEEK_DESCRIPTION = \
    """ US_ND counts of incidents (reprehensible offenses while in custody) by facility by week"""

INCIDENTS_BY_FACILITY_BY_WEEK_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH incidents AS (
        SELECT
          -- Using the REPORT_DATE as the date of the incident to account for reporting lag time --
          PARSE_DATE('%m/%d/%Y', REGEXP_EXTRACT(REPORT_DATE, r{date_regex})) as incident_date,
          INCIDENT_TYPE as incident_type,
          RESULT_OIC_OFFENCE_CATEGORY as category,
          AGY_LOC_ID as facility
        FROM
          `{project_id}.{static_reference_dataset}.us_nd_offense_in_custody_and_pos_report_data`
        WHERE INCIDENT_TYPE NOT IN ('POSREPORT', 'PROP')
        AND (RESULT_OIC_OFFENCE_CATEGORY IS NULL OR RESULT_OIC_OFFENCE_CATEGORY NOT IN ('PBR', 'DELETE'))
        AND AGY_LOC_ID IN ({us_nd_report_facilities})
      ),
      -- Week dates for incident counts in the COVID-19 Report --
      incident_report_weeks AS (
       SELECT
        (row_number() OVER ()) - 4 as week_num,
        start_date,
        DATE_ADD(start_date, INTERVAL 6 DAY) as end_date
      FROM
        (SELECT * FROM UNNEST(GENERATE_DATE_ARRAY('2020-02-20', DATE_SUB(CURRENT_DATE('America/Los_Angeles'), INTERVAL 1 DAY), INTERVAL 1 WEEK)) as start_date)
      )
    
    
    SELECT
      'US_ND' as state_code,
      week_num,
      IFNULL(start_date, DATE_ADD(start_date_19, INTERVAL 1 YEAR)) AS start_date,
      IFNULL(end_date, DATE_ADD(end_date_19, INTERVAL 1 YEAR)) AS end_date,
      facility,
      IFNULL(level_1, 0) AS level_1,
      IFNULL(level_2, 0) AS level_2,
      IFNULL(level_3, 0) AS level_3,
      IFNULL(incident_count, 0) as incident_count,
      IFNULL(incident_count_2019, 0) as incident_count_2019
    FROM
      (SELECT
        week_num,
        start_date,
        end_date,
        facility,
        COUNTIF(REGEXP_CONTAINS(category, 'LVL1') OR REGEXP_CONTAINS(category, 'MIN')) as level_1,
        COUNTIF(REGEXP_CONTAINS(category, 'LVL2')) as level_2,
        COUNTIF(REGEXP_CONTAINS(category, 'LVL3')) as level_3,
        COUNT(IF(category IS NOT NULL, incident_date, NULL)) as incident_count
      FROM
        (SELECT 
          week_num,
          start_date,
          end_date,
          incident_date,
          IF((category IS NULL AND incident_type = 'MINOR'), 'MIN', category) AS category,
          weeks.facility
        FROM
        (SELECT * FROM
          UNNEST(['NDSP','JRCC', 'DWCRC', 'MRCC']) as facility,
          incident_report_weeks) weeks
        LEFT JOIN incidents
        ON weeks.facility = incidents.facility AND INCIDENT_DATE BETWEEN start_date AND end_date)
      GROUP BY week_num, start_date, end_date, facility)
    FULL OUTER JOIN
    (SELECT
      week_num,
      facility,
      start_date as start_date_19,
      end_date as end_date_19,
      COUNT(incident_date) as incident_count_2019
    FROM
      (SELECT 
        week_num,
        start_date,
        end_date,
        incident_date,
        IF((category IS NULL AND incident_type = 'MINOR'), 'MIN', category) AS category,
        weeks.facility
      FROM
      (SELECT
        week_num,
        DATE_SUB(start_date, INTERVAL 1 YEAR) as start_date,
        DATE_SUB(end_date, INTERVAL 1 YEAR) as end_date,
        facility
       FROM 
       (SELECT * FROM
          UNNEST([{us_nd_report_facilities}]) as facility,
          incident_report_weeks)) weeks
      LEFT JOIN incidents
      ON weeks.facility = incidents.facility AND INCIDENT_DATE BETWEEN start_date AND end_date)
    GROUP BY week_num, start_date, end_date, facility)
    USING (week_num, facility)
    ORDER BY facility, week_num, start_date, end_date
"""

DATE_REGEX_MATCHER = r"'\d{{1,2}}/\d{{1,2}}/\d{{4}}'"
US_ND_REPORT_FACILITIES = "'NDSP','JRCC', 'DWCRC', 'MRCC'"

INCIDENTS_BY_FACILITY_BY_WEEK_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.COVID_REPORT_DATASET,
    view_id=INCIDENTS_BY_FACILITY_BY_WEEK_VIEW_NAME,
    view_query_template=INCIDENTS_BY_FACILITY_BY_WEEK_QUERY_TEMPLATE,
    description=INCIDENTS_BY_FACILITY_BY_WEEK_DESCRIPTION,
    covid_report_dataset=COVID_REPORT_DATASET,
    date_regex=DATE_REGEX_MATCHER,
    us_nd_report_facilities=US_ND_REPORT_FACILITIES,
    static_reference_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCIDENTS_BY_FACILITY_BY_WEEK_VIEW_BUILDER.build_and_print()
