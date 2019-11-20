# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
# pylint: disable=line-too-long
"""Unpivot stitch data"""

from recidiviz.calculator.query.bqview import BigQueryView
from recidiviz.calculator.query.county.views.stitch.combined_stitch_drop_overlapping \
    import COMBINED_STITCH_DROP_OVERLAPPING_VIEW
from recidiviz.calculator.query.county.view_config import VIEWS_DATASET
from recidiviz.utils import metadata

PROJECT_ID: str = metadata.project_id()

_DESCRIPTION = """
Unpivot stitch data by breaking out gender & race counts.
"""

# Note: This query is written using the pivot_bq_stitch_query.py tool
_QUERY = """
/*{description}*/

WITH male_asian AS (
  SELECT
    fips,
    day,
    data_source,
    male_asian AS count,
    'MALE' AS gender,
    'ASIAN' AS race
  FROM
    `{project_id}.{views_dataset}.{combined_stitch_drop_overlapping}`
),


male_black AS (
  SELECT
    fips,
    day,
    data_source,
    male_black AS count,
    'MALE' AS gender,
    'BLACK' AS race
  FROM
    `{project_id}.{views_dataset}.{combined_stitch_drop_overlapping}`
),


male_native_american AS (
  SELECT
    fips,
    day,
    data_source,
    male_native_american AS count,
    'MALE' AS gender,
    'NATIVE_AMERICAN' AS race
  FROM
    `{project_id}.{views_dataset}.{combined_stitch_drop_overlapping}`
),


male_latino AS (
  SELECT
    fips,
    day,
    data_source,
    male_latino AS count,
    'MALE' AS gender,
    'LATINO' AS race
  FROM
    `{project_id}.{views_dataset}.{combined_stitch_drop_overlapping}`
),


male_white AS (
  SELECT
    fips,
    day,
    data_source,
    male_white AS count,
    'MALE' AS gender,
    'WHITE' AS race
  FROM
    `{project_id}.{views_dataset}.{combined_stitch_drop_overlapping}`
),


male_other AS (
  SELECT
    fips,
    day,
    data_source,
    male_other AS count,
    'MALE' AS gender,
    'OTHER' AS race
  FROM
    `{project_id}.{views_dataset}.{combined_stitch_drop_overlapping}`
),


male_unknown_race AS (
  SELECT
    fips,
    day,
    data_source,
    male_unknown_race AS count,
    'MALE' AS gender,
    'UNKNOWN_RACE' AS race
  FROM
    `{project_id}.{views_dataset}.{combined_stitch_drop_overlapping}`
),


female_asian AS (
  SELECT
    fips,
    day,
    data_source,
    female_asian AS count,
    'FEMALE' AS gender,
    'ASIAN' AS race
  FROM
    `{project_id}.{views_dataset}.{combined_stitch_drop_overlapping}`
),


female_black AS (
  SELECT
    fips,
    day,
    data_source,
    female_black AS count,
    'FEMALE' AS gender,
    'BLACK' AS race
  FROM
    `{project_id}.{views_dataset}.{combined_stitch_drop_overlapping}`
),


female_native_american AS (
  SELECT
    fips,
    day,
    data_source,
    female_native_american AS count,
    'FEMALE' AS gender,
    'NATIVE_AMERICAN' AS race
  FROM
    `{project_id}.{views_dataset}.{combined_stitch_drop_overlapping}`
),


female_latino AS (
  SELECT
    fips,
    day,
    data_source,
    female_latino AS count,
    'FEMALE' AS gender,
    'LATINO' AS race
  FROM
    `{project_id}.{views_dataset}.{combined_stitch_drop_overlapping}`
),


female_white AS (
  SELECT
    fips,
    day,
    data_source,
    female_white AS count,
    'FEMALE' AS gender,
    'WHITE' AS race
  FROM
    `{project_id}.{views_dataset}.{combined_stitch_drop_overlapping}`
),


female_other AS (
  SELECT
    fips,
    day,
    data_source,
    female_other AS count,
    'FEMALE' AS gender,
    'OTHER' AS race
  FROM
    `{project_id}.{views_dataset}.{combined_stitch_drop_overlapping}`
),


female_unknown_race AS (
  SELECT
    fips,
    day,
    data_source,
    female_unknown_race AS count,
    'FEMALE' AS gender,
    'UNKNOWN_RACE' AS race
  FROM
    `{project_id}.{views_dataset}.{combined_stitch_drop_overlapping}`
),


unknown_gender_asian AS (
  SELECT
    fips,
    day,
    data_source,
    unknown_gender_asian AS count,
    'UNKNOWN_GENDER' AS gender,
    'ASIAN' AS race
  FROM
    `{project_id}.{views_dataset}.{combined_stitch_drop_overlapping}`
),


unknown_gender_black AS (
  SELECT
    fips,
    day,
    data_source,
    unknown_gender_black AS count,
    'UNKNOWN_GENDER' AS gender,
    'BLACK' AS race
  FROM
    `{project_id}.{views_dataset}.{combined_stitch_drop_overlapping}`
),


unknown_gender_native_american AS (
  SELECT
    fips,
    day,
    data_source,
    unknown_gender_native_american AS count,
    'UNKNOWN_GENDER' AS gender,
    'NATIVE_AMERICAN' AS race
  FROM
    `{project_id}.{views_dataset}.{combined_stitch_drop_overlapping}`
),


unknown_gender_latino AS (
  SELECT
    fips,
    day,
    data_source,
    unknown_gender_latino AS count,
    'UNKNOWN_GENDER' AS gender,
    'LATINO' AS race
  FROM
    `{project_id}.{views_dataset}.{combined_stitch_drop_overlapping}`
),


unknown_gender_white AS (
  SELECT
    fips,
    day,
    data_source,
    unknown_gender_white AS count,
    'UNKNOWN_GENDER' AS gender,
    'WHITE' AS race
  FROM
    `{project_id}.{views_dataset}.{combined_stitch_drop_overlapping}`
),


unknown_gender_other AS (
  SELECT
    fips,
    day,
    data_source,
    unknown_gender_other AS count,
    'UNKNOWN_GENDER' AS gender,
    'OTHER' AS race
  FROM
    `{project_id}.{views_dataset}.{combined_stitch_drop_overlapping}`
),


unknown_gender_unknown_race AS (
  SELECT
    fips,
    day,
    data_source,
    unknown_gender_unknown_race AS count,
    'UNKNOWN_GENDER' AS gender,
    'UNKNOWN_RACE' AS race
  FROM
    `{project_id}.{views_dataset}.{combined_stitch_drop_overlapping}`
)

SELECT * FROM male_asian
UNION ALL
SELECT * FROM male_black
UNION ALL
SELECT * FROM male_native_american
UNION ALL
SELECT * FROM male_latino
UNION ALL
SELECT * FROM male_white
UNION ALL
SELECT * FROM male_other
UNION ALL
SELECT * FROM male_unknown_race
UNION ALL
SELECT * FROM female_asian
UNION ALL
SELECT * FROM female_black
UNION ALL
SELECT * FROM female_native_american
UNION ALL
SELECT * FROM female_latino
UNION ALL
SELECT * FROM female_white
UNION ALL
SELECT * FROM female_other
UNION ALL
SELECT * FROM female_unknown_race
UNION ALL
SELECT * FROM unknown_gender_asian
UNION ALL
SELECT * FROM unknown_gender_black
UNION ALL
SELECT * FROM unknown_gender_native_american
UNION ALL
SELECT * FROM unknown_gender_latino
UNION ALL
SELECT * FROM unknown_gender_white
UNION ALL
SELECT * FROM unknown_gender_other
UNION ALL
SELECT * FROM unknown_gender_unknown_race
""".format(project_id=PROJECT_ID, views_dataset=VIEWS_DATASET,
           combined_stitch_drop_overlapping=
           COMBINED_STITCH_DROP_OVERLAPPING_VIEW.view_id,
           description=_DESCRIPTION)

# TODO(#1578): Export this query once COMBINED_STITCH_DROP_OVERLAPPING_VIEW
#  is materialized
COMBINED_STITCH_DROP_OVERLAPPING_UNPIVOT_VIEW = BigQueryView(
    view_id='combined_stitch_drop_overlapping_unpivot',
    view_query=_QUERY
)

if __name__ == '__main__':
    print(COMBINED_STITCH_DROP_OVERLAPPING_UNPIVOT_VIEW.view_query)
