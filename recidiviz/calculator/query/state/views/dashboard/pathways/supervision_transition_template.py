#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2021 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#   =============================================================================
"""Template for queries to count transitions out of supervision by month."""

from datetime import date


def supervision_transition_template(status: str) -> str:
    current_year = date.today().year

    supervision_categories = [
        "PAROLE",
        "PROBATION",
        "INFORMAL_PROBATION",
        "BENCH_WARRANT",
        "ABSCONSION",
        "ALL",
    ]
    genders = ["MALE", "FEMALE", "ALL"]

    return f"""
    WITH summed_person_count AS (
      SELECT
        state_code,
        EXTRACT(YEAR FROM transition_date) as year,
        EXTRACT(MONTH FROM transition_date) as month,
        gender,
        supervision_type,
        COUNT(1) as person_count
      FROM
        `{{project_id}}.{{reference_dataset}}.supervision_to_{status}_transitions`,
      UNNEST ([gender, 'ALL']) AS gender,
      UNNEST ([supervision_type, 'ALL']) AS supervision_type
      GROUP BY 1, 2, 3, 4, 5
    ), blanks_filled AS (
      SELECT
        * EXCEPT (person_count),
        IFNULL(person_count, 0) as person_count
      FROM
        summed_person_count
      FULL OUTER JOIN
        (SELECT * FROM
          UNNEST(["US_ID"]) AS state_code,
          UNNEST({[current_year - y for y in range(6)]}) AS year,
          UNNEST({list(range(1,13))}) AS month,
          UNNEST({supervision_categories}) as supervision_type,
          UNNEST({genders}) as gender)
      USING (state_code, year, month, gender, supervision_type)
    ), averaged_person_count AS (
      SELECT
        *,
        ROUND(AVG(person_count) OVER (
          PARTITION BY state_code, gender, supervision_type
          ORDER BY year, month
          ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        )) as avg_90day,
      FROM blanks_filled
    )

    SELECT *
    FROM averaged_person_count
    WHERE DATE(year, month, 1) BETWEEN
      DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 60 MONTH) AND CURRENT_DATE('US/Eastern')
    ORDER BY state_code, year, month, gender, supervision_type
    """
