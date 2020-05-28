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
"""Helper templates for the US_ID period queries."""
# Idaho provides us with a table 'movements', which acts as a ledger for each person's physical movements throughout
# the ID criminal justice system. By default, each row of this table includes a location and the date that the
# person arrived at that location (`move_dtd`). New entries into this table are created every time a person's
# location is updated. However, this table tracks not just movements across facilities (prisons or supervision
# districts) but also movements within a single facility (changing prison pods, for example). The goal of these
# query fragments is to transform the raw `movements` table into a set of movement_periods, where each row has
# a single facility with a start and end date which represent a person's overall time in that facility.
FACILITY_PERIOD_FRAGMENT = """

    # Raw movements table with a move_dtd parsed as a datetime
    facilities_with_datetime AS (
       SELECT 
          m.docno,
          m.incrno,
          m.fac_cd,
          f.fac_typ,
          f.fac_ldesc,
          loc.loc_cd,
          loc.loc_ldesc,
          m.move_srl,
          # TODO(3284): Remove SUBSTR call if we completely transition away from the manually uploaded files and 
          #  therefore only expect one date format
          DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M', SUBSTR(m.move_dtd, 1, 16))) AS move_dtd,
          LAG(m.fac_cd) 
            OVER (PARTITION BY 
              m.docno,
              m.incrno
              ORDER BY DATETIME(PARSE_TIMESTAMP('%Y-%m-%d %H:%M', SUBSTR(m.move_dtd, 1, 16))))
            AS previous_fac_cd,
          LAG(loc.loc_cd) 
            OVER (PARTITION BY 
              m.docno,
              m.incrno
              ORDER BY DATETIME(PARSE_TIMESTAMP('%Y-%m-%d %H:%M', SUBSTR(m.move_dtd, 1, 16))))
          AS previous_loc_cd
        FROM 
          {movement} m
        LEFT JOIN 
           {facility} f
        USING (fac_cd)
        LEFT JOIN 
           {location} loc
        USING (loc_cd)
    ),
    # This query here only keeps rows from `facilities_with_datetime` that represent a movement of a person between
    # facilities and locations (but not a movement within a single facility/location). If we ever need to track
    # movements between living units, we can delete this subquery
    collapsed_facilities AS (
      SELECT 
        * 
        EXCEPT (previous_fac_cd)
      FROM 
        facilities_with_datetime 
      WHERE 
        # Default values in equality check if nulls are provided. Without this any null values result in the row being 
        # ignored
        (IFNULL(fac_cd, '') != IFNULL(previous_fac_cd, ''))
        OR (IFNULL(loc_cd, '') != IFNULL(previous_loc_cd, ''))
    ), 
    # This query transforms the `collapsed_facilities` table, which contains a facility and the date that a person
    # entered that facility, and transforms it into a table which has a start and end date for the given facility. We
    # get a facility end date by looking at the person's next facility start date.
    facility_periods AS (
      SELECT 
        docno,
        incrno,
        move_dtd AS start_date,
        COALESCE(
          LEAD(move_dtd)
            OVER (PARTITION BY docno, incrno ORDER BY move_dtd),
          CAST('9999-12-31' AS DATE)) AS end_date,
        fac_cd,
        fac_typ,
        fac_ldesc,
        loc_cd,
        loc_ldesc
      FROM collapsed_facilities
    )
"""

OFFSTAT_STATUS_FRAGMENT_TEMPLATE = """
      SELECT
        *
      FROM
        offstat_with_dates
      WHERE 
        stat_cd = '{stat_cd}'
        AND stat_strt_typ = '{strt_typ}'
"""

PERIOD_TO_DATES_FRAGMENT_TEMPLATE = """
      SELECT
        docno,
        incrno,
        start_date AS important_date
      FROM 
        {periods}
      UNION DISTINCT
      SELECT
        docno,
        incrno,
        end_date AS important_date
      FROM 
        {periods}
"""


FACILITY_PERIOD_TO_STATUS_INFO_FRAGMENT_TEMPLATE = """
      SELECT 
        p.*, 
        (s.docno IS NOT NULL) AS {status_name}
      FROM
        {facility_periods} p
      LEFT JOIN
        {status_periods} s
      ON
        (p.docno = s.docno
        AND p.incrno = s.incrno
        AND (p.start_date
             BETWEEN s.start_date
             AND IF (s.start_date = s.end_date, s.start_date, DATE_SUB(s.end_date, INTERVAL 1 DAY))))
"""


# The goal of this query fragment is to create a single row per '(incarceration/supervision)period' as Recidiviz has
# defined it.
#
# In this case, it means we need to combine information from two tables: movements and offstat. `movements` is a
# ledger of all people's physical movements throughout the criminal justice system (i.e. moving prison facilities,
# districts, prison cells, etc). Offstat is a table which contains relevant status information about people. For
# purposes of determining incarceration/supervision periods, the most useful statuses are Rider (treatment in
# prison), investigative (not sentenced yet), and parole violator (temporary custody before parole board determines
# if a revocation should occur).
#
# With the information from these two tables, the goal is create a new row when someone changes facility or when
# their status changes.
#
# At a high level, our approach is:
# 1. Create time spans for each person's stay in a specific facility (prison facility or supervision district).
# 2. Create time spans for each person's time with specific statuses
# 3. Add all significant dates for a person (start/end dates of each facility and of each status term) into a single
#    view. These ordered dates signify dates at which something changed that indicates we need to create a new
#    incarceration period.
# 4. Create new time spans based on the important dates from part 3. This is a superset of the distinct
#    periods we want at the end. It is a superset, because a) it still includes time spans for a person on
#    supervision, but b) because includes spans for which a person is not currently under DOC custody, but is
#    between sentences (i.e. span between the end date of one sentence and the beginning of another).
# 5. Inner joins these new time spans from part 4 back onto the facility spans in part 1. Any span which represents
#    a time period where the person was not under DOC supervision will not have a facility, and therefore will fall
#    out of our query.
# 6. Left join the time spans from part 5 to the status spans from part 2 in order to tell a persons status for the
#    given time span.
# 7. Use LEAD/LAG functions to get previous and next facility info for each row (this will later let us know more
#    info about in and out edges of periods. During this time also add a row number to each period.
ALL_PERIODS_FRAGMENT = f"""

    # Create facility time spans

    {FACILITY_PERIOD_FRAGMENT},

    # Create status time spans

    # Offstat table with dates parsed
    offstat_with_dates AS (
      SELECT 
        docno, 
        incrno,
        statno, 
        stat_cd, 
        # TODO(3284): Remove SAFE_CAST call if we completely transition away from the manually uploaded files and 
        #  therefore only expect one date format
        COALESCE(SAFE_CAST(stat_intake_dtd AS DATE), PARSE_DATE('%x', stat_intake_dtd)) AS stat_intake_dtd,
        COALESCE(SAFE_CAST(stat_strt_dtd AS DATE), PARSE_DATE('%x', stat_strt_dtd)) AS start_date,
        COALESCE(COALESCE(SAFE_CAST(stat_rls_dtd AS DATE), PARSE_DATE('%x', stat_rls_dtd)),
                 CAST('9999-12-31' AS DATE)) AS end_date,
        stat_strt_typ, 
        stat_rls_typ, 
      FROM {{offstat}}
    ),
    # Time spans that a person spent on a rider
    rider_periods AS ({OFFSTAT_STATUS_FRAGMENT_TEMPLATE.format(stat_cd='I', strt_typ='RJ')}),
    # Time spans that a person spent as a parole violator
    parole_violator_periods AS ({OFFSTAT_STATUS_FRAGMENT_TEMPLATE.format(stat_cd='I', strt_typ='PV')}),
    # Time spans that a person spent on investigative probation
    investigative_periods AS ({OFFSTAT_STATUS_FRAGMENT_TEMPLATE.format(stat_cd='P', strt_typ='PS')}),

    # Create view with all important dates

    # All facility period start/end dates
    facility_dates AS (
        {PERIOD_TO_DATES_FRAGMENT_TEMPLATE.format(periods='facility_periods')}
    ),
    # All rider period start/end dates
    rider_dates AS (
        {PERIOD_TO_DATES_FRAGMENT_TEMPLATE.format(periods='rider_periods')}
    ),
    # All parole violator dates
    parole_violator_dates AS (
        {PERIOD_TO_DATES_FRAGMENT_TEMPLATE.format(periods='parole_violator_periods')}
    ),
    investigative_dates AS (
        {PERIOD_TO_DATES_FRAGMENT_TEMPLATE.format(periods='investigative_periods')}
    ),
    # All dates where something we care about changed.
    all_dates AS (
      SELECT 
        *
      FROM 
        facility_dates
      UNION DISTINCT
      SELECT
        * 
      FROM
        rider_dates
      UNION DISTINCT
      SELECT
        * 
      FROM
        parole_violator_dates
      UNION DISTINCT
      SELECT
        * 
      FROM
        investigative_dates
    ),

    # Create time spans from the important dates

    all_periods AS (
      SELECT
        docno,
        incrno,
        important_date AS start_date,
        LEAD(important_date) 
          OVER (PARTITION BY docno, incrno ORDER BY important_date) AS end_date
      FROM 
        all_dates
    ),

    # Join important date spans back with the facility spans from Part 1.

    periods_with_facility_info AS (
      SELECT
        a.docno,
        a.incrno,
        a.start_date,
        # Choose smaller of two dates to handle facility_periods that last 1 day
        LEAST(a.end_date, f.end_date) AS end_date,
        f.fac_cd,
        f.fac_typ,
        f.fac_ldesc,
        f.loc_cd,
        f.loc_ldesc
      FROM 
        all_periods a
      # Inner join to only get periods of time where there is a matching facility
      JOIN
        facility_periods f
      USING
        (docno, incrno)
      WHERE
        a.start_date 
          BETWEEN f.start_date
          AND IF(f.start_date = f.end_date, f.start_date, DATE_SUB(f.end_date, INTERVAL 1 DAY))
    ),

    # Left join important date spans from part 5 with the status spans from part 2.

    periods_with_facility_and_rider_info AS (
        {FACILITY_PERIOD_TO_STATUS_INFO_FRAGMENT_TEMPLATE.format(
    status_name='rider',
    facility_periods='periods_with_facility_info',
    status_periods='rider_periods')}  
    ),
    periods_with_facility_rider_and_pv_info AS (
        {FACILITY_PERIOD_TO_STATUS_INFO_FRAGMENT_TEMPLATE.format(
    status_name='parole_violator',
    facility_periods='periods_with_facility_and_rider_info',
    status_periods='parole_violator_periods')}  
    ),
    periods_with_all_info AS (
        {FACILITY_PERIOD_TO_STATUS_INFO_FRAGMENT_TEMPLATE.format(
    status_name='investigative',
    facility_periods='periods_with_facility_rider_and_pv_info',
    status_periods='investigative_periods')}  
    ),

    # Add row numbers and include information about previous and next stays
    periods_with_previous_and_next_info AS (
        SELECT 
          docno,
          incrno,
          start_date,
          end_date,
          LAG(fac_cd)
            OVER (PARTITION BY docno, incrno ORDER BY start_date, end_date) AS prev_fac_cd,
          LAG(fac_typ)
            OVER (PARTITION BY docno, incrno ORDER BY start_date, end_date) AS prev_fac_typ,
          LAG(fac_ldesc)
            OVER (PARTITION BY docno, incrno ORDER BY start_date, end_date) AS prev_fac_ldesc,
          LAG(loc_cd)
            OVER (PARTITION BY docno, incrno ORDER BY start_date, end_date) AS prev_loc_cd,
          LAG(loc_ldesc)
            OVER (PARTITION BY docno, incrno ORDER BY start_date, end_date) AS prev_loc_ldesc,
          LAG(rider)
            OVER (PARTITION BY docno, incrno ORDER BY start_date, end_date) AS prev_rider,
          LAG(parole_violator)
            OVER (PARTITION BY docno, incrno ORDER BY start_date, end_date) AS prev_parole_violator,
          LAG(investigative)
            OVER (PARTITION BY docno, incrno ORDER BY start_date, end_date) AS prev_investigative,
          fac_cd,
          fac_typ,
          fac_ldesc,
          loc_cd,
          loc_ldesc,
          rider,
          parole_violator,
          investigative,
          LEAD(fac_cd)
            OVER (PARTITION BY docno, incrno ORDER BY start_date, end_date) AS next_fac_cd,
          LEAD(fac_typ)
            OVER (PARTITION BY docno, incrno ORDER BY start_date, end_date) AS next_fac_typ,
          LEAD(fac_ldesc)
            OVER (PARTITION BY docno, incrno ORDER BY start_date, end_date) AS next_fac_ldesc,
          LEAD(loc_cd)
            OVER (PARTITION BY docno, incrno ORDER BY start_date, end_date) AS next_loc_cd,
          LEAD(loc_ldesc)
            OVER (PARTITION BY docno, incrno ORDER BY start_date, end_date) AS next_loc_ldesc,
          LEAD(rider)
            OVER (PARTITION BY docno, incrno ORDER BY start_date, end_date) AS next_rider,
          LEAD(parole_violator)
            OVER (PARTITION BY docno, incrno ORDER BY start_date, end_date) AS next_parole_violator,
          LEAD(investigative)
            OVER (PARTITION BY docno, incrno ORDER BY start_date, end_date) AS next_investigative
        FROM 
          periods_with_all_info
    )
"""


def get_all_periods_query_fragment() -> str:
    return ALL_PERIODS_FRAGMENT
