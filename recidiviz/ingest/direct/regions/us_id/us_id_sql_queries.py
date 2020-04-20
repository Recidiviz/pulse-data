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

"""These queries should be used to generate CSVs for Idaho ingestion."""

# TODO(3020): Move queries into BQ once ingestion flow supports querying directly from BQ.
from typing import List, Tuple, Optional

from recidiviz.ingest.direct.query_utils import output_sql_queries
from recidiviz.utils import metadata

project_id = metadata.project_id()


OFFENDER_OFNDR_DOB = \
    f"""
    -- offender_ofndr_dob
    SELECT
        *
    FROM
        `{project_id}.us_id_raw_data.offender` offender
    LEFT JOIN
        `{project_id}.us_id_raw_data.ofndr_dob` dob
    ON
      offender.docno = dob.ofndr_num
    """

OFNDR_TST_OFNDR_TST_CERT_QUERY = \
    f"""
    -- ofndr_tst_ofndr_tst_cert
    SELECT
        *
    EXCEPT
        (updt_usr_id, updt_dt)
    FROM
        `{project_id}.us_id_raw_data.ofndr_tst`  tst
    LEFT JOIN
        `{project_id}.us_id_raw_data.ofndr_tst_cert` tst_cert
    USING
        (ofndr_tst_id, assess_tst_id)
    WHERE
        tst.assess_tst_id = '2'  # LSIR assessments
        AND tst_cert.cert_pass_flg = 'Y'  # Test score has been certified
    ORDER BY
        tst.ofndr_num
    """

MITTIMUS_JUDGE_SENTENCE_OFFENSE_SENTPROB_SUPERVISION_SENTENCES_QUERY = \
    f"""
    SELECT
        *
    FROM
        `{project_id}.us_id_raw_data.mittimus`
    LEFT JOIN
        `{project_id}.us_id_raw_data.county`
    USING
        (cnty_cd)
    # Only ignore jud_cd because already present in `county` table
    LEFT JOIN
        (SELECT
                judge_cd,
                judge_name
        FROM
            `{project_id}.us_id_raw_data.judge`)
    USING
        (judge_cd)
    JOIN
        `{project_id}.us_id_raw_data.sentence`
    USING
        (mitt_srl)
    LEFT JOIN
        `{project_id}.us_id_raw_data.offense`
    USING
        (off_cat, off_cd, off_deg)
    # Inner join against sentences that are supervision sentences.
    JOIN
        `{project_id}.us_id_raw_data.sentprob` sentprob
    USING
        (mitt_srl, sent_no)
    WHERE
        sent_disp != 'A' # TODO(2999): Figure out how to deal with the 10.7k amended sentences
    ORDER BY
        CAST(docno AS INT64),
        CAST(sent_no AS INT64)
"""

MITTIMUS_JUDGE_SENTENCE_OFFENSE_SENTPROB_INCARCERATION_SENTENCES_QUERY = \
    f"""
    SELECT
        *
    FROM
        `{project_id}.us_id_raw_data.mittimus`
    LEFT JOIN
        `{project_id}.us_id_raw_data.county`
    USING
        (cnty_cd)
    # Only ignore jud_cd because already present in `county` table
    LEFT JOIN
        (SELECT
                judge_cd,
                judge_name
        FROM
            `{project_id}.us_id_raw_data.judge`)
    USING
        (judge_cd)
    JOIN
        `{project_id}.us_id_raw_data.sentence`
    USING
        (mitt_srl)
    LEFT JOIN
        `{project_id}.us_id_raw_data.offense`
    USING
        (off_cat, off_cd, off_deg)
    JOIN
        # Inner join against sentences that are incarceration sentences.
        (SELECT
            mitt_srl,
            sent_no
        FROM
            `{project_id}.us_id_raw_data.sentence`
        LEFT JOIN
            `{project_id}.us_id_raw_data.sentprob` sentprob
        USING
            (mitt_srl, sent_no)
        WHERE
            sentprob.mitt_srl IS NULL)
    USING
        (mitt_srl, sent_no)
    WHERE
        sent_disp != 'A' # TODO(2999): Figure out how to deal with the 10.7k amended sentences
    ORDER BY
        CAST(docno AS INT64),
        CAST(sent_no AS INT64)
"""

# Idaho provides us with a table 'movements', which acts as a ledger for each person's physical movements throughout
# the ID criminal justice system. By default, each row of this table includes a location and the date that the
# person arrived at that location (`move_dtd`). New entries into this table are created every time a person's
# location is updated. However, this table tracks not just movements across facilities (prisons or supervision
# districts) but also movements within a single facility (changing prison pods, for example). The goal of these
# query fragments is to transform the raw `movements` table into a set of movement_periods, where each row has
# a single facility with a start and end date which represent a person's overall time in that facility.
FACILITY_PERIOD_FRAGMENT = f"""

    # Raw movements table with a move_dtd parsed as a datetime
    facilities_with_datetime AS (
       SELECT 
          m.docno,
          m.incrno,
          m.fac_cd,
          f.fac_typ,
          f.fac_ldesc,
          m.move_srl,
          DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M', m.move_dtd)) AS move_dtd,
          LAG(m.fac_cd) 
            OVER (PARTITION BY 
              m.docno,
              m.incrno
              ORDER BY DATETIME(PARSE_TIMESTAMP('%Y-%m-%d %H:%M', m.move_dtd)))
            AS previous_fac_cd
        FROM 
          `{project_id}.us_id_raw_data.movement` m
        LEFT JOIN 
           `{project_id}.us_id_raw_data.facility` f
        USING (fac_cd)
    ),
    # This query here only keeps rows from `facilities_with_datetime` that represent a movement of a person between
    # facilities (and not a movement within a single facility).
    collapsed_facilities AS (
      SELECT 
        * 
        EXCEPT (previous_fac_cd)
      FROM 
        facilities_with_datetime 
      WHERE 
        # Default values in equality check if nulls are provided. Without this any null in fac_cd
        # or previous_fac_cd results in the row being ignored
        (IFNULL(fac_cd, '') != IFNULL(previous_fac_cd, ''))
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
        fac_ldesc
      FROM collapsed_facilities
    )
"""

OFFSTAT_STATUS_FRAGMENT = """
      SELECT
        *
      FROM
        offstat_with_dates
      WHERE 
        stat_cd = '{stat_cd}'
        AND stat_strt_typ = '{strt_typ}'
"""

PERIOD_TO_DATES_FRAGMENT = """
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

FACILITY_PERIOD_TO_STATUS_INFO_FRAGMENT = """
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
        PARSE_DATE('%x', stat_intake_dtd) AS stat_intake_dtd,
        PARSE_DATE('%x', stat_strt_dtd) AS start_date,
        COALESCE(PARSE_DATE('%x', stat_rls_dtd), CAST('9999-12-31' AS DATE)) AS end_date,
        stat_strt_typ, 
        stat_rls_typ, 
      FROM `{project_id}.us_id_raw_data.offstat`
    ),
    # Time spans that a person spent on a rider
    rider_periods AS ({OFFSTAT_STATUS_FRAGMENT.format(stat_cd='I', strt_typ='RJ')}),
    # Time spans that a person spent as a parole violator
    parole_violator_periods AS ({OFFSTAT_STATUS_FRAGMENT.format(stat_cd='I', strt_typ='PV')}),
    # Time spans that a person spent on investigative probation
    investigative_periods AS ({OFFSTAT_STATUS_FRAGMENT.format(stat_cd='P', strt_typ='PS')}),

    # Create view with all important dates

    # All facility period start/end dates
    facility_dates AS (
        {PERIOD_TO_DATES_FRAGMENT.format(periods='facility_periods')}
    ),
    # All rider period start/end dates
    rider_dates AS (
        {PERIOD_TO_DATES_FRAGMENT.format(periods='rider_periods')}
    ),
    # All parole violator dates
    parole_violator_dates AS (
        {PERIOD_TO_DATES_FRAGMENT.format(periods='parole_violator_periods')}
    ),
    investigative_dates AS (
        {PERIOD_TO_DATES_FRAGMENT.format(periods='investigative_periods')}
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
        {FACILITY_PERIOD_TO_STATUS_INFO_FRAGMENT.format(
    status_name='rider',
    facility_periods='periods_with_facility_info',
    status_periods='rider_periods')}  
    ),
    periods_with_facility_rider_and_pv_info AS (
        {FACILITY_PERIOD_TO_STATUS_INFO_FRAGMENT.format(
    status_name='parole_violator',
    facility_periods='periods_with_facility_and_rider_info',
    status_periods='parole_violator_periods')}  
    ),
    periods_with_all_info AS (
        {FACILITY_PERIOD_TO_STATUS_INFO_FRAGMENT.format(
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
          LAG(rider)
            OVER (PARTITION BY docno, incrno ORDER BY start_date, end_date) AS prev_rider,
          LAG(parole_violator)
            OVER (PARTITION BY docno, incrno ORDER BY start_date, end_date) AS prev_parole_violator,
          LAG(investigative)
            OVER (PARTITION BY docno, incrno ORDER BY start_date, end_date) AS prev_investigative,
          fac_cd,
          fac_typ,
          fac_ldesc,
          rider,
          parole_violator,
          investigative,
          LEAD(fac_cd)
            OVER (PARTITION BY docno, incrno ORDER BY start_date, end_date) AS next_fac_cd,
          LEAD(fac_typ)
            OVER (PARTITION BY docno, incrno ORDER BY start_date, end_date) AS next_fac_typ,
          LEAD(fac_ldesc)
            OVER (PARTITION BY docno, incrno ORDER BY start_date, end_date) AS next_fac_ldesc,
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

MOVEMENT_FACILITY_OFFSTAT_INCARCERATION_PERIODS_QUERY = f"""
    WITH {ALL_PERIODS_FRAGMENT}

    # Filter to just incarceration periods

    SELECT
      *,
      ROW_NUMBER() 
        OVER (PARTITION BY docno ORDER BY start_date, end_date) AS period_id
    FROM
      periods_with_previous_and_next_info
    WHERE 
      fac_typ = 'I'                             # Facility type incarceration
    # TODO(2999): Consider tracking escape incarceration periods in the same way we track absconscion.
    ORDER BY docno, incrno, start_date, end_date
"""

MOVEMENT_FACILITY_OFFSTAT_SUPERVISION_PERIODS_QUERY = f"""
    WITH {ALL_PERIODS_FRAGMENT}

    # Filter to just supervision periods

    SELECT 
      *,
      ROW_NUMBER() 
        OVER (PARTITION BY docno ORDER BY start_date, end_date) AS period_id
    FROM 
      periods_with_previous_and_next_info
    WHERE 
      fac_typ = 'P'                             # Facility type probation/parole
      OR (fac_typ = 'F' AND prev_fac_typ = 'P') # Fugitive, but escaped from supervision
    ORDER BY docno, incrno, start_date, end_date
"""

QSTN_NUMS_WITH_DESCRIPTIVE_ANSWERS_FRAGMENT = """
    qstn_nums_with_descriptive_answers AS (
      SELECT
        ofndr_num,
        body_loc_cd,
        ofndr_tst_id, 
        assess_tst_id,
        tst_dt,
        score_by_name,
        CONCAT(assess_qstn_num, '-', tst_sctn_num) AS qstn_num, 
        STRING_AGG(qstn_choice_desc) AS qstn_answer
      FROM 
        `{project_id}.us_id_raw_data.ofndr_tst`
      LEFT JOIN 
        `{project_id}.us_id_raw_data.tst_qstn_rspns`
      USING 
        (ofndr_tst_id, assess_tst_id)
      LEFT JOIN 
        `{project_id}.us_id_raw_data.assess_qstn_choice`
      USING 
        (assess_tst_id, tst_sctn_num, qstn_choice_num, assess_qstn_num)
      WHERE 
        assess_tst_id = '{test_id}'
      GROUP BY 
        ofndr_num, 
        body_loc_cd,
        ofndr_tst_id, 
        assess_tst_id,
        tst_dt,
        score_by_name,
        assess_qstn_num, 
        tst_sctn_num
    ) 
"""

VIOLATION_REPORT_OLD_QSTN_NUMS_WITH_DESCRIPTIVE_ANSWERS_FRAGMENT = \
    QSTN_NUMS_WITH_DESCRIPTIVE_ANSWERS_FRAGMENT.format(test_id='204', project_id=project_id)
VIOLATION_REPORT_QSTN_NUMS_WITH_DESCRIPTIVE_ANSWERS_FRAGMENT = \
    QSTN_NUMS_WITH_DESCRIPTIVE_ANSWERS_FRAGMENT.format(test_id='210', project_id=project_id)


OFNDR_TST_TST_QSTN_RSPNS_VIOLATION_REPORTS_QUERY = f"""
    WITH {VIOLATION_REPORT_QSTN_NUMS_WITH_DESCRIPTIVE_ANSWERS_FRAGMENT}
    SELECT 
      ofndr_num,
      body_loc_cd,
      ofndr_tst_id,
      assess_tst_id,
      tst_dt,
      score_by_name,
      # Question 1-1 is violation date, which we already have access to from tst_dt
      MAX(IF(qstn_num='2-2', qstn_answer, NULL)) AS violation_types,
      MAX(IF(qstn_num='3-3', qstn_answer, NULL)) AS new_crime_types,
      MAX(IF(qstn_num='4-4', qstn_answer, NULL)) AS parolee_placement_recommendation,
      MAX(IF(qstn_num='5-5', qstn_answer, NULL)) AS probationer_placement_recommendation,
      MAX(IF(qstn_num='6-6', qstn_answer, NULL)) AS legal_status
    FROM 
      qstn_nums_with_descriptive_answers
    GROUP BY
      ofndr_num,
      body_loc_cd,
      ofndr_tst_id,
      assess_tst_id,
      tst_dt,
      score_by_name
    ORDER BY 
      ofndr_num,
      ofndr_tst_id 
"""

OFNDR_TST_TST_QSTN_RSPNS_VIOLATION_REPORTS_OLD_QUERY = f"""
  WITH {VIOLATION_REPORT_OLD_QSTN_NUMS_WITH_DESCRIPTIVE_ANSWERS_FRAGMENT}
    SELECT 
      ofndr_num,
      body_loc_cd,
      ofndr_tst_id,
      assess_tst_id,
      tst_dt,
      score_by_name,
      # Question 1-1 is violation date, which we already have access to from tst_dt
      MAX(IF(qstn_num='1-2', qstn_answer, NULL)) AS violation_types,
      MAX(IF(qstn_num='1-3', qstn_answer, NULL)) AS new_crime_types,
      MAX(IF(qstn_num='1-4', qstn_answer, NULL)) AS pv_initiated_by_prosecutor,
      MAX(IF(qstn_num='1-5', qstn_answer, NULL)) AS parolee_placement_recommendation,
      MAX(IF(qstn_num='1-6', qstn_answer, NULL)) AS probationer_placement_recommendation,
      MAX(IF(qstn_num='1-7', qstn_answer, NULL)) AS legal_status,
    FROM 
      qstn_nums_with_descriptive_answers
    GROUP BY
      ofndr_num,
      body_loc_cd,
      ofndr_tst_id,
      assess_tst_id,
      tst_dt,
      score_by_name
    ORDER BY 
      ofndr_num,
      ofndr_tst_id
"""

OFNDR_AGNT_APPLC_USR_BODY_LOC_CD_CURRENT_POS_QUERY = f"""
    # TODO(2999): Integrate PO assignments into supervision query once we have a loss-less table with POs and their 
    #  assignments through history.
    SELECT 
      ofndr_num, 
      agnt_id,
      usr_id,
      name,
      # TODO(2999): Understand why these agencies don't align.
      a.agcy_id AS ofndr_agent_agcy,
      u.agcy_id AS applc_usr_agcy,
      agnt_strt_dt,
      end_dt,
      usr_typ_cd,
      updt_usr_id,
      updt_dt,
      lan_id,
      st_id_num,
      body_loc_cd,
      body_loc_desc,
      loc_typ_cd,
      body_loc_cd_id
    FROM 
      `{project_id}.us_id_raw_data.ofndr_agnt` a
    LEFT JOIN 
      `{project_id}.us_id_raw_data.applc_usr` u
    ON 
      (agnt_id = usr_id)
    LEFT JOIN 
      `{project_id}.us_id_raw_data.body_loc_cd`
    USING
      (body_loc_cd)
"""


def get_query_name_to_query_list() -> List[Tuple[str, str]]:
    return [
        ('offender_ofndr_dob', OFFENDER_OFNDR_DOB),
        ('ofndr_tst_ofndr_tst_cert', OFNDR_TST_OFNDR_TST_CERT_QUERY),
        ('mittimus_judge_sentence_offense_sentprob_supervision_sentences',
         MITTIMUS_JUDGE_SENTENCE_OFFENSE_SENTPROB_SUPERVISION_SENTENCES_QUERY),
        ('mittimus_judge_sentence_offense_sentprob_incarceration_sentences',
         MITTIMUS_JUDGE_SENTENCE_OFFENSE_SENTPROB_INCARCERATION_SENTENCES_QUERY),
        ('movement_facility_offstat_incarceration_periods', MOVEMENT_FACILITY_OFFSTAT_INCARCERATION_PERIODS_QUERY),
        ('movement_facility_offstat_supervision_periods', MOVEMENT_FACILITY_OFFSTAT_SUPERVISION_PERIODS_QUERY),
        ('ofndr_tst_tst_qstn_rspns_violation_reports', OFNDR_TST_TST_QSTN_RSPNS_VIOLATION_REPORTS_QUERY),
        ('ofndr_tst_tst_qstn_rspns_violation_reports_old', OFNDR_TST_TST_QSTN_RSPNS_VIOLATION_REPORTS_OLD_QUERY),
        ('ofndr_agnt_applc_usr_body_loc_cd_current_pos_query', OFNDR_AGNT_APPLC_USR_BODY_LOC_CD_CURRENT_POS_QUERY),
    ]


if __name__ == '__main__':
    # Uncomment the os.path clause below (change the directory as desired) if you want the queries to write out to
    # separate files instead of to the console.
    output_dir: Optional[str] = None  # os.path.expanduser('~/Downloads/id_queries')
    output_sql_queries(get_query_name_to_query_list(), output_dir)
