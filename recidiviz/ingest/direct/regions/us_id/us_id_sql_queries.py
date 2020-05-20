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

from recidiviz.ingest.direct.controllers.direct_ingest_big_query_view_types import \
    DirectIngestRawDataTableUpToDateView, DirectIngestRawDataTableLatestView
from recidiviz.ingest.direct.query_utils import output_sql_queries, get_raw_table_config
from recidiviz.utils import metadata


# TODO(3020): Create class(es) to contain state queries so that we don't have to update global vars when running locally
project_id = metadata.project_id()
# Update to `False` if using this script to print queries locally (rather than querying through the API)
parameterized_query = True

STATE_CODE = 'us_id'


def _create_date_bounded_query(raw_table_name) -> str:
    if parameterized_query:
        return DirectIngestRawDataTableUpToDateView(
            region_code=STATE_CODE,
            raw_file_config=get_raw_table_config(region_code=STATE_CODE,
                                                 raw_table_name=raw_table_name)).view_query
    return DirectIngestRawDataTableLatestView(
        region_code=STATE_CODE,
        raw_file_config=get_raw_table_config(region_code=STATE_CODE,
                                             raw_table_name=raw_table_name)).select_query


def create_sentence_query_fragment(add_supervision_args: bool = False) -> str:
    """Helper method that creates query parameters meant for combining sentences rows with their corresponding
    amendment rows. If |add_supervision_args| is True, adds arguments unique to supervision sentences.
    """
    base_sentence_args = [
        'mitt_srl',
        'sent_no',
        'sent_disp',
    ]
    supervision_base_sentence_args = ['prob_no']

    amended_sentence_args = [
        'off_dtd',
        'off_cat',
        'off_cd',
        'off_deg',
        'off_cnt',
        'sent_min_yr',
        'sent_min_mo',
        'sent_min_da',
        'sent_max_yr',
        'sent_max_mo',
        'sent_max_da',
        'law_cd',
        'vio_doc',
        'vio_1311',
        'lifer',
        'enhanced',
        'govn_sent',
        'sent_gtr_dtd',
        'sent_beg_dtd',
        'sent_par_dtd',
        'sent_ind_dtd',
        'sent_ft_dtd',
        'sent_sat_dtd',
        'consec_typ',
        'consec_sent_no',
        'am_sent_no',
        'string_no',
    ]
    supervision_amended_sentence_args = [
        'prob_strt_dtd',
        'prob_yr',
        'prob_mo',
        'prob_da',
        'prob_end_dtd',
    ]

    if add_supervision_args:
        base_sentence_args += supervision_base_sentence_args
        amended_sentence_args += supervision_amended_sentence_args

    n_spaces = 8 * ' '
    query_fragment = '\n'
    for arg in base_sentence_args:
        query_fragment += f'{n_spaces}COALESCE(s.{arg}, a.{arg}) AS {arg},\n'
    for arg in amended_sentence_args:
        query_fragment += f'{n_spaces}COALESCE(a.{arg}, s.{arg}) AS {arg},\n'
    return query_fragment


OFFENDER_OFNDR_DOB = \
    f"""
    -- offender_ofndr_dob
    WITH
    offender AS ({_create_date_bounded_query('offender')}),
    dob AS ({_create_date_bounded_query('ofndr_dob')})
    SELECT
        *
        EXCEPT(updt_dt, updt_usr_id)  # Seem to update every week? (table is generated)
    FROM
        offender
    LEFT JOIN
        dob
    ON
      offender.docno = dob.ofndr_num
    """

OFNDR_TST_OFNDR_TST_CERT_QUERY = \
    f"""
    -- ofndr_tst_ofndr_tst_cert
    WITH
    ofndr_tst AS ({_create_date_bounded_query('ofndr_tst')}),
    ofndr_tst_cert AS ({_create_date_bounded_query('ofndr_tst_cert')})
    SELECT
        *
    EXCEPT
        (updt_usr_id, updt_dt)
    FROM
        ofndr_tst
    LEFT JOIN
        ofndr_tst_cert
    USING
        (ofndr_tst_id, assess_tst_id)
    WHERE
        ofndr_tst.assess_tst_id = '2'  # LSIR assessments
        AND ofndr_tst_cert.cert_pass_flg = 'Y'  # Test score has been certified
    ORDER BY
        ofndr_tst.ofndr_num
    """

# Gets all probation sentences
PROBATION_SENTENCES_QUERY = """
    SELECT
        *
    FROM
        sentence
    # Inner join against sentences that are supervision sentences.
    JOIN
        sentprob
    USING
        (mitt_srl, sent_no)
    ORDER BY
        CAST(mitt_srl AS INT64),
        CAST(sent_no AS INT64)
"""

# Gets all incarceration sentences
INCARCERATION_SENTENCES_QUERY = """
    SELECT
        *
    FROM
        sentence
    JOIN
        # Inner join against sentences that are incarceration sentences.
        (SELECT
            mitt_srl,
            sent_no
        FROM
            sentence
        LEFT JOIN
            sentprob
        USING
            (mitt_srl, sent_no)
        WHERE
            sentprob.mitt_srl IS NULL)
    USING
        (mitt_srl, sent_no)
"""

# In Idaho, if a sentence is amended, a new row is added to the `sentence` table with a sentence disposition of `A` and
# an am_sent_no which points back to the sent_no of the original sentence (that is being amended).
# When this happens, we trust all values of the amended sentence (aside from sentence number and status) over that of
# the original sentence. With that in mind, at a high level, this query gets all sentences of a given type (either
# incarceration or supervision) and collapses all amended-sentence rows into their referenced sentence. If a sentence
# has multiple amendments, the last amendment is trusted over all others.
UP_TO_DATE_SENTENCE_QUERY_FRAGMENT = """
# Get all sentences, either probation or incarceration
WITH 
sentence AS ({date_bounded_sentence}),
sentprob AS ({date_bounded_sentprob}),
mittimus AS ({date_bounded_mittimus}),
county AS ({date_bounded_county}),
judge AS ({date_bounded_judge}),
offense AS ({date_bounded_offense}),
relevant_sentences AS ({sentence_query}),
# Only non-amended sentences
non_amended_sentences AS (
    SELECT
        * 
      FROM
      relevant_sentences
    WHERE sent_disp != 'A'
),
# Only amended sentences with an added amendment_number.
amended_sentences AS (
    SELECT
        *,
        ROW_NUMBER() 
          OVER (PARTITION BY mitt_srl, am_sent_no ORDER BY sent_no DESC) AS amendment_number    
      FROM
      relevant_sentences
    WHERE sent_disp = 'A'
),
# Keep only the most recent amended sentence. We assume this has the most up to date information.
most_recent_amended_sentences AS (
    SELECT
        *
    EXCEPT 
        (amendment_number)
    FROM 
      amended_sentences
    WHERE 
        amendment_number = 1
),
# Join the amended sentences with the non-amended sentences. Only keep sentence number and status information from the 
# non-amended sentence.
up_to_date_sentences AS (
    SELECT 
        {sentence_args}
        (a.mitt_srl IS NOT NULL) AS was_amended
    FROM 
        non_amended_sentences s 
    LEFT JOIN 
        most_recent_amended_sentences a
    ON 
        (s.mitt_srl = a.mitt_srl AND s.sent_no = a.am_sent_no)
)
# Join sentence level information with that from mittimus, county, judge, and offense to get descriptive details.
SELECT
    *
FROM
    mittimus
LEFT JOIN
    county
USING
    (cnty_cd)
# Only ignore jud_cd because already present in `county` table
LEFT JOIN
    (SELECT
            judge_cd,
            judge_name
    FROM
        judge)
USING
    (judge_cd)
JOIN
    up_to_date_sentences
USING
    (mitt_srl)
LEFT JOIN
    offense
USING
    (off_cat, off_cd, off_deg)
ORDER BY
    CAST(docno AS INT64),
    CAST(sent_no AS INT64)
"""

MITTIMUS_JUDGE_SENTENCE_OFFENSE_SENTPROB_SUPERVISION_SENTENCES_QUERY = f"""
{UP_TO_DATE_SENTENCE_QUERY_FRAGMENT.format(project_id=project_id,
                                           sentence_query=PROBATION_SENTENCES_QUERY,
                                           date_bounded_sentence=_create_date_bounded_query('sentence'),
                                           date_bounded_sentprob=_create_date_bounded_query('sentprob'),
                                           date_bounded_mittimus=_create_date_bounded_query('mittimus'),
                                           date_bounded_county=_create_date_bounded_query('county'),
                                           date_bounded_offense=_create_date_bounded_query('offense'),
                                           date_bounded_judge=_create_date_bounded_query('judge'),
                                           sentence_args=create_sentence_query_fragment(add_supervision_args=True))}
"""

MITTIMUS_JUDGE_SENTENCE_OFFENSE_SENTPROB_INCARCERATION_SENTENCES_QUERY = f"""
{UP_TO_DATE_SENTENCE_QUERY_FRAGMENT.format(project_id=project_id,
                                           sentence_query=INCARCERATION_SENTENCES_QUERY,
                                           date_bounded_sentence=_create_date_bounded_query('sentence'),
                                           date_bounded_sentprob=_create_date_bounded_query('sentprob'),
                                           date_bounded_mittimus=_create_date_bounded_query('mittimus'),
                                           date_bounded_county=_create_date_bounded_query('county'),
                                           date_bounded_offense=_create_date_bounded_query('offense'),
                                           date_bounded_judge=_create_date_bounded_query('judge'),
                                           sentence_args=create_sentence_query_fragment(add_supervision_args=False))}
"""


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
          DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M', m.move_dtd)) AS move_dtd,
          LAG(m.fac_cd) 
            OVER (PARTITION BY 
              m.docno,
              m.incrno
              ORDER BY DATETIME(PARSE_TIMESTAMP('%Y-%m-%d %H:%M', m.move_dtd)))
            AS previous_fac_cd,
          LAG(loc.loc_cd) 
            OVER (PARTITION BY 
              m.docno,
              m.incrno
              ORDER BY DATETIME(PARSE_TIMESTAMP('%Y-%m-%d %H:%M', m.move_dtd)))
          AS previous_loc_cd
        FROM 
          movement m
        LEFT JOIN 
           facility f
        USING (fac_cd)
        LEFT JOIN 
           location loc
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
      FROM offstat
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

MOVEMENT_FACILITY_LOCATION_OFFSTAT_INCARCERATION_PERIODS_QUERY = f"""
    WITH 
    movement AS ({_create_date_bounded_query('movement')}),
    facility AS ({_create_date_bounded_query('facility')}),
    location AS ({_create_date_bounded_query('location')}),
    offstat AS ({_create_date_bounded_query('offstat')}),
    {ALL_PERIODS_FRAGMENT}

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

MOVEMENT_FACILITY_LOCATION_OFFSTAT_SUPERVISION_PERIODS_QUERY = f"""
    WITH 
    movement AS ({_create_date_bounded_query('movement')}),
    facility AS ({_create_date_bounded_query('facility')}),
    location AS ({_create_date_bounded_query('location')}),
    offstat AS ({_create_date_bounded_query('offstat')}),
    {ALL_PERIODS_FRAGMENT}

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
        ofndr_tst
      LEFT JOIN 
        tst_qstn_rspns
      USING 
        (ofndr_tst_id, assess_tst_id)
      LEFT JOIN 
        assess_qstn_choice
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
    WITH 
    ofndr_tst AS ({_create_date_bounded_query('ofndr_tst')}),
    tst_qstn_rspns AS ({_create_date_bounded_query('tst_qstn_rspns')}),
    assess_qstn_choice AS ({_create_date_bounded_query('assess_qstn_choice')}),
    {VIOLATION_REPORT_QSTN_NUMS_WITH_DESCRIPTIVE_ANSWERS_FRAGMENT}
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
  WITH 
    ofndr_tst AS ({_create_date_bounded_query('ofndr_tst')}),
    tst_qstn_rspns AS ({_create_date_bounded_query('tst_qstn_rspns')}),
    assess_qstn_choice AS ({_create_date_bounded_query('assess_qstn_choice')}),
    {VIOLATION_REPORT_OLD_QSTN_NUMS_WITH_DESCRIPTIVE_ANSWERS_FRAGMENT}
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

OFNDR_AGNT_APPLC_USR_BODY_LOC_CD_CURRENT_POS_QUERY = """
    # TODO(2999): Integrate PO assignments into supervision query once we have a loss-less table with POs and their 
    #  assignments through history.
    WITH
    ofndr_agnt AS ({_create_date_bounded_query('ofndr_agnt')}),
    applc_usr AS ({_create_date_bounded_query('applc_usr')}),
    # added '_table' extension to differentiate with column 'body_loc_cd' in this table
    body_loc_cd_table AS ({_create_date_bounded_query('body_loc_cd')})
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
      # updt_usr_id,
      # updt_dt,
      lan_id,
      st_id_num,
      body_loc_cd,
      body_loc_desc,
      loc_typ_cd,
      body_loc_cd_id
    FROM 
      ofndr_agnt a
    LEFT JOIN 
      applc_usr u
    ON 
      (agnt_id = usr_id)
    LEFT JOIN 
      body_loc_cd_table
    USING
      (body_loc_cd)
"""

INCARCERATION_SENTENCE_IDS_QUERY = """
      SELECT
          mitt_srl,
          incrno,
          sent_no
      FROM
          sentence
      LEFT JOIN
          mittimus
      USING
          (mitt_srl)
      LEFT JOIN
          sentprob
      USING
          (mitt_srl, sent_no)
      WHERE
          sentprob.mitt_srl IS NULL

"""

PROBATION_SENTENCE_IDS_QUERY = """
      SELECT
          mitt_srl,
          incrno,
          sent_no
      FROM
          sentence
      LEFT JOIN
          mittimus
      USING
          (mitt_srl)
      LEFT JOIN
          sentprob
      USING
          (mitt_srl, sent_no)
      WHERE
          sentprob.mitt_srl IS NOT NULL
"""

EARLY_DISCHARGE_QUERY = """
    WITH 
    early_discharge AS ({date_bounded_early_discharge}),
    early_discharge_form_typ AS ({date_bounded_early_discharge_form_typ}),
    jurisdiction_decision_code AS ({date_bounded_jurisdiction_decision_code}),
    early_discharge_sent AS ({date_bounded_early_discharge_sent}),
    sentence AS ({date_bounded_sentence}),
    mittimus AS ({date_bounded_mittimus}),
    sentprob AS ({date_bounded_sentprob}),
    relevant_sentences AS ({relevant_sentence_query}),
    filtered_early_discharge AS (
      SELECT 
        * 
      EXCEPT (
        jurisdiction_decision_code_id, 
        supervisor_review_date,
        jurisdiction_authize_date)  # Ignore fields which are always NULL.
      FROM 
        early_discharge
    ),
    form_type AS (
      SELECT 
        early_discharge_form_typ_id,
        early_discharge_form_typ_desc,
      FROM 
        early_discharge_form_typ
    ),
    jurisdiction_code AS (
      SELECT
        jurisdiction_decision_code_id,
        jurisdiction_decision_description
      FROM 
       jurisdiction_decision_code
    )
    SELECT 
      *
    FROM 
      early_discharge_sent
    LEFT JOIN 
      filtered_early_discharge
    USING 
      (early_discharge_id)
    LEFT JOIN 
      form_type
    USING 
      (early_discharge_form_typ_id)
    LEFT JOIN 
      jurisdiction_code
    USING 
      (jurisdiction_decision_code_id)
    JOIN 
      relevant_sentences
    USING
      (mitt_srl, sent_no)
    ORDER BY 
      ofndr_num, early_discharge_id

"""


EARLY_DISCHARGE_INCARCERATION_SENTENCE_QUERY = f"""
    {EARLY_DISCHARGE_QUERY.format(
        relevant_sentence_query=INCARCERATION_SENTENCE_IDS_QUERY.format(project_id=project_id),
        date_bounded_early_discharge=_create_date_bounded_query('early_discharge'),
        date_bounded_jurisdiction_decision_code=_create_date_bounded_query('jurisdiction_decision_code'),
        date_bounded_early_discharge_sent=_create_date_bounded_query('early_discharge_sent'),
        date_bounded_early_discharge_form_typ=_create_date_bounded_query('early_discharge_form_typ'),
        date_bounded_sentence=_create_date_bounded_query('sentence'),
        date_bounded_mittimus=_create_date_bounded_query('mittimus'),
        date_bounded_sentprob=_create_date_bounded_query('sentprob'),
        project_id=project_id)}
"""


EARLY_DISCHARGE_SUPERVISION_SENTENCE_QUERY = f"""
    {EARLY_DISCHARGE_QUERY.format(
        relevant_sentence_query=PROBATION_SENTENCE_IDS_QUERY.format(project_id=project_id),
        date_bounded_early_discharge=_create_date_bounded_query('early_discharge'),
        date_bounded_jurisdiction_decision_code=_create_date_bounded_query('jurisdiction_decision_code'),
        date_bounded_early_discharge_sent=_create_date_bounded_query('early_discharge_sent'),
        date_bounded_early_discharge_form_typ=_create_date_bounded_query('early_discharge_form_typ'),
        date_bounded_sentence=_create_date_bounded_query('sentence'),
        date_bounded_mittimus=_create_date_bounded_query('mittimus'),
        date_bounded_sentprob=_create_date_bounded_query('sentprob'),
        project_id=project_id)}
"""


def get_query_name_to_query_list() -> List[Tuple[str, str]]:
    return [
        ('offender_ofndr_dob', OFFENDER_OFNDR_DOB),
        ('ofndr_tst_ofndr_tst_cert', OFNDR_TST_OFNDR_TST_CERT_QUERY),
        ('mittimus_judge_sentence_offense_sentprob_supervision_sentences',
         MITTIMUS_JUDGE_SENTENCE_OFFENSE_SENTPROB_SUPERVISION_SENTENCES_QUERY),
        ('mittimus_judge_sentence_offense_sentprob_incarceration_sentences',
         MITTIMUS_JUDGE_SENTENCE_OFFENSE_SENTPROB_INCARCERATION_SENTENCES_QUERY),
        ('early_discharge_incarceration_sentence', EARLY_DISCHARGE_INCARCERATION_SENTENCE_QUERY),
        ('early_discharge_supervision_sentence', EARLY_DISCHARGE_SUPERVISION_SENTENCE_QUERY),
        ('movement_facility_location_offstat_incarceration_periods',
         MOVEMENT_FACILITY_LOCATION_OFFSTAT_INCARCERATION_PERIODS_QUERY),
        ('movement_facility_location_offstat_supervision_periods',
         MOVEMENT_FACILITY_LOCATION_OFFSTAT_SUPERVISION_PERIODS_QUERY),
        ('ofndr_tst_tst_qstn_rspns_violation_reports', OFNDR_TST_TST_QSTN_RSPNS_VIOLATION_REPORTS_QUERY),
        ('ofndr_tst_tst_qstn_rspns_violation_reports_old', OFNDR_TST_TST_QSTN_RSPNS_VIOLATION_REPORTS_OLD_QUERY),
        ('ofndr_agnt_applc_usr_body_loc_cd_current_pos', OFNDR_AGNT_APPLC_USR_BODY_LOC_CD_CURRENT_POS_QUERY),
    ]


if __name__ == '__main__':
    # Uncomment the os.path clause below (change the directory as desired) if you want the queries to write out to
    # separate files instead of to the console.
    output_dir: Optional[str] = None  # os.path.expanduser('~/Downloads/id_queries')
    output_sql_queries(get_query_name_to_query_list(), output_dir)
