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

"""Shared helper fragments for the US_AR ingest view queries."""
SENTENCECOMPUTE_CLEANED_FRAGMENT = """
    sc_prison_only AS (
        SELECT *
        FROM {SENTENCECOMPUTE}
        WHERE MAXSENTDAYSMR != '0'
    )
"""
SUPVTIMELINE_CLEANED_FRAGMENT = """
    st_cleaned AS (
        SELECT *
        FROM (
            SELECT *
            FROM (
                SELECT 
                    st.OFFENDERID,
                    st.COMMITMENTPREFIX,
                    st.SENTENCECOMPONENT,
                    -- All columns that can accept the magic dates meaning NULL ('1000-' and
                    -- '9999-' dates) are cleaned by setting these dates to NULL.
                    CASE 
                        WHEN st.SUPVSTARTDATE LIKE '1000%' OR st.SUPVSTARTDATE LIKE '9999%' THEN NULL
                        ELSE st.SUPVSTARTDATE
                    END AS SUPVSTARTDATE,
                    CASE 
                        WHEN st.SUPVPERIODBEGINDATE LIKE '1000%' OR st.SUPVPERIODBEGINDATE LIKE '9999%' THEN NULL
                        ELSE st.SUPVPERIODBEGINDATE
                    END AS SUPVPERIODBEGINDATE,
                    CASE 
                        WHEN st.SUPVTERMDATE LIKE '1000%' OR st.SUPVTERMDATE LIKE '9999%' THEN NULL
                        ELSE st.SUPVTERMDATE
                    END AS SUPVTERMDATE,
                    CASE 
                        WHEN st.MINSUPVTERMDATE LIKE '1000%' OR st.MINSUPVTERMDATE LIKE '9999%' THEN NULL
                        ELSE st.MINSUPVTERMDATE
                    END AS MINSUPVTERMDATE,
                    -- To simplify logic in ingest views using SUPVTIMELINE, we convert the
                    -- sentence length columns into a single column expressed in days. This
                    -- approach involves using the average number of days in a month, and
                    -- then converting the total to an integer using the floor; this doesn't
                    -- always exactly line up with AR's calculations, but they don't appear
                    -- to be using a calendar-based approach either. This approach should
                    -- be fine for now, since it's rarely more than a day off from AR's dates.
                    CAST(
                        FLOOR(
                            (CAST(st.LENGTHPAROLEYEAR AS INT64) * 365) +
                            (CAST(st.LENGTHPAROLEMONTH AS INT64) * 30.437) +
                            (CAST(st.LENGTHPAROLEDAY AS INT64))
                        ) AS INT64
                    ) AS LENGTHPAROLE,
                    CAST(
                    FLOOR(
                        (CAST(st.LENGTHPROBYEAR AS INT64) * 365) +
                        (CAST(st.LENGTHPROBMONTH AS INT64) * 30.437) +
                        (CAST(st.LENGTHPROBDAY AS INT64))
                        ) AS INT64
                    ) AS LENGTHPROB,
                    st.SUPVTIMESTATUSFLAG
                FROM {SUPVTIMELINE} st
                INNER JOIN {SENTENCECOMPONENT} sc
                USING(OFFENDERID,COMMITMENTPREFIX,SENTENCECOMPONENT)
                -- We don't ingest sentences that are missing an imposed date, so we never
                -- want to look at these sentences when dealing with SUPVTIMELINE.
                WHERE sc.SENTENCEIMPOSEDDATE != '1000-01-01 00:00:00' 
                /*
                SUPVTIMELINE is structured such that the relevant sentencing dates should
                stay the same over each unique SUPVPERIODBEGINDATE within a sentence, since
                SUPVPERIODBEGINDATE is the date on which those sentencing dates became true.
                Therefore, we don't need to look at each individual record with the same 
                SUPVPERIODBEGINDATE for a sentence (new records will be created with different 
                SUPVSTARTDATE values whenever a sentence is stopped and then resumed). Here, 
                we just take the most record with the most recent SUPVSTARTDATE for each 
                SUPVPERIODBEGINDATE within a sentence.
                */
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY OFFENDERID,COMMITMENTPREFIX,SENTENCECOMPONENT,SUPVPERIODBEGINDATE
                    ORDER BY SUPVSTARTDATE DESC NULLS LAST
                ) = 1
            )
            /*
            In the supervision_sentence_length view, there's documentation about how SUPVTIMELINE
            records can be created for a sentence component that's already been completed.
            Here, we filter these records out; this includes records with a SUPVTERMDATE on
            or after the SUPVPERIODBEGINDATE (treated as the update date for a supervision
            sentence length record), and also records that are clearly entered after a sentence
            has already ended based on the sentence status. Any records for a sentence that
            follow a record with a 'complete' or 'vacated' status are assumed to be system-generated
            artifacts and filtered out. These cases usually overlap with the other condition 
            looking at SUPVTERMDATE, but we include both conditions for thoroughness.
            */
            QUALIFY 
                COALESCE(
                    LAG(SUPVTIMESTATUSFLAG) OVER (
                        PARTITION BY OFFENDERID,COMMITMENTPREFIX,SENTENCECOMPONENT 
                        ORDER BY SUPVPERIODBEGINDATE
                    ),
                    'NA'
                ) 
                NOT IN (
                    '9', -- Complete
                    'V' -- Vacated
                )
            ) 
        WHERE SUPVTERMDATE >= SUPVPERIODBEGINDATE
    )
"""

ALL_SENTENCES_FRAGMENT = f"""
    -- SENTENCECOMPONENT needs some slight processing so that we can later determine the
    -- parent sentence ID array. Here, we calculate the prison sentence in days for each
    -- component, so that we can determine which of the concurrent sentence components in
    -- a commitment is 'controlling'.
    cleaned_component AS (
        SELECT 
            -- If a sentence component has concurrent chaining, and is the longest among
            -- non-consecutive components in the commitment, it's treated as the controlling 
            -- concurrent sentence (CONTROLLING_CC = True).
            *,
            MAXPRISONTERMD_ACTUAL = MAXPRISONTERMD_NON_CS AND SENTENCECHAINTYPE != "CS" AS CONTROLLING_CC
        FROM (
            -- The following subqueries give us each sentence component's actual length in days.
            -- Here, we take the maximum sentence length over each commitment (among non-consecutive components).
            SELECT 
            *,
            MAX(
                CASE WHEN SENTENCECHAINTYPE != "CS" THEN MAXPRISONTERMD_ACTUAL ELSE 0 END
            ) OVER (
                PARTITION BY OFFENDERID, COMMITMENTPREFIX
            ) AS MAXPRISONTERMD_NON_CS
            FROM (
            SELECT 
                * EXCEPT(MAXPRISONTERMD_PER_COUNT),
                CASE 
                    /*
                    In the following subquery, we get the sentence length in days, but this 
                    is not always the total sentence length. If MULTICNTCHAINING = 'CS',
                    then this value will actually be the number of days to be served per count
                    in the sentence, so in these cases we multiply MAXPRISONTERMD_PER_COUNT
                    by the number of counts. Note the distinction between MULTICNTCHAINING
                    and SENTENCECHAINTYPE: the former determines how an individual sentence's
                    length is calculated, whereas the latter determines how the sentence
                    is sequenced with other sentences in the commitment.
                    */
                    WHEN MULTICNTCHAINING = 'CS' THEN MAXPRISONTERMD_PER_COUNT * CAST(NUMBERCOUNTS AS INT64) 
                    ELSE MAXPRISONTERMD_PER_COUNT 
                END AS MAXPRISONTERMD_ACTUAL
            FROM (
                SELECT 
                *,
                -- Convert sentence length to a single integer value (expressed as a number of 
                -- days) using the MAXPRISONTERM columns.
                FLOOR(
                    (CAST(MAXPRISONTERMY AS INT64)*365) +
                    (CAST(MAXPRISONTERMM AS INT64)*30.437) +
                    (CAST(MAXPRISONTERMD AS INT64))
                ) AS MAXPRISONTERMD_PER_COUNT
                FROM {{SENTENCECOMPONENT}}
            )
            )
        )
    ),
    -- Use the cleaned version of SENTENCECOMPUTE. More documentation in the fragment itself.
    {SENTENCECOMPUTE_CLEANED_FRAGMENT},
    /*
    Once the CONTROLLING_CC flag has been added to SENTENCECOMPONENT, we take the relevant
    columns, and join some additional data that'll be used to hydrate fields in state_sentence.
    Note that the tables joined to SENTENCECOMPONENT here are shaped differently: COMMITMENTSUMMARY
    only has one row per commitment, and SENTENCECOMPUTE only has rows for sentence components 
    that a) have prison time, b) have been fully calculated, and c) have been determined by 
    the system to contribute to someone's overall sentence. This means that each component will
    have the same COMMITMENTSUMMARY data as each other component in the parent commitment,
    and not every component will have SENTENCECOMPUTE data.
    */
    prison_prob_sentences AS (
        SELECT 
            component.OFFENDERID,
            component.COMMITMENTPREFIX,
            component.SENTENCECOMPONENT,
            IF(component.SENTENCEIMPOSEDDATE = '1000-01-01 00:00:00', NULL, component.SENTENCEIMPOSEDDATE) AS SENTENCEIMPOSEDDATE,
            component.JAILCREDITS,
            component.SENTENCETYPE,
            component.SENTENCETYPE2,
            component.SENTENCETYPE3,
            component.SENTENCETYPE4,
            component.TIMECOMPFLAG,
            component.COMPSTATUSCODE,
            IF(component.OFFENSEDATE = '1000-01-01 00:00:00', NULL, component.OFFENSEDATE) AS OFFENSEDATE,
            component.STATUTE1,
            component.STATUTE2,
            component.STATUTE3,
            component.STATUTE4,
            component.FELONYCLASS,
            component.SERIOUSNESSLEVEL,
            component.NUMBERCOUNTS,
            component.CONTROLLING_CC,
            component.SENTENCECHAINTYPE,

            compute.PAROLEREVOKEDFLAG,
            compute.MRRULINGINDICATOR,

            commitment.TYPEORDER,
            commitment.COUNTYOFCONVICTION,
            commitment.JUDGEPARTYID,
            commitment.COURTID
        FROM cleaned_component component
        LEFT JOIN {{COMMITMENTSUMMARY}} commitment
        USING(OFFENDERID,COMMITMENTPREFIX)
        LEFT JOIN sc_prison_only compute
        ON 
            component.OFFENDERID = compute.OFFENDERID AND 
            component.COMMITMENTPREFIX = compute.COMMITMENTPREFIX AND 
            /*
            The SENTENCECOUNT column in SENTENCECOMPUTE corresponds to the SENTENCECOMPONENT
            column in SENTENCECOMPONENT. As mentioned above, not every sentence component
            has data in SENTENCECOMPUTE, so each commitment will have as many or fewer
            entries in SENTENCECOMPUTE as compared to SENTENCECOMPONENT. Note that SENTENCECOMPUTE
            has a SENTENCESEQUENCE column as well, which does NOT line up with the SENTENCECOMPONENT
            IDs; this column is simply an ordinal sequence number (though it's formatted
            the same way which can cause confusion). Also note that we have to be careful
            with join conditions when we deal with SENTENCECOMPONENT and SENTENCECOMPUTE,
            since SENTENCECOUNT is actually a column in SENTENCECOMPONENT as well; its purpose
            is unknown and it doesn't appear to be used.
            */
            component.SENTENCECOMPONENT = compute.SENTENCECOUNT
    ),
    -- Use the cleaned version of SUPVTIMELINE, documented in the fragment itself.
    {SUPVTIMELINE_CLEANED_FRAGMENT},
    /*
    Like most states, AR doesn't treat parole actions as independent sentences, but rather
    as parts of prison sentences (meaning they don't have their own sentence components: parole
    information can be found in certain columns for the prison sentence component someone is paroled
    from). Because we want each sentence entity to have a single sentence type, we break these out
    into separate rows. 

    Parole information in SENTENCECOMPONENT is system-generated upon sentencing and not fully
    reliable; every sentence containing prison time has parole dates calculated and automatically 
    added to the sentence component's row, but not every prison sentence is eligible for parole
    (for instance, probation-plus sentences contain prison time, but don't include parole).
    Therefore, we get parole sentence data from SUPVTIMELINE, which only includes supervision
    data for parole terms that have actually started. SUPVTIMELINE entries for parolees will have
    the same identifying columns (OFFENDERID, COMMITMENTPREFIX, and SENTENCECOMPONENT) as
    the prison sentence they're attached to. 
    */
    parole_sentences AS (
        SELECT 
            OFFENDERID,
            COMMITMENTPREFIX,
            SENTENCECOMPONENT,
            SUPVPERIODBEGINDATE
        FROM st_cleaned
        WHERE LENGTHPAROLE != 0
        -- The only data we need from SUPVTIMELINE for these sentences is SUPVPERIODBEGINDATE,
        -- which will be used as the date parole was imposed. A parole sentence can have
        -- multiple rows with different SUPVPERIODBEGINDATE values if the sentence becomes
        -- inactive and then gets reactivated. In this QUALIFY, we just take the row with
        -- the earliest SUPVPERIODBEGINDATE, since this is the date that the parole sentence
        -- was originally imposed.
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY OFFENDERID,COMMITMENTPREFIX,SENTENCECOMPONENT 
            ORDER BY SUPVPERIODBEGINDATE
        ) = 1
    ),
    /*
    Some commitments involve both probation and prison time, which can show up one of two ways:

    - 1. The commitment has one or more components with only probation time, and one or
    more components with only prison time.
    - 2. The commitment has one or more components with both probation AND prison time. This
    generally represents a Probation Plus sentence, which is how we'll refer to this case
    in the ingest view (even though there are some rare cases where these components aren't
    designated as Probation Plus in the data).

    The first case doesn't need to be addressed in the ingest view query, since each component
    will naturally be ingested as a separate sentence. The second case, however, needs to 
    be handled the same way parole sentences are in the previous CTE: we need to disentangle
    the probation element of a component from the prison component so they can be ingested
    as separate sentences with the correct sentence type.

    In this CTE, we take a similar approach for components containing probation time as we did
    for components containing parole time in the parole_sentences CTE; however, once we
    pull probation sentences from SUPVTIMELINE, we do an inner join for each component against 
    SENTENCECOMPUTE, ensuring that this CTE only picks up probation sentence components that
    also include actual prison time. If we didn't do this inner join, we'd double count
    the probation sentences that show up as their own components in SENTENCECOMPONENT. 

    Note that we use this join on SENTENCECOMPUTE rather than a WHERE within SENTENCECOMPONENT,
    because a component showing up in SENTENCECOMPUTE guarantees that the component carries
    prison time, whereas the SENTENCETYPE columns and the MAXPRISONTERM columns in SENTENCECOMPONENT
    are less reliable and may be subject to change if a sentence is adjusted.
    */
    probation_plus_sentences AS (
        SELECT 
            st.OFFENDERID,
            st.COMMITMENTPREFIX,
            st.SENTENCECOMPONENT,
            st.SUPVPERIODBEGINDATE
        FROM st_cleaned st
        INNER JOIN sc_prison_only sc
        ON st.OFFENDERID = sc.OFFENDERID AND
            st.COMMITMENTPREFIX = sc.COMMITMENTPREFIX AND
            st.SENTENCECOMPONENT = sc.SENTENCECOUNT
        WHERE st.LENGTHPROB != 0
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY st.OFFENDERID,st.COMMITMENTPREFIX,st.SENTENCECOMPONENT 
            ORDER BY st.SUPVPERIODBEGINDATE
        ) = 1
    ),
    /*
    Using the previous CTEs, we can now get a list of all the sentences that need to be 
    ingested by unioning the data from SENTENCECOMPONENT (which includes pure prison sentences,
    pure probation sentences, and prison sentences with parole or Probation Plus elements; this
    last group will be ingested as prison sentences, since the supervision elements have been
    split off into their own sentences) with data from the parole_sentences and probation_plus_sentences
    CTEs. Data from these CTEs will be identical to the data from the parent prison sentence
    component, but the sentence_category will include a 'PAROLE' or 'PROBPLUS' flag, and the
    relevant imposed date for the supervision portion of the sentence will be specified.
    */
    all_sentences AS (
        SELECT 
            *, 
            CAST(NULL AS STRING) AS parole_imposed_date,
            CAST(NULL AS STRING) AS prob_plus_imposed_date,
            NULL AS sentence_category
        FROM prison_prob_sentences
        WHERE SENTENCEIMPOSEDDATE IS NOT NULL
        UNION ALL (
            -- This subquery will return a copy of prison_prob_sentences, but only contains
            -- components that have a parole_imposed_date value identified by the parole_sentences
            -- CTE. The sentences here can be distinguished from their parent sentence components
            -- by the sentence_category column (which will be 'PAROLE') and by the parole_imposed_date
            -- value (which will be non-null).
            SELECT *
            FROM (
            SELECT 
                prison_prob.*,
                parole.SUPVPERIODBEGINDATE AS parole_imposed_date,
                CAST(NULL AS STRING) AS prob_plus_imposed_date,
                "PAROLE" AS sentence_category
            FROM prison_prob_sentences prison_prob
            LEFT JOIN parole_sentences parole
            USING(OFFENDERID,COMMITMENTPREFIX,SENTENCECOMPONENT)
            ) WHERE parole_imposed_date IS NOT NULL
        )
        UNION ALL (
            -- This subquery will return a copy of prison_prob_sentences, but only contains
            -- components that have a prob_plus_imposed_date value identified by the probation_plus_sentences
            -- CTE. The sentences here can be distinguished from their parent sentence components
            -- by the sentence_category column (which will be 'PROBPLUS') and by the prob_plus_imposed_date
            -- value (which will be non-null).
            SELECT *
            FROM (
            SELECT 
                prison_prob.*,
                CAST(NULL AS STRING) AS parole_imposed_date,
                prob_plus.SUPVPERIODBEGINDATE AS prob_plus_imposed_date,
                "PROBPLUS" AS sentence_category
            FROM prison_prob_sentences prison_prob
            LEFT JOIN probation_plus_sentences prob_plus
            USING(OFFENDERID,COMMITMENTPREFIX,SENTENCECOMPONENT)
            ) WHERE prob_plus_imposed_date IS NOT NULL
        )
    ),
    -- Append a CS_ID_ARRAY column which will be used to hydrate parent_sentence_external_id_array.
    consecutive_id_arrays AS (
        SELECT * 
        FROM (
            -- Among each commitment, look only at sentence components that are either consecutive 
            -- or non-consecutive but controlling, and concatenate the IDs from all previous
            -- sentence components in the commitment that meet these criteria into a single
            -- CS_ID_ARRAY value. We only care about doing this for consecutive components,
            -- but controlling concurrent sentences need to be included in the window.
            SELECT 
            *,
            STRING_AGG(
                CASE 
                    WHEN sentence_category IS NOT NULL THEN CONCAT(OFFENDERID,'-',COMMITMENTPREFIX,'-',SENTENCECOMPONENT,'-',sentence_category)
                    ELSE CONCAT(OFFENDERID,'-',COMMITMENTPREFIX,'-',SENTENCECOMPONENT)
                END,
                ','
            ) OVER (
                PARTITION BY OFFENDERID,COMMITMENTPREFIX 
                ORDER BY SENTENCECOMPONENT 
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS CS_ID_ARRAY 
            FROM all_sentences 
            WHERE sentence_category IS NULL AND (
                CONTROLLING_CC OR SENTENCECHAINTYPE = 'CS'
            )
        ) 
        -- Once we've generated the CS_ID_ARRAY columns, we can filter this CTE's output
        -- to only include consecutive sentences, since controlling concurrent sentences
        -- are relevant for the CS_ID_ARRAY but don't have parent sentences themselves.
        WHERE SENTENCECHAINTYPE = 'CS'
    ),
    -- Join all_sentences with consecutive_id_arrays so that consecutive sentence components
    -- in all_sentences get data in the CS_ID_ARRAY column.
    all_sentences_with_parents AS (
        SELECT 
            cc.* EXCEPT(CONTROLLING_CC,SENTENCECHAINTYPE),
            cia.CS_ID_ARRAY
        FROM all_sentences cc 
        LEFT JOIN consecutive_id_arrays cia
        ON cc.OFFENDERID = cia.OFFENDERID AND
            cc.COMMITMENTPREFIX = cia.COMMITMENTPREFIX AND
            cc.SENTENCECOMPONENT = cia.SENTENCECOMPONENT AND
            IFNULL(cc.sentence_category,'NONE') = IFNULL(cia.sentence_category,'NONE')
    )
"""

SUPERVISION_SENTENCE_LENGTHS_FRAGMENT = f"""
    -- Use the cleaned version of SENTENCECOMPUTE. More documentation in the fragment itself.
    {SENTENCECOMPUTE_CLEANED_FRAGMENT},
    -- Use the cleaned version of SUPVTIMELINE. More documentation in the fragment itself.
    {SUPVTIMELINE_CLEANED_FRAGMENT},
    -- Split components containing both probation and parole into separate rows.
    st_split AS (
        SELECT *
        FROM (
            SELECT 
                * EXCEPT(SUPVTIMESTATUSFLAG),
                'PAROLE' AS sentence_type,
                LENGTHPAROLE AS sentence_length,
                LENGTHPAROLE >= LENGTHPROB AS is_controlling
            FROM st_cleaned 
            WHERE LENGTHPAROLE != 0
        ) UNION ALL (
            SELECT 
                * EXCEPT(SUPVTIMESTATUSFLAG),
                'PROBATION' AS sentence_type,
                LENGTHPROB AS sentence_length,
                LENGTHPROB >= LENGTHPAROLE AS is_controlling
            FROM st_cleaned 
            WHERE LENGTHPROB != 0
        )
    ),
    -- Get a historical ledger of sentence lengths, which is essentially just SUPVTERMDATE and 
    -- MINSUPVTERMDATE from SUPVTIMELINE, with a little bit of extra logic to handle the rare
    -- components with concurrent parole and probation time.
    sentence_lengths AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY 
                    OFFENDERID,
                    COMMITMENTPREFIX,
                    SENTENCECOMPONENT,
                    sentence_type
                ORDER BY SUPVPERIODBEGINDATE
            ) AS seq_num
        FROM (
            SELECT 
                OFFENDERID,
                COMMITMENTPREFIX,
                SENTENCECOMPONENT,
                SUPVPERIODBEGINDATE,
                -- SUPVSTARTDATE is used whenever we need to know the actual start date used
                -- for calculating release dates, whereas SUPVPERIODBEGINDATE is used as the
                -- update date for when a sentence's information became active. In some rare
                -- cases, SUPVSTARTDATE is null; we use SUPVPERIODBEGINDATE in these cases to 
                -- infer the start date, since it's our closest approximation (and these values
                -- are generally the same anyway).
                COALESCE(SUPVSTARTDATE,SUPVPERIODBEGINDATE) AS SUPVSTARTDATE,
                sentence_type,
                sentence_length,
                /*
                In most cases, the max end date is simply the SUPVTERMDATE value. However, when 
                hybrid sentences are split into separate probation and parole sentences in the
                previous CTE, the end date columns for these sentences will only apply to the
                longer portion of the sentence, which is 'controlling'. Therefore, to get the
                max end date for non-controlling probation or parole sentences, we have to
                calculate it ourselves using the SUPVSTARTDATE and sentence length. Note the 
                distinction between SUPVSTARTDATE and SUPVPERIODBEGINDATE described earlier;
                SUPVPERIODBEGINDATE representes the date on which the sentence length information
                became true, but SUPVSTARTDATE is the date supervision for the sentence actually
                started, and is therefore used for calculating end dates.
                */
                CASE 
                    WHEN is_controlling THEN SUPVTERMDATE 
                    ELSE CAST(
                    DATETIME_ADD(
                        CAST(SUPVSTARTDATE AS DATETIME), 
                        INTERVAL CAST(sentence_length AS INT64) DAY
                    ) AS STRING
                )
                END AS max_end_date,
                -- We can't calculate min end dates for non-controlling sentences, so they're
                -- left as null for these sentences.
                CASE 
                    WHEN NOT is_controlling THEN CAST(NULL AS STRING)
                    ELSE MINSUPVTERMDATE
                    END AS min_end_date
            FROM st_split
        )
        /*
        In the prison sentence length view logic, we have to account for max dates that are
        earlier than the update date, which can occur due to data entry errors or system-generated
        artifacts (since the data for completed components can get re-computed whenever the other
        components in a commitment change, even though the re-computed data for completed sentences
        doesn't usually show any actual changes). As mentioned earlier, SUPVTIMELINE serves as 
        an analog for RELEASEDATECHANGE, but for supervision rather than prison sentences; as such,
        we see the same artifacts for this table, but with SUPVPERIODBEGINDATE as the update date
        and SUPVTERMDATE as the max release date rather than DATELASTUPDATE and MAXRELEASEDATE.
        We take the same approach for both types of sentence length, simply filtering out records
        that have an update date after the max release date, which are usually trivial anyway and
        would cause errors in ingest.

        A similar WHERE clause is used in the st_cleaned CTE, but needs to be replicated here
        since some max dates are now being calculated using sentence length rather than taken
        from SUPVTERMDATE directly.
        */
        WHERE max_end_date >= SUPVPERIODBEGINDATE
    ),
    /*
    For entity matching to work when this runs alongside the sentence ingest view, we need
    to distinguish probation sentences and Probation-Plus sentences (which are sentence
    components carrying both probation and prison time). Like in the sentences view, we identify
    these by joining against SENTENCECOMPUTE (which has only prison sentence data). Any probation
    sentences that show up in SENTENCECOMPUTE are flagged as 'PROBPLUS'. The other type of sentence
    that can have both prison and supervision time and therefore need special identification are 
    parole sentences, but these have already been flagged.
    */
    sentence_lengths_pp AS (
        SELECT 
                sl.OFFENDERID,
                sl.COMMITMENTPREFIX,
                sl.SENTENCECOMPONENT,
                sl.SUPVPERIODBEGINDATE,
                sl.SUPVSTARTDATE,
                CASE 
                    WHEN sl.sentence_type = 'PROBATION' AND sc.OFFENDERID IS NOT NULL THEN 'PROBPLUS'
                    ELSE sl.sentence_type
                END AS sentence_type,
                sl.sentence_length,
                sl.max_end_date,
                sl.min_end_date,
                sl.seq_num
        FROM sentence_lengths sl
        LEFT JOIN sc_prison_only sc
        ON sl.OFFENDERID = sc.OFFENDERID AND 
            sl.COMMITMENTPREFIX = sc.COMMITMENTPREFIX AND 
            sl.SENTENCECOMPONENT = sc.SENTENCECOUNT
    )
"""
INCARCERATION_SENTENCE_LENGTHS_FRAGMENT = f"""
    /*
    While we use SENTENCECOMPONENT to get the full list of sentence entities, we use SENTENCECOMPUTE
    (and the related table RELEASEDATECHANGE) to ingest sentence length entities for incarceration 
    sentences, since SENTENCECOMPUTE only includes sentence components that include actual 
    prison time to be served. SENTENCECOMPUTE works as follows:

    1. Before the controlling dates for a prison sentence component have been calculated, 
    the record will not appear in SENTENCECOMPUTE.
    2. Once the dates have been calculated (AR refers to this as a sentence being 'computed'),
    a record for the sentence component will show up in SENTENCECOMPUTE with these dates. Note that
    the SENTENCECOMPONENT column in SENTENCECOMPONENT corresponds to the SENTENCECOUNT column in
    SENTENCECOMPUTE, so each sentence component will be uniquely identified by an OFFENDERID,
    COMMITMENTPREFIX, and SENTENCECOUNT ID.
    3. Every time a prison sentence's dates change, it is re-computed. When this happens,
    a record will be created in RELEASEDATECHANGE, which will look like a SENTENCECOMPUTE
    record, with the updated sentence information, and a DATELASTUPDATE showing the date
    the sentence was changed. When this happens, SENTENCECOMPUTE will automatically be updated
    to show the same data. This means that the data in SENTENCECOMPUTE for a given component
    will always be the same as the row in RELEASEDATECHANGE for that component with the 
    most recent DATELASTUPDATE.

    Since RELEASEDATECHANGE has a historical record whereas SENTENCECOMPUTE updates in place,
    we mainly use RELEASEDATECHANGE to construct a sentence length ledger. However, if a 
    sentence never deviates from the dates initially imposed, it won't have any data in 
    RELEASEDATECHANGE. Also, when a sentence changes for the first time, the initial data
    for that sentence will not get a row in either table (since RELEASEDATECHANGE only shows
    what a sentence changes to, not from, and SENTENCECOMPUTE updates in place). In this case,
    we can still identify the two most important dates a sentence was initially imposed with
    using the OLDMRDATE and OLDPEDATE columns. These columns do not change for a given component,
    and will always show the first-computed max release and parole eligibility dates for a sentence.

    With this in mind, sentence length needs to be ingested from 3 sources:

    1. SENTENCECOMPUTE records, which will show the most up-to-date dates for a component.
    These should be the same as the most recent record in RELEASEDATECHANGE for a component,
    but unlike RELEASEDATECHANGE, this will cover dates for sentences that have stayed the same.
    2. RELEASEDATECHANGE records (to cover the case where a sentence's length changes as it's being served)
    3. OLDMRDATE and OLDPEDATE data from SENTENCECOMPUTE (to cover the initial dates for a
    sentence that changes)

    Data from these sources will be unioned together and then deduped so that each component
    only has one row per update date. The 3 sources listed above are each assigned a dedup
    priority, ranked in the order they're listed.
    */

    -- Source 1: SENTENCECOMPUTE records
    -- Use the cleaned version of SENTENCECOMPUTE. More documentation in the fragment itself.
    {SENTENCECOMPUTE_CLEANED_FRAGMENT},
    -- Get the relevant dates from the cleaned version of SENTENCECOMPUTE.
    current_dates AS (
        SELECT * 
        FROM (
            SELECT 
                *,
                CAST(CAST(rd_update_datetime AS DATETIME) AS DATE) AS rd_update_date
            FROM (
            SELECT 
                OFFENDERID,
                COMMITMENTPREFIX,
                SENTENCECOUNT,
                DATELASTUPDATE AS rd_update_datetime,
                NETGTBEFOREPE,
                CAST(CAST(CREDITSPROJECTED AS INT64) + CAST(CREDITSPROJECTEDSED AS INT64) AS STRING) AS CREDITSPROJECTED,
                CASE 
                    -- Both types of magic dates are set to null, but note the conceptual distinction:
                    -- dates starting with 1000 are missing or have not been calculated for some reason,
                    -- while dates starting with 9999 represent dates that are intentionally absent,
                    -- usually due to life sentences. This is a data entry standard, not 
                    -- system-enforced rule, so there are some exceptions.
                    WHEN MINIMUMRELEASEDATE LIKE '1000%' OR MINIMUMRELEASEDATE LIKE '9999%' THEN NULL
                    ELSE MINIMUMRELEASEDATE
                END AS MINIMUMRELEASEDATE,
                CASE 
                    WHEN MAXRELEASEDATE LIKE '1000%' OR MAXRELEASEDATE LIKE '9999%' THEN NULL
                    ELSE MAXRELEASEDATE
                END AS MAXRELEASEDATE,
                CASE 
                    WHEN PAROLEELIGIBILITYDATE LIKE '1000%' OR PAROLEELIGIBILITYDATE LIKE '9999%' THEN NULL
                    ELSE PAROLEELIGIBILITYDATE
                END AS PAROLEELIGIBILITYDATE,
                CASE 
                    WHEN TIMESTARTDATE LIKE '1000%' OR TIMESTARTDATE LIKE '9999%' THEN NULL
                    ELSE TIMESTARTDATE
                END AS TIMESTARTDATE,
                SENTENCESTATUSFLAG,
                1 AS source_rank
            FROM sc_prison_only
            )
        ) 
    ),
    -- Source 2: RELEASEDATECHANGE records
    rdc_cleaned AS (
        SELECT * 
        FROM (
            SELECT 
                *,
                CAST(CAST(rd_update_datetime AS DATETIME) AS DATE) AS rd_update_date,
            FROM (
                SELECT 
                    OFFENDERID,
                    COMMITMENTPREFIX,
                    SENTENCECOUNT,
                    CONCAT(SPLIT(DATELASTUPDATE, ' ')[OFFSET(0)],' ',TIMELASTUPDATE) AS rd_update_datetime,
                    NETGTBEFOREPE,
                    CAST(CAST(CREDITSPROJECTED AS INT64) + CAST(CREDITSPROJECTEDSED AS INT64)AS STRING) AS CREDITSPROJECTED,
                    CASE 
                        WHEN MINIMUMRELEASEDATE LIKE '1000%' OR MINIMUMRELEASEDATE LIKE '9999%' THEN NULL
                        ELSE MINIMUMRELEASEDATE
                    END AS MINIMUMRELEASEDATE,
                    CASE 
                        WHEN MAXRELEASEDATE LIKE '1000%' OR MAXRELEASEDATE LIKE '9999%' THEN NULL
                        ELSE MAXRELEASEDATE
                    END AS MAXRELEASEDATE,
                    CASE 
                        WHEN PAROLEELIGIBILITYDATE LIKE '1000%' OR PAROLEELIGIBILITYDATE LIKE '9999%' THEN NULL
                        ELSE PAROLEELIGIBILITYDATE
                    END AS PAROLEELIGIBILITYDATE,
                    CASE 
                        WHEN OLDMRDATE LIKE '1000%' OR OLDMRDATE LIKE '9999%' THEN NULL
                        ELSE OLDMRDATE
                    END AS OLDMRDATE,
                    CASE 
                        WHEN OLDPEDATE LIKE '1000%' OR OLDPEDATE LIKE '9999%' THEN NULL
                        ELSE OLDPEDATE
                    END AS OLDPEDATE,
                    CASE 
                        WHEN TIMESTARTDATE LIKE '1000%' OR TIMESTARTDATE LIKE '9999%' THEN NULL
                        ELSE TIMESTARTDATE
                    END AS TIMESTARTDATE,
                    SENTENCESTATUSFLAG,
                    2 AS source_rank
                FROM {{RELEASEDATECHANGE}}
            ) 
        ) 
        -- When release dates are changed, RELEASEDATECHANGE will often be updated with several
        -- records on the same day, often within a span of a few seconds. To cut down on
        -- noise and possible system-generated artifacts, we deduplicate data from RELEASEDATECHANGE
        -- by date (NOT datetime) to keep only the final record for a sentence per day. 
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY OFFENDERID, COMMITMENTPREFIX, SENTENCECOUNT, rd_update_date 
            ORDER BY rd_update_datetime DESC) = 1
    ),
    -- Source 3: Initial sentencing dates taken from SENTENCECOMPUTE
    initial_dates AS (
        SELECT 
            compute.OFFENDERID,
            compute.COMMITMENTPREFIX,
            compute.SENTENCECOUNT,
            /*
            When using SENTENCECOMPUTE to get the initial dates for a sentence, we use the 
            sentence imposition date (taken from SENTENCECOMPONENT) as the update date for 
            the sentence length record, since this record covers the dates that were true
            at time of imposition. Note that while some components in SENTENCECOMPONENT
            have no imposition date (SENTENCEIMPOSEDDATE LIKE '1000%'), every sentence 
            component record in SENTENCECOMPUTE corresponds to a SENTENCECOMPONENT record
            with a valid imposition date, meaning that rd_update_datetime for this source's 
            date will never be null.
            */
            component.SENTENCEIMPOSEDDATE AS rd_update_datetime,
            -- OLDMRDATE and OLDPEDATE are the only columns with data about the original 
            -- sentence, so this source doesn't provide data for the minimum release date or
            -- credit columns (setting the credit columns to 0 is fine for this source, 
            -- though, since someone shouldn't have any credits at time of imposition, 
            -- outside of jail credits which are ingested in state_sentence). 
            '0' AS NETGTBEFOREPE,
            '0' AS CREDITSPROJECTED,
            CAST(NULL AS STRING) AS MINIMUMRELEASEDATE,
            -- Since we're getting data about the initial sentencing dates, the 'OLD_' dates
            -- are treated as the maximum release / parole eligibility dates for these records.
            CASE 
                WHEN compute.OLDMRDATE LIKE '1000%' OR compute.OLDMRDATE LIKE '9999%' THEN NULL
                ELSE compute.OLDMRDATE
            END AS MAXRELEASEDATE,
            CASE 
                WHEN compute.OLDPEDATE LIKE '1000%' OR compute.OLDPEDATE LIKE '9999%' THEN NULL
                ELSE compute.OLDPEDATE
            END AS PAROLEELIGIBILITYDATE,
            CASE 
                WHEN compute.TIMESTARTDATE LIKE '1000%' OR compute.TIMESTARTDATE LIKE '9999%' THEN NULL
                ELSE compute.TIMESTARTDATE
            END AS TIMESTARTDATE,
            CAST(NULL AS STRING) AS SENTENCESTATUSFLAG,
            3 as source_rank,
            CAST(CAST(component.SENTENCEIMPOSEDDATE AS DATETIME) AS DATE) AS rd_update_date
        FROM sc_prison_only compute
        LEFT JOIN {{SENTENCECOMPONENT}} component
        ON
            compute.OFFENDERID = component.OFFENDERID AND 
            compute.COMMITMENTPREFIX = component.COMMITMENTPREFIX AND 
            compute.SENTENCECOUNT = component.SENTENCECOMPONENT
    ),
    -- Union the 3 sources together.
    rdc_all AS (
        SELECT * EXCEPT(OLDMRDATE, OLDPEDATE) 
        FROM rdc_cleaned 
        UNION DISTINCT
        SELECT * FROM initial_dates 
        UNION DISTINCT
        SELECT * FROM current_dates
    ),
    /*
    For specificity's sake, we use datetimes as the update date for sentence length entities,
    but first we deduplicate the records from the different sources over a given day by 
    source_rank. So, if the last RELEASEDATECHANGE record occurs on the same day as the 
    SENTENCECOMPUTE record for a component, but has an different timestamp (this shouldn't 
    ever happen, but is possible if sentence records get manually edited), then we'll only 
    keep the SENTENCECOMPUTE record for that day, since SENTENCECOMPUTE is the most authoritative 
    source for current sentencing details. Even though we dedup by day, the datetime from 
    the SENTENCECOMPUTE record would be used for rd_update_datetime.
    */
    sentence_lengths AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY 
                    OFFENDERID, 
                    COMMITMENTPREFIX, 
                    SENTENCECOUNT
                ORDER BY rd_update_datetime
            ) AS seq_num 
        FROM (
            -- Deduplicate to return one sentence length record per day using source_rank.
            SELECT *
            FROM (
                SELECT * EXCEPT(source_rank, rd_update_date) 
                FROM rdc_all
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY OFFENDERID, COMMITMENTPREFIX, SENTENCECOUNT, rd_update_date 
                    ORDER BY source_rank
                ) = 1
            )
            -- RELEASEDATECHANGE records can have an update date past the sentence's expiration.
            -- Sometimes, the system will automatically append rows to this table for a completed
            -- sentence component if another sentence in the commitment changes (these almost
            -- never have actual changes to the completed component's dates, though there
            -- are some exceptions). We filter these rows out, since they're usually trivial,
            -- don't generally reflect reality, and would cause errors in ingest. 
            WHERE rd_update_datetime <= MAXRELEASEDATE OR MAXRELEASEDATE IS NULL
        )
    ),
    /*
    At this point, we have the full historical ledger of incarceration sentence lengths.
    The last step is to append some data from SENTENCECOMPONENT and SENTENCECOMPUTE.
    - TIMECOMPFLAG is used to identify life/LWOP sentences. These sentences are usually also
    identifiable using their release dates, which should start with 9999, but we've nulled
    those out in an earlier step, and because 9999- and 1000- magic dates are sometimes used
    interchangeably, TIMECOMPFLAG is more reliable for this.
    - PAROLEREVOKEDFLAG is used to avoid hydrating parole eligibility dates for sentence
    components that aren't eligible for parole. The system automatically calculates parole
    eligibility dates without checking if someone is actually eligibile, so we can't rely
    on the PE date from SENTENCECOMPUTE or RELEASEDATECHANGE being null.

    Unfortunately, both of these tables (and therefore the TIMECOMPFLAG and PAROLEREVOKED
    flags) are update-in-place. This means that these flags will be correct for the most
    recent sentence length entity, but may have had other values for earlier sentence
    length records that we can't identify since they have been overridden.
    */
    sentence_lengths_life_flags AS (
        SELECT 
            sl.*,
            component.TIMECOMPFLAG,
            compute.PAROLEREVOKEDFLAG
        FROM sentence_lengths sl
        LEFT JOIN {{SENTENCECOMPONENT}} component
        ON 
            sl.OFFENDERID = component.OFFENDERID AND 
            sl.COMMITMENTPREFIX = component.COMMITMENTPREFIX AND 
            sl.SENTENCECOUNT = component.SENTENCECOMPONENT
        LEFT JOIN sc_prison_only compute
        ON 
            sl.OFFENDERID = compute.OFFENDERID AND 
            sl.COMMITMENTPREFIX = compute.COMMITMENTPREFIX AND 
            sl.SENTENCECOUNT = compute.SENTENCECOUNT
    )
"""
