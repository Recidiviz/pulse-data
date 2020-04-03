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

project_name = metadata.project_id()


OFFENDER_OFNDR_DOB = \
    f"""
    -- offender_ofndr_dob
    SELECT
        *
    FROM
        `{project_name}.us_id_raw_data.offender` offender
    LEFT JOIN
        `{project_name}.us_id_raw_data.ofndr_dob` dob
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
        `{project_name}.us_id_raw_data.ofndr_tst`  tst
    LEFT JOIN
        `{project_name}.us_id_raw_data.ofndr_tst_cert` tst_cert
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
        `{project_name}.us_id_raw_data.mittimus`
    LEFT JOIN
        `{project_name}.us_id_raw_data.county`
    USING
        (cnty_cd)
    # Only ignore jud_cd because already present in `county` table
    LEFT JOIN
        (SELECT
                judge_cd,
                judge_name
        FROM
            `{project_name}.us_id_raw_data.judge`)
    USING
        (judge_cd)
    JOIN
        `{project_name}.us_id_raw_data.sentence`
    USING
        (mitt_srl)
    LEFT JOIN
        `{project_name}.us_id_raw_data.offense`
    USING
        (off_cat, off_cd, off_deg)
    # Inner join against sentences that are supervision sentences.
    JOIN
        `{project_name}.us_id_raw_data.sentprob` sentprob
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
        `{project_name}.us_id_raw_data.mittimus`
    LEFT JOIN
        `{project_name}.us_id_raw_data.county`
    USING
        (cnty_cd)
    # Only ignore jud_cd because already present in `county` table
    LEFT JOIN
        (SELECT
                judge_cd,
                judge_name
        FROM
            `{project_name}.us_id_raw_data.judge`)
    USING
        (judge_cd)
    JOIN
        `{project_name}.us_id_raw_data.sentence`
    USING
        (mitt_srl)
    LEFT JOIN
        `{project_name}.us_id_raw_data.offense`
    USING
        (off_cat, off_cd, off_deg)
    JOIN
        # Inner join against sentences that are incarceration sentences.
        (SELECT
            mitt_srl,
            sent_no
        FROM
            `{project_name}.us_id_raw_data.sentence`
        LEFT JOIN
            `{project_name}.us_id_raw_data.sentprob` sentprob
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


def get_query_name_to_query_list() -> List[Tuple[str, str]]:
    return [
        ('offender_ofndr_dob', OFFENDER_OFNDR_DOB),
        ('ofndr_tst_ofndr_tst_cert', OFNDR_TST_OFNDR_TST_CERT_QUERY),
        ('mittimus_judge_sentence_offense_sentprob_supervision_sentences',
         MITTIMUS_JUDGE_SENTENCE_OFFENSE_SENTPROB_SUPERVISION_SENTENCES_QUERY),
        ('mittimus_judge_sentence_offense_sentprob_incarceration_sentences',
         MITTIMUS_JUDGE_SENTENCE_OFFENSE_SENTPROB_INCARCERATION_SENTENCES_QUERY),
    ]


if __name__ == '__main__':
    # Uncomment the os.path clause below (change the directory as desired) if you want the queries to write out to
    # separate files instead of to the console.
    output_dir: Optional[str] = None  # os.path.expanduser('~/Downloads/id_queries')
    output_sql_queries(get_query_name_to_query_list(), output_dir)
