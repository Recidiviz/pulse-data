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


TAK001_OFFENDER_IDENTIFICATION_QUERY = \
    f"""
    -- offender
    SELECT
        *
    FROM
        `{project_name}.us_id_raw_data.offender`
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
    LEFT JOIN `
        recidiviz-staging.us_id_raw_data.ofndr_tst_cert` tst_cert
    USING
        (ofndr_tst_id, assess_tst_id)
    WHERE
        tst.assess_tst_id = '2'  # LSIR assessments
        AND tst_cert.cert_pass_flg = 'Y'  # Test score has been certified
    ORDER BY
        tst.ofndr_num
    """


def get_query_name_to_query_list() -> List[Tuple[str, str]]:
    return [
        ('offender', TAK001_OFFENDER_IDENTIFICATION_QUERY),
        ('ofndr_tst_ofndr_tst_cert', OFNDR_TST_OFNDR_TST_CERT_QUERY),
    ]


if __name__ == '__main__':
    # Uncomment the os.path clause below (change the directory as desired) if you want the queries to write out to
    # separate files instead of to the console.
    output_dir: Optional[str] = None  # os.path.expanduser('~/Downloads/id_queries')
    output_sql_queries(get_query_name_to_query_list(), output_dir)
