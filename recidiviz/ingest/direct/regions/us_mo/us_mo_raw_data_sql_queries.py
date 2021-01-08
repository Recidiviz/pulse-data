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

"""The queries below can be used to generate the raw data tables of Missouri Department
of Corrections data that we export to BQ for pre-processing.
"""

from typing import List, Tuple, Optional

from recidiviz.ingest.direct.query_utils import output_sql_queries


# Y?YYddd e.g. January 1, 2016 --> 116001; November 2, 1982 --> 82306;
julian_format_lower_bound_update_date = 0

# M?MDDYY e.g. January 1, 2016 --> 10116; November 2, 1982 --> 110282;
mmddyy_format_lower_bound_update_date = 0

FOCTEST_ORAS_ASSESSMENTS_WEEKLY = \
    """
    SELECT *
    FROM
        FOCTEST.ORAS_ASSESSMENTS_WEEKLY;
    """

LANTERN_DA_RA_LIST = \
    """
    SELECT *
    FROM
        LANTERN.DA_RA_LIST;
    """

LBAKRCOD_TAK146 = \
    f"""
    SELECT *
    FROM
        LBAKRCOD.TAK146
    WHERE
        MAX(COALESCE(FH$DLU, 0),
            COALESCE(FH$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK001 = \
    f"""
    SELECT *
    FROM
        LBAKRDTA.TAK001
    WHERE
        MAX(COALESCE(EK$DLU, 0),
            COALESCE(EK$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK022 = \
    f"""
    SELECT *
    FROM
        LBAKRDTA.TAK022
    WHERE
        MAX(COALESCE(BS$DLU, 0),
            COALESCE(BS$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK023 = \
    f"""
    SELECT *
    FROM
        LBAKRDTA.TAK023
    WHERE
        MAX(COALESCE(BT$DLU, 0),
            COALESCE(BT$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK024 = \
    f"""
    SELECT *
    FROM
        LBAKRDTA.TAK024
    WHERE
        MAX(COALESCE(BU$DLU, 0),
            COALESCE(BU$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK025 = \
    f"""
    SELECT *
    FROM
        LBAKRDTA.TAK025
    WHERE
        MAX(COALESCE(BV$DLU, 0),
            COALESCE(BV$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK026 = \
    f"""
    SELECT *
    FROM
        LBAKRDTA.TAK026
    WHERE
        MAX(COALESCE(BW$DLU, 0),
            COALESCE(BW$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK028 = \
    f"""
    SELECT *
    FROM
        LBAKRDTA.TAK028
    WHERE
        MAX(COALESCE(BY$DLU, 0),
            COALESCE(BY$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK034 = \
    f"""
    SELECT *
    FROM
        LBAKRDTA.TAK034
    WHERE
        MAX(COALESCE(CE$DLU, 0),
            COALESCE(CE$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK039 = \
    f"""
    SELECT *
    FROM
        LBAKRDTA.TAK039
    WHERE
        MAX(COALESCE(DN$DLU, 0),
            COALESCE(DN$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK040 = \
    f"""
    SELECT *
    FROM
        LBAKRDTA.TAK040
    WHERE
        MAX(COALESCE(DQ$DLU, 0),
            COALESCE(DQ$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK042 = \
    f"""
    SELECT *
    FROM
        LBAKRDTA.TAK042
    WHERE
        MAX(COALESCE(CF$DLU, 0),
            COALESCE(CF$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK076 = \
    f"""
    SELECT *
    FROM
        LBAKRDTA.TAK076
    WHERE
        MAX(COALESCE(CZ$DLU, 0),
            COALESCE(CZ$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK142 = \
    f"""
    SELECT *
    FROM
        LBAKRDTA.TAK142
    WHERE
        MAX(COALESCE(E6$DLU, 0),
            COALESCE(E6$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK158 = \
    """
    SELECT *
    FROM
        LBAKRDTA.TAK158
    """

LBAKRDTA_TAK291 = \
    f"""
    SELECT *
    FROM
        LBAKRDTA.TAK291
    WHERE
        MAX(COALESCE(JS$DLU, 0),
            COALESCE(JS$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK292 = \
    f"""
    SELECT *
    FROM
        LBAKRDTA.TAK292
    WHERE
        MAX(COALESCE(JT$DLU, 0),
            COALESCE(JT$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_VAK003 = \
    f"""
    SELECT *
    FROM
        LBAKRDTA.VAK003
    WHERE
        MAX(COALESCE(CREATE_DT, 0),
            COALESCE(UPDATE_DT, 0)) >= {julian_format_lower_bound_update_date};
    """

LBCMDATA_APFX90 = \
    f"""
    SELECT *
    FROM
        LBCMDATA.APFX90
    WHERE
        MAX(COALESCE(CRTDTE, 0),
            COALESCE(UPDDTE, 0)) >= {mmddyy_format_lower_bound_update_date};
    """

LBCMDATA_APFX91 = \
    f"""
    SELECT *
    FROM
        LBCMDATA.APFX91
    WHERE
        COALESCE(CRTDTE, 0) >= {mmddyy_format_lower_bound_update_date};
    """

OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW = \
    """
    SELECT *
    FROM
        OFNDR_PDB.FOC_SUPERVISION_ENHANCEMENTS_VW;
    """


def get_query_name_to_query_list() -> List[Tuple[str, str]]:
    return [
        ('FOCTEST_ORAS_ASSESSMENTS_WEEKLY', FOCTEST_ORAS_ASSESSMENTS_WEEKLY),
        ('LANTERN_DA_RA_LIST', LANTERN_DA_RA_LIST),
        ('LBAKRCOD_TAK146', LBAKRCOD_TAK146),
        ('LBAKRDTA_TAK001', LBAKRDTA_TAK001),
        ('LBAKRDTA_TAK022', LBAKRDTA_TAK022),
        ('LBAKRDTA_TAK023', LBAKRDTA_TAK023),
        ('LBAKRDTA_TAK024', LBAKRDTA_TAK024),
        ('LBAKRDTA_TAK025', LBAKRDTA_TAK025),
        ('LBAKRDTA_TAK026', LBAKRDTA_TAK026),
        ('LBAKRDTA_TAK028', LBAKRDTA_TAK028),
        ('LBAKRDTA_TAK034', LBAKRDTA_TAK034),
        ('LBAKRDTA_TAK039', LBAKRDTA_TAK039),
        ('LBAKRDTA_TAK040', LBAKRDTA_TAK040),
        ('LBAKRDTA_TAK042', LBAKRDTA_TAK042),
        ('LBAKRDTA_TAK076', LBAKRDTA_TAK076),
        ('LBAKRDTA_TAK142', LBAKRDTA_TAK142),
        ('LBAKRDTA_TAK158', LBAKRDTA_TAK158),
        ('LBAKRDTA_TAK291', LBAKRDTA_TAK291),
        ('LBAKRDTA_TAK292', LBAKRDTA_TAK292),
        ('LBAKRDTA_VAK003', LBAKRDTA_VAK003),
        ('LBCMDATA_APFX90', LBCMDATA_APFX90),
        ('LBCMDATA_APFX91', LBCMDATA_APFX91),
        ('OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW', OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW),
    ]


if __name__ == '__main__':
    # Uncomment the os.path clause below (change the directory as desired) if you want the queries to write out to
    # separate files instead of to the console.
    output_dir: Optional[str] = None  # os.path.expanduser('~/Downloads/mo_queries')
    output_sql_queries(get_query_name_to_query_list(), output_dir)
