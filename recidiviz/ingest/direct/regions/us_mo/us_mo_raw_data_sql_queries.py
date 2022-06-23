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
import os
from typing import List, Optional, Tuple

# Y?YYddd e.g. January 1, 2016 --> 116001; November 2, 1982 --> 82306;
julian_format_lower_bound_update_date = 0

# M?MDDYY e.g. January 1, 2016 --> 10116; November 2, 1982 --> 110282;
mmddyy_format_lower_bound_update_date = 0

# YYYY-MM-DD e.g. January 1, 2016 --> 2016-01-01; November 2, 1982 --> 1982-11-02;
iso_format_lower_bound_update_date = 0

FOCTEST_ORAS_ASSESSMENTS_WEEKLY = """
    SELECT *
    FROM
        FOCTEST.ORAS_ASSESSMENTS_WEEKLY;
    """

LANTERN_DA_RA_LIST = """
    SELECT *
    FROM
        LANTERN.DA_RA_LIST;
    """

LBAKRCOD_TAK146 = f"""
    SELECT *
    FROM
        LBAKRCOD.TAK146
    WHERE
        MAX(COALESCE(FH$DLU, 0),
            COALESCE(FH$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK001 = f"""
    SELECT *
    FROM
        LBAKRDTA.TAK001
    WHERE
        MAX(COALESCE(EK$DLU, 0),
            COALESCE(EK$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK017 = f"""
    SELECT *
    FROM
        LBAKRDTA.TAK017
    WHERE
        MAX(COALESCE(BN$DLU, 0),
            COALESCE(BN$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK020 = f"""
    SELECT *
    FROM
        LBAKRDTA.TAK020
    WHERE
        MAX(COALESCE(BQ$DLU, 0),
            COALESCE(BQ$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK022 = f"""
    SELECT *
    FROM
        LBAKRDTA.TAK022
    WHERE
        MAX(COALESCE(BS$DLU, 0),
            COALESCE(BS$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK023 = f"""
    SELECT *
    FROM
        LBAKRDTA.TAK023
    WHERE
        MAX(COALESCE(BT$DLU, 0),
            COALESCE(BT$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK024 = f"""
    SELECT *
    FROM
        LBAKRDTA.TAK024
    WHERE
        MAX(COALESCE(BU$DLU, 0),
            COALESCE(BU$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK025 = f"""
    SELECT *
    FROM
        LBAKRDTA.TAK025
    WHERE
        MAX(COALESCE(BV$DLU, 0),
            COALESCE(BV$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK026 = f"""
    SELECT *
    FROM
        LBAKRDTA.TAK026
    WHERE
        MAX(COALESCE(BW$DLU, 0),
            COALESCE(BW$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK028 = f"""
    SELECT *
    FROM
        LBAKRDTA.TAK028
    WHERE
        MAX(COALESCE(BY$DLU, 0),
            COALESCE(BY$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK034 = f"""
    SELECT *
    FROM
        LBAKRDTA.TAK034
    WHERE
        MAX(COALESCE(CE$DLU, 0),
            COALESCE(CE$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK039 = f"""
    SELECT *
    FROM
        LBAKRDTA.TAK039
    WHERE
        MAX(COALESCE(DN$DLU, 0),
            COALESCE(DN$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK040 = f"""
    SELECT *
    FROM
        LBAKRDTA.TAK040
    WHERE
        MAX(COALESCE(DQ$DLU, 0),
            COALESCE(DQ$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK042 = f"""
    SELECT *
    FROM
        LBAKRDTA.TAK042
    WHERE
        MAX(COALESCE(CF$DLU, 0),
            COALESCE(CF$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK065 = f"""
    SELECT *
    FROM
        LBAKRDTA.TAK065
    WHERE
        MAX(COALESCE(CS$DLU, 0),
            COALESCE(CS$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK076 = f"""
    SELECT *
    FROM
        LBAKRDTA.TAK076
    WHERE
        MAX(COALESCE(CZ$DLU, 0),
            COALESCE(CZ$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK090 = f"""
    SELECT *
    FROM
        LBAKRDTA.TAK090
    WHERE
        MAX(COALESCE(DD$DLU, 0),
            COALESCE(DD$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK142 = f"""
    SELECT *
    FROM
        LBAKRDTA.TAK142
    WHERE
        MAX(COALESCE(E6$DLU, 0),
            COALESCE(E6$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK222 = f"""
    SELECT *
    FROM
        LBAKRDTA.TAK222
    WHERE
        MAX(COALESCE(IB$DLU, 0),
            COALESCE(IB$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK223 = f"""
    SELECT *
    FROM
        LBAKRDTA.TAK223
    WHERE
        MAX(COALESCE(IE$DLU, 0),
            COALESCE(IE$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK158 = """
    SELECT *
    FROM
        LBAKRDTA.TAK158;
    """

LBAKRDTA_TAK291 = f"""
    SELECT *
    FROM
        LBAKRDTA.TAK291
    WHERE
        MAX(COALESCE(JS$DLU, 0),
            COALESCE(JS$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK292 = f"""
    SELECT *
    FROM
        LBAKRDTA.TAK292
    WHERE
        MAX(COALESCE(JT$DLU, 0),
            COALESCE(JT$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_VAK003 = f"""
    SELECT *
    FROM
        LBAKRDTA.VAK003
    WHERE
        MAX(COALESCE(CREATE_DT, 0),
            COALESCE(UPDATE_DT, 0)) >= {julian_format_lower_bound_update_date};
    """

LBCMDATA_APFX90 = f"""
    SELECT *
    FROM
        LBCMDATA.APFX90
    WHERE
        MAX(COALESCE(CRTDTE, 0),
            COALESCE(UPDDTE, 0)) >= {mmddyy_format_lower_bound_update_date};
    """

LBCMDATA_APFX91 = f"""
    SELECT *
    FROM
        LBCMDATA.APFX91
    WHERE
        COALESCE(CRTDTE, 0) >= {mmddyy_format_lower_bound_update_date};
    """

OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW = """
    SELECT *
    FROM
        OFNDR_PDB.FOC_SUPERVISION_ENHANCEMENTS_VW;
    """

MO_CASEPLANS_DB2 = """
    SELECT *
    FROM
        ORAS.MO_CASEPLANS_DB2
    WHERE
        COALESCE(CREATED_AT, '1900-01-01') >= {iso_format_lower_bound_update_date};
    """

# Note: for this table without time bounds, there is no date field or ordering field to allow for only new rows to
# be pulled weekly. Therefore, each time we pull the table, it will be a full historical pull. There is effort to add
# in a date column from the MO side, but until that is done, these will not be pulled in the regular weekly upload
# and instead pulled only in a one-off capacity when a refesh of the data is needed. Genevieve will inform Josh
# when to pull these files while she is awaiting MO Warehouse Access and the default will be to NOT pull them
# unless specifically asked to do so.
# TODO(##12970) - Revisit this process once SFTP / Automated transfer process is addressed with MO
MO_CASEPLAN_INFO_DB2 = """
    SELECT *
    FROM
        ORAS.MO_CASEPLAN_INFO_DB2;
    """

# Note: for this table without time bounds, there is no date field or ordering field to allow for only new rows to
# be pulled weekly. Therefore, each time we pull the table, it will be a full historical pull. There is effort to add
# in a date column from the MO side, but until that is done, these will not be pulled in the regular weekly upload
# and instead pulled only in a one-off capacity when a refesh of the data is needed. Genevieve will inform Josh
# when to pull these files while she is awaiting MO Warehouse Access and the default will be to NOT pull them
# unless specifically asked to do so.
# TODO(##12970) - Revisit this process once SFTP / Automated transfer process is addressed with MO
MO_CASEPLAN_TARGETS_DB2 = """
    SELECT *
    FROM
        ORAS.MO_CASEPLAN_TARGETS_DB2;
    """

MO_CASEPLAN_GOALS_DB2 = """
    SELECT *
    FROM
        ORAS.MO_CASEPLAN_GOALS_DB2
    WHERE
        MAX(COALESCE(CREATED_AT, '1900-01-01'),
            COALESCE(CREATED_AT, '1900-01-01')) >= {iso_format_lower_bound_update_date};
    """

# Note: for this table without time bounds, there is no date field or ordering field to allow for only new rows to
# be pulled weekly. Therefore, each time we pull the table, it will be a full historical pull. There is effort to add
# in a date column from the MO side, but until that is done, these will not be pulled in the regular weekly upload
# and instead pulled only in a one-off capacity when a refesh of the data is needed. Genevieve will inform Josh
# when to pull these files while she is awaiting MO Warehouse Access and the default will be to NOT pull them
# unless specifically asked to do so.
# TODO(##12970) - Revisit this process once SFTP / Automated transfer process is addressed with MO
MO_CASEPLAN_OBJECTIVES_DB2 = """
    SELECT *
    FROM
        ORAS.MO_CASEPLAN_OBJECTIVES_DB2;
    """

# Note: for this table without time bounds, there is no date field or ordering field to allow for only new rows to
# be pulled weekly. Therefore, each time we pull the table, it will be a full historical pull. There is effort to add
# in a date column from the MO side, but until that is done, these will not be pulled in the regular weekly upload
# and instead pulled only in a one-off capacity when a refesh of the data is needed. Genevieve will inform Josh
# when to pull these files while she is awaiting MO Warehouse Access and the default will be to NOT pull them
# unless specifically asked to do so.
# TODO(##12970) - Revisit this process once SFTP / Automated transfer process is addressed with MO
MO_CASEPLAN_TECHNIQUES_DB2 = """
    SELECT *
    FROM
        ORAS.MO_CASEPLAN_TECHNIQUES_DB2;
    """


def get_query_name_to_query_list() -> List[Tuple[str, str]]:
    return [
        ("FOCTEST_ORAS_ASSESSMENTS_WEEKLY", FOCTEST_ORAS_ASSESSMENTS_WEEKLY),
        ("LANTERN_DA_RA_LIST", LANTERN_DA_RA_LIST),
        ("LBAKRCOD_TAK146", LBAKRCOD_TAK146),
        ("LBAKRDTA_TAK001", LBAKRDTA_TAK001),
        ("LBAKRDTA_TAK017", LBAKRDTA_TAK017),
        ("LBAKRDTA_TAK020", LBAKRDTA_TAK020),
        ("LBAKRDTA_TAK022", LBAKRDTA_TAK022),
        ("LBAKRDTA_TAK023", LBAKRDTA_TAK023),
        ("LBAKRDTA_TAK024", LBAKRDTA_TAK024),
        ("LBAKRDTA_TAK025", LBAKRDTA_TAK025),
        ("LBAKRDTA_TAK026", LBAKRDTA_TAK026),
        ("LBAKRDTA_TAK028", LBAKRDTA_TAK028),
        ("LBAKRDTA_TAK034", LBAKRDTA_TAK034),
        ("LBAKRDTA_TAK039", LBAKRDTA_TAK039),
        ("LBAKRDTA_TAK040", LBAKRDTA_TAK040),
        ("LBAKRDTA_TAK042", LBAKRDTA_TAK042),
        ("LBAKRDTA_TAK065", LBAKRDTA_TAK065),
        ("LBAKRDTA_TAK076", LBAKRDTA_TAK076),
        ("LBAKRDTA_TAK090", LBAKRDTA_TAK090),
        ("LBAKRDTA_TAK142", LBAKRDTA_TAK142),
        ("LBAKRDTA_TAK158", LBAKRDTA_TAK158),
        ("LBAKRDTA_TAK222", LBAKRDTA_TAK222),
        ("LBAKRDTA_TAK223", LBAKRDTA_TAK223),
        ("LBAKRDTA_TAK291", LBAKRDTA_TAK291),
        ("LBAKRDTA_TAK292", LBAKRDTA_TAK292),
        ("LBAKRDTA_VAK003", LBAKRDTA_VAK003),
        ("LBCMDATA_APFX90", LBCMDATA_APFX90),
        ("LBCMDATA_APFX91", LBCMDATA_APFX91),
        (
            "OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW",
            OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW,
        ),
        # These queries should only be run ad-hoc for now. See above for more details.
        # ("MO_CASEPLANS_DB2", MO_CASEPLANS_DB2),
        # ("MO_CASEPLAN_INFO_DB2", MO_CASEPLAN_INFO_DB2),
        # ("MO_CASEPLAN_TARGETS_DB2", MO_CASEPLAN_TARGETS_DB2),
        # ("MO_CASEPLAN_GOALS_DB2", MO_CASEPLAN_GOALS_DB2),
        # ("MO_CASEPLAN_OBJECTIVES_DB2", MO_CASEPLAN_OBJECTIVES_DB2),
        # ("MO_CASEPLAN_TECHNIQUES_DB2", MO_CASEPLAN_TECHNIQUES_DB2),
    ]


def _output_sql_queries(
    query_name_to_query_list: List[Tuple[str, str]], dir_path: Optional[str] = None
) -> None:
    """If |dir_path| is unspecified, prints the provided |query_name_to_query_list| to the console. Otherwise
    writes the provided |query_name_to_query_list| to the specified |dir_path|.
    """
    if not dir_path:
        _print_all_queries_to_console(query_name_to_query_list)
    else:
        _write_all_queries_to_files(dir_path, query_name_to_query_list)


def _write_all_queries_to_files(
    dir_path: str, query_name_to_query_list: List[Tuple[str, str]]
) -> None:
    """Writes the provided queries to files in the provided path."""
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)

    for query_name, query_str in query_name_to_query_list:
        with open(
            os.path.join(dir_path, f"{query_name}.sql"), "w", encoding="utf-8"
        ) as output_path:
            output_path.write(query_str)


def _print_all_queries_to_console(
    query_name_to_query_list: List[Tuple[str, str]]
) -> None:
    """Prints all the provided queries onto the console."""
    for query_name, query_str in query_name_to_query_list:
        print(f"\n\n/* {query_name.upper()} */\n")
        print(query_str)


if __name__ == "__main__":
    # Uncomment the os.path clause below (change the directory as desired) if you want the queries to write out to
    # separate files instead of to the console.
    output_dir: Optional[str] = None  # os.path.expanduser('~/Downloads/mo_queries')
    _output_sql_queries(get_query_name_to_query_list(), output_dir)
