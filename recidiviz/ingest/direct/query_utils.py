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
"""Util functions useful for any region specific SQL queries."""
import os
from typing import List, Tuple, Optional


def output_sql_queries(query_name_to_query_list: List[Tuple[str, str]], dir_path: Optional[str] = None):
    """If |dir_path| is unspecified, prints the provided |query_name_to_query_list| to the console. Otherwise
    writes the provided |query_name_to_query_list| to the specified |dir_path|.
    """
    if not dir_path:
        _print_all_queries_to_console(query_name_to_query_list)
    else:
        _write_all_queries_to_files(dir_path, query_name_to_query_list)


def _write_all_queries_to_files(dir_path: str, query_name_to_query_list: List[Tuple[str, str]]):
    """Writes the provided queries to files in the provided path."""
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)

    for query_name, query_str in query_name_to_query_list:
        with open(os.path.join(dir_path, f'{query_name}.sql'), 'w') as output_path:
            output_path.write(query_str)


def _print_all_queries_to_console(query_name_to_query_list: List[Tuple[str, str]]):
    """Prints all the provided queries onto the console."""
    for query_name, query_str in query_name_to_query_list:
        print(f'\n\n/* {query_name.upper()} */\n')
        print(query_str)
