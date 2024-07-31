# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Helpers for parsing script arguments."""

from recidiviz.big_query.big_query_address import BigQueryAddress


def str_to_address_list(list_str: str) -> list[BigQueryAddress]:
    """
    Takes a string representation of BigQuery addresses in the format of
    "dataset_1.table_1,dataset_2.table_2" and returns a list of BigQueryAddress.
    """
    return [BigQueryAddress.from_str(s.strip()) for s in list_str.split(",")]
