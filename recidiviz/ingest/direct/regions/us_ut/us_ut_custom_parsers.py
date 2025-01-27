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
"""
Custom parsing functions for US_UT. 
Can be referenced in an ingest view manifest like this:

my_enum_field:
  $custom:
    $function: us_ut_custom_parsers.parse_address_line_1
    $args:
      ofndr_num: ofndr_num
      psda_pre_dir: psda_pre_dir
      psda_street: psda_street
      psda_sfx_cd: psda_sfx_cd
      psda_post_dir: psda_post_dir
"""


def _fields_to_string(fields: list[str | None]) -> str:
    # TODO(#37668) Allow returning None here instead of empty string
    # parsing funcs can't return optional values currently
    return " ".join("" if not s else s.strip() for s in fields).strip()


def parse_address_line_1(
    ofndr_num: str,
    psda_pre_dir: str,
    psda_street: str,
    psda_sfx_cd: str,
    psda_post_dir: str,
) -> str:
    result = _fields_to_string(
        [
            psda_pre_dir,
            psda_street,
            psda_sfx_cd,
            psda_post_dir,
        ]
    )
    if not result:
        raise ValueError(f"Expected at least one address component: {ofndr_num=}")
    return result


# TODO(#37668) Allow returning None here instead of empty string
def parse_address_line_2(
    pssa_unit_cd: str,
    pssa_range: str,
) -> str:
    return _fields_to_string(
        [
            pssa_unit_cd,
            pssa_range,
        ]
    )
