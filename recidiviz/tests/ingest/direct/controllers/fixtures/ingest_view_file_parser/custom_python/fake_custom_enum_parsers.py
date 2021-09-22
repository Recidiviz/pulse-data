# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Custom enum parser functions for use in ingest_view_file_parser_test.py."""

from recidiviz.tests.ingest.direct.controllers.fixtures.ingest_view_file_parser.fake_schema.entities import (
    FakeRace,
)


def flip_black_and_white(raw_text: str) -> FakeRace:
    if raw_text == "B":
        return FakeRace.WHITE
    if raw_text == "W":
        return FakeRace.BLACK

    raise ValueError(f"Unexpected raw_text value: [{raw_text}]")


def race_two_parts(raw_text: str) -> FakeRace:
    # No normalization of raw text values before they are passed to this custom parser.
    pt1, pt2 = raw_text.split("$$")
    if pt1 == "B" and pt2 == "x":
        return FakeRace.BLACK
    if pt1 == "W" and pt2 == "y":
        return FakeRace.WHITE

    raise ValueError(f"Unexpected raw_text value: [{raw_text}]")


def enum_parser_bad_arg_name(bad_arg: str) -> FakeRace:
    raise NotImplementedError(f"Shouldn't get here {bad_arg}")


def enum_parser_missing_arg() -> FakeRace:
    raise NotImplementedError("Shouldn't get here")


def enum_parser_extra_arg(raw_text: str, another_arg: bool) -> FakeRace:
    raise NotImplementedError(f"Shouldn't get here {raw_text} {another_arg}")


def enum_parser_bad_arg_type(raw_text: bool) -> FakeRace:
    raise NotImplementedError(f"Shouldn't get here {raw_text}")
