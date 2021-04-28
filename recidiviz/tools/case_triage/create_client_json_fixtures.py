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
"""This is a script that takes our CSV fixtures for etl_clients and outputs them to
json for use as demo data by the Case Triage project.

This is highly specific to the current version of the fixtures. The output destination
is intentional, so we do not offer command-line arguments to write to a different directory.

python -m recidiviz.tools.case_triage.create_client_json_fixtures
"""
import csv
import json
from datetime import date
from typing import Any, Callable, Dict, List, Literal, Optional, Union

import dateutil.parser


FixtureType = Union[Literal["clients"], Literal["opportunities"]]


def parse_nullable_date(date_str: str) -> Optional[date]:
    if not date_str:
        return None
    return dateutil.parser.parse(date_str).date()


def csv_row_to_etl_client_json(row: List[str]) -> Dict[str, Any]:
    return {
        "person_external_id": row[1],
        "state_code": row[9],
        "supervising_officer_external_id": row[0],
        "full_name": row[2],
        "gender": row[13],
        "current_address": row[3],
        "birthdate": parse_nullable_date(row[4]),
        "birthdate_inferred_from_age": bool(row[5]),
        "supervision_start_date": parse_nullable_date(row[14]),
        "projected_end_date": parse_nullable_date(row[16]),
        "supervision_type": row[6],
        "case_type": row[7],
        "supervision_level": row[8],
        "employer": row[10],
        "last_known_date_of_employment": parse_nullable_date(row[17]),
        "most_recent_assessment_date": parse_nullable_date(row[11]),
        "assessment_score": int(row[15]),
        "most_recent_face_to_face_date": parse_nullable_date(row[12]),
        "most_recent_home_visit_date": parse_nullable_date(row[18]),
    }


def csv_row_to_etl_opportunity_json(row: List[str]) -> Dict[str, Any]:
    return {
        "state_code": row[0],
        "supervising_officer_external_id": row[1],
        "person_external_id": row[2],
        "opportunity_type": row[3],
        "opportunity_metadata": json.loads(row[4]),
    }


def generate_json_fixtures_from_csv(
    fixture_type: FixtureType, converter_fn: Callable[[List[str]], Dict[str, Any]]
) -> None:
    converted_fixtures = []
    with open(
        f"./recidiviz/tools/case_triage/fixtures/etl_{fixture_type}.csv"
    ) as csvfile:
        csv_reader = csv.reader(csvfile)
        for row in csv_reader:
            if "SIN" not in row:  # Only take entries belonging to the agent SIN
                continue
            converted_fixtures.append(converter_fn(row))

    with open(
        f"./recidiviz/case_triage/fixtures/demo_{fixture_type}.json", "w"
    ) as jsonfile:
        json.dump(converted_fixtures, jsonfile, default=str)


if __name__ == "__main__":
    generate_json_fixtures_from_csv("clients", csv_row_to_etl_client_json)
    generate_json_fixtures_from_csv("opportunities", csv_row_to_etl_opportunity_json)
