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
from typing import Any, Callable, Dict, List, Literal, Union

from recidiviz.tools.case_triage.common import (
    csv_row_to_etl_client_json,
    parse_nullable_date,
)

FixtureType = Union[
    Literal["clients"], Literal["opportunities"], Literal["client_events"]
]


def csv_row_to_etl_opportunity_json(row: List[str]) -> Dict[str, Any]:
    return {
        "state_code": row[0],
        "supervising_officer_external_id": row[1],
        "person_external_id": row[2],
        "opportunity_type": row[3],
        "opportunity_metadata": json.loads(row[4]),
        "exported_at": parse_nullable_date(row[5]),
    }


def csv_row_to_etl_client_event_json(row: List[str]) -> Dict[str, Any]:
    return {
        "exported_at": parse_nullable_date(row[0]),
        "event_id": row[1],
        "person_external_id": row[2],
        "state_code": row[3],
        "event_type": row[4],
        "event_date": row[5],
        "event_metadata": json.loads(row[6]),
    }


def generate_json_fixtures_from_csv(
    fixture_type: FixtureType, converter_fn: Callable[[List[str]], Dict[str, Any]]
) -> None:
    converted_fixtures = []
    with open(
        f"./recidiviz/tools/case_triage/fixtures/etl_{fixture_type}.csv",
        encoding="utf-8",
    ) as csvfile:
        csv_reader = csv.reader(csvfile)
        for row in csv_reader:
            # Only take entries belonging to the agent SIN, where applicable
            if fixture_type != "client_events" and "SIN" not in row:
                continue
            converted_fixtures.append(converter_fn(row))

    with open(
        f"./recidiviz/case_triage/fixtures/demo_{fixture_type}.json",
        "w",
        encoding="utf-8",
    ) as jsonfile:
        json.dump(converted_fixtures, jsonfile, default=str)


if __name__ == "__main__":
    generate_json_fixtures_from_csv("clients", csv_row_to_etl_client_json)
    generate_json_fixtures_from_csv("opportunities", csv_row_to_etl_opportunity_json)
    generate_json_fixtures_from_csv("client_events", csv_row_to_etl_client_event_json)
