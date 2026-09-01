#!/usr/bin/env bash
# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
Script that copies fixture files to a demo GCP bucket, one copy per outliers-enabled
state. Before upload, each record's state_code and any pseudonymized_id fields are
rewritten to be accurate for the destination state, since pseudonymized_ids are a hash
of state_code + external_id and the frontend relies on them being correct.

uv run python -m recidiviz.tools.insights.fixtures.copy_fixtures_to_demo_bucket
"""

import json

from google.cloud import storage

from recidiviz.auth.helpers import generate_pseudonymized_id
from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states,
)

STATE_CODE_KEY = "state_code"

# Maps each fixture file name to the (external_id_key, pseudonymized_id_key) pairs it
# contains. generate_pseudonymized_id() hashes state_code + external_id, so these
# pseudonymized_id_key values must be recomputed for each destination state rather
# than copied verbatim from the US_XX source fixture.
PSEUDONYMIZED_ID_FIELDS_BY_FILE_NAME: dict[str, list[tuple[str, str]]] = {
    "supervision_client_events.json": [
        ("client_id", "pseudonymized_client_id"),
        ("officer_id", "pseudonymized_officer_id"),
    ],
    "supervision_clients.json": [("client_id", "pseudonymized_client_id")],
    "supervision_district_managers.json": [],
    "supervision_officer_metrics.json": [],
    "supervision_officer_outlier_status.json": [],
    "supervision_officer_supervisors.json": [("external_id", "pseudonymized_id")],
    "supervision_officers.json": [("external_id", "pseudonymized_id")],
    "metric_benchmarks.json": [],
}


def _rewrite_record_for_state(
    record: dict[str, object],
    *,
    state_code: str,
    pseudonymized_id_fields: list[tuple[str, str]],
) -> dict[str, object]:
    """Returns a copy of record with its state_code and any pseudonymized_id fields
    rewritten so they are accurate for state_code."""
    updated_record = {**record, STATE_CODE_KEY: state_code}
    for external_id_key, pseudonymized_id_key in pseudonymized_id_fields:
        external_id = updated_record[external_id_key]
        if external_id is not None and not isinstance(external_id, str):
            raise ValueError(
                f"Expected str or None for [{external_id_key}], found "
                f"[{type(external_id)}]"
            )
        updated_record[pseudonymized_id_key] = generate_pseudonymized_id(
            state_code, external_id
        )
    return updated_record


def copy_fixtures() -> None:
    storage_client = storage.Client()
    bucket = storage_client.bucket("recidiviz-staging-insights-etl-data-demo")
    base_dir = "recidiviz/tools/insights/fixtures/"

    for state_code in get_outliers_enabled_states():
        for (
            file_name,
            pseudonymized_id_fields,
        ) in PSEUDONYMIZED_ID_FIELDS_BY_FILE_NAME.items():
            full_file_name = base_dir + file_name
            with open(full_file_name, "r", encoding="utf-8") as fixture_file:
                source_lines = [
                    line for line in fixture_file.read().splitlines() if line
                ]

            rewritten_lines = [
                json.dumps(
                    _rewrite_record_for_state(
                        json.loads(line),
                        state_code=state_code,
                        pseudonymized_id_fields=pseudonymized_id_fields,
                    )
                )
                for line in source_lines
            ]

            blob = bucket.blob(f"{state_code}/{file_name}")
            blob.upload_from_string("\n".join(rewritten_lines) + "\n")
            print(f"File {full_file_name} uploaded to {state_code}/{file_name}.")


if __name__ == "__main__":
    copy_fixtures()
