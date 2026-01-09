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
Script that copies fixutre files to a demo GCP bucket.

uv run python -m recidiviz.tools.insights.fixtures.copy_fixtures_to_demo_bucket
"""
from google.cloud import storage

from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states,
)

storage_client = storage.Client()
bucket = storage_client.bucket("recidiviz-staging-insights-etl-data-demo")
base_dir = "recidiviz/tools/insights/fixtures/"

for state_code in get_outliers_enabled_states():
    for file_name in [
        "supervision_client_events.json",
        "supervision_clients.json",
        "supervision_district_managers.json",
        "supervision_officer_metrics.json",
        "supervision_officer_outlier_status.json",
        "supervision_officer_supervisors.json",
        "supervision_officers.json",
        "metric_benchmarks.json",
    ]:
        blob = bucket.blob(f"{state_code}/{file_name}")
        full_file_name = base_dir + file_name
        blob.upload_from_filename(full_file_name)
        print(f"File {full_file_name} uploaded to {state_code}/{file_name}.")
