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
"""Constants for datasets / tables containing sentencing product data."""
from recidiviz.big_query.big_query_address import BigQueryAddress

SENTENCING_DATASET: str = "sentencing"

CASE_INSIGHTS_RATES_ADDRESS = BigQueryAddress(
    dataset_id=SENTENCING_DATASET, table_id="case_insights_rates"
)

CASE_INSIGHTS_RATES_SCHEMA = [
    {"name": "state_code", "type": "STRING", "mode": "REQUIRED"},
    {"name": "gender", "type": "STRING", "mode": "REQUIRED"},
    {"name": "assessment_score_bucket_start", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "assessment_score_bucket_end", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "most_severe_description", "type": "STRING", "mode": "REQUIRED"},
    {"name": "recidivism_rollup", "type": "STRING", "mode": "REQUIRED"},
    {"name": "recidivism_num_records", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "recidivism_probation_series", "type": "STRING", "mode": "NULLABLE"},
    {"name": "recidivism_rider_series", "type": "STRING", "mode": "NULLABLE"},
    {"name": "recidivism_term_series", "type": "STRING", "mode": "NULLABLE"},
    {"name": "disposition_num_records", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "disposition_probation_pc", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "disposition_rider_pc", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "disposition_term_pc", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "recidivism_series", "type": "STRING", "mode": "NULLABLE"},
    {"name": "dispositions", "type": "STRING", "mode": "REQUIRED"},
]
