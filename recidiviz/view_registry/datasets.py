# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Dataset references."""
from recidiviz.calculator.query.county.dataset_config import COUNTY_BASE_DATASET
from recidiviz.calculator.query.county.views.vera.vera_view_constants import (
    VERA_DATASET,
)
from recidiviz.calculator.query.operations.dataset_config import OPERATIONS_BASE_DATASET
from recidiviz.calculator.query.state.dataset_config import (
    STATE_BASE_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
    DATAFLOW_METRICS_DATASET,
    COVID_DASHBOARD_REFERENCE_DATASET,
    POPULATION_PROJECTION_OUTPUT_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.datasets.static_data.config import EXTERNAL_REFERENCE_DATASET
from recidiviz.validation.views.dataset_config import EXTERNAL_ACCURACY_DATASET

RAW_TABLE_DATASETS = {
    f"{state_code.value.lower()}_raw_data" for state_code in StateCode
}

LATEST_VIEW_DATASETS = {
    f"{state_code.value.lower()}_raw_data_up_to_date_views" for state_code in StateCode
}

OTHER_SOURCE_TABLE_DATASETS = {
    COUNTY_BASE_DATASET,
    COVID_DASHBOARD_REFERENCE_DATASET,
    DATAFLOW_METRICS_DATASET,
    EXTERNAL_ACCURACY_DATASET,
    EXTERNAL_REFERENCE_DATASET,
    OPERATIONS_BASE_DATASET,
    POPULATION_PROJECTION_OUTPUT_DATASET,
    STATE_BASE_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
    VERA_DATASET,
}

# These datasets should only contain tables that provide the source data for our view graph.
VIEW_SOURCE_TABLE_DATASETS = OTHER_SOURCE_TABLE_DATASETS | RAW_TABLE_DATASETS

RAW_DATA_TABLE_DATASETS_TO_DESCRIPTIONS = {
    f"{state_code.value.lower()}_raw_data": f"Raw data tables from {StateCode.get_state(state_code)}"
    for state_code in StateCode
}

OTHER_SOURCE_TABLE_DATASETS_TO_DESCRIPTIONS = {
    COUNTY_BASE_DATASET: "Ingested county jail data. This dataset is a copy of the jails postgres database.",
    COVID_DASHBOARD_REFERENCE_DATASET: "Reference tables used by the COVID dashboard. Updated manually.",
    DATAFLOW_METRICS_DATASET: "Stores metric output of Dataflow pipeline jobs.",
    EXTERNAL_ACCURACY_DATASET: "Stores data provided by the states that are used in"
    " external validations of state metrics. Requires manual"
    " updates when onboarding a new state to a new external"
    " validation.",
    EXTERNAL_REFERENCE_DATASET: "Stores data gathered from external sources. CSV versions"
    " of tables are committed to our codebase, and updates to"
    " tables are fully managed by Terraform.",
    OPERATIONS_BASE_DATASET: "Internal Recidiviz operations data. This dataset is a"
    " copy of the operations postgres database.",
    POPULATION_PROJECTION_OUTPUT_DATASET: "Stores output of the population projection"
    " simulations.",
    STATE_BASE_DATASET: "Ingested state data. This dataset is a copy of the state"
    " postgres database.",
    STATIC_REFERENCE_TABLES_DATASET: "Reference tables used by various views in BigQuery."
    " May need to be updated manually for new states.",
    VERA_DATASET: "Stores data calculated outside of our codebase by Vera. Used only by Vera.",
}

VIEW_SOURCE_TABLE_DATASETS_TO_DESCRIPTIONS = {
    **RAW_DATA_TABLE_DATASETS_TO_DESCRIPTIONS,
    **OTHER_SOURCE_TABLE_DATASETS_TO_DESCRIPTIONS,
}
