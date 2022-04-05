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
from recidiviz.calculator.pipeline.supplemental.dataset_config import (
    SUPPLEMENTAL_DATA_DATASET,
)
from recidiviz.calculator.query.county.dataset_config import COUNTY_BASE_DATASET
from recidiviz.calculator.query.county.views.vera.vera_view_constants import (
    VERA_DATASET,
)
from recidiviz.calculator.query.experiments.dataset_config import (
    CASE_TRIAGE_SEGMENT_DATASET,
)
from recidiviz.calculator.query.operations.dataset_config import OPERATIONS_BASE_DATASET
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_DATA_SCRATCH_SPACE_DATASET,
    COVID_DASHBOARD_REFERENCE_DATASET,
    DATAFLOW_METRICS_DATASET,
    NORMALIZED_STATE_DATASET,
    POPULATION_PROJECTION_OUTPUT_DATASET,
    SENDGRID_EMAIL_DATA_DATASET,
    STATE_BASE_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
    normalized_state_dataset_for_state_code,
)
from recidiviz.case_triage.views.dataset_config import CASE_TRIAGE_FEDERATED_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.datasets.static_data.config import EXTERNAL_REFERENCE_DATASET
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
    raw_tables_dataset_for_region,
)
from recidiviz.validation.views.dataset_config import EXTERNAL_ACCURACY_DATASET

RAW_DATA_TABLE_DATASETS_TO_DESCRIPTIONS = {
    raw_tables_dataset_for_region(
        state_code.value.lower(), sandbox_dataset_prefix=None
    ): f"Raw data tables from {StateCode.get_state(state_code)}"
    for state_code in StateCode
}
RAW_TABLE_DATASETS = set(RAW_DATA_TABLE_DATASETS_TO_DESCRIPTIONS.keys())

LATEST_VIEW_DATASETS = {
    raw_latest_views_dataset_for_region(
        state_code.value.lower(), sandbox_dataset_prefix=None
    )
    for state_code in StateCode
}

SUPPLEMENTAL_DATASETS_TO_DESCRIPTIONS = {
    f"{state_code.value.lower()}_supplemental": f"Contains data provided directly by {StateCode.get_state(state_code)} that is not run through direct ingest, e.g. validation data."
    for state_code in StateCode
}
SUPPLEMENTAL_DATASETS = set(SUPPLEMENTAL_DATASETS_TO_DESCRIPTIONS.keys())

NORMALIZED_DATASETS_TO_DESCRIPTIONS = {
    **{
        normalized_state_dataset_for_state_code(
            state_code
        ): "Contains normalized versions of the entities in the state dataset produced by the normalization pipeline for the state."
        for state_code in StateCode
    },
    **{
        NORMALIZED_STATE_DATASET: "Contains normalized versions of the entities in the "
        "state dataset produced by the normalization pipeline, and copies of non-normalized entities "
        "from the state dataset."
    },
}
NORMALIZED_DATASETS = set(NORMALIZED_DATASETS_TO_DESCRIPTIONS.keys())

OTHER_SOURCE_TABLE_DATASETS_TO_DESCRIPTIONS = {
    ANALYST_DATA_SCRATCH_SPACE_DATASET: "Analyst data scratch space. Contains views for scrappy impact",
    CASE_TRIAGE_FEDERATED_DATASET: "Case Triage data. This dataset is a copy of the case-triage postgres database.",
    CASE_TRIAGE_SEGMENT_DATASET: "Stores metrics about users on case triage",
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
    SENDGRID_EMAIL_DATA_DATASET: "Stores the output of email activity data from Sendgrid.",
    STATE_BASE_DATASET: "Ingested state data. This dataset is a copy of the state"
    " postgres database.",
    STATIC_REFERENCE_TABLES_DATASET: "Reference tables used by various views in BigQuery."
    " May need to be updated manually for new states.",
    SUPPLEMENTAL_DATA_DATASET: "Stores datasets generated not by traditional ingest or calc pipelines in BigQuery.",
    VERA_DATASET: "Stores data calculated outside of our codebase by Vera. Used only by Vera.",
}
OTHER_SOURCE_TABLE_DATASETS = set(OTHER_SOURCE_TABLE_DATASETS_TO_DESCRIPTIONS.keys())

# These datasets should only contain tables that provide the source data for our view graph.
VIEW_SOURCE_TABLE_DATASETS_TO_DESCRIPTIONS = {
    **RAW_DATA_TABLE_DATASETS_TO_DESCRIPTIONS,
    **SUPPLEMENTAL_DATASETS_TO_DESCRIPTIONS,
    **OTHER_SOURCE_TABLE_DATASETS_TO_DESCRIPTIONS,
    **NORMALIZED_DATASETS_TO_DESCRIPTIONS,
}
VIEW_SOURCE_TABLE_DATASETS = set(VIEW_SOURCE_TABLE_DATASETS_TO_DESCRIPTIONS.keys())
