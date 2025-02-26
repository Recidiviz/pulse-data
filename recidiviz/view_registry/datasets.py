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
import re

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.success_persister import VIEW_UPDATE_METADATA_DATASET
from recidiviz.calculator.query.experiments.dataset_config import (
    CASE_TRIAGE_SEGMENT_DATASET,
)
from recidiviz.calculator.query.operations.dataset_config import OPERATIONS_BASE_DATASET
from recidiviz.calculator.query.state.dataset_config import (
    COVID_DASHBOARD_REFERENCE_DATASET,
    DATAFLOW_METRICS_DATASET,
    EXPORT_ARCHIVES_DATASET,
    NORMALIZED_STATE_DATASET,
    POPULATION_PROJECTION_OUTPUT_DATASET,
    PULSE_DASHBOARD_SEGMENT_DATASET,
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
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.pipelines.supplemental.dataset_config import SUPPLEMENTAL_DATA_DATASET
from recidiviz.validation.views.dataset_config import (
    validation_dataset_for_state,
    validation_oneoff_dataset_for_state,
)

RAW_DATA_TABLE_DATASETS_TO_DESCRIPTIONS = {
    raw_tables_dataset_for_region(
        state_code=state_code,
        instance=instance,
        sandbox_dataset_prefix=None,
    ): f"Raw data tables from {StateCode.get_state(state_code)}"
    for instance in DirectIngestInstance
    for state_code in StateCode
}
RAW_TABLE_DATASETS = set(RAW_DATA_TABLE_DATASETS_TO_DESCRIPTIONS.keys())

LATEST_VIEW_DATASETS = {
    raw_latest_views_dataset_for_region(
        state_code=state_code,
        instance=instance,
        sandbox_dataset_prefix=None,
    )
    for instance in DirectIngestInstance
    for state_code in StateCode
}

SUPPLEMENTAL_DATASETS_TO_DESCRIPTIONS = {
    f"{state_code.value.lower()}_supplemental": f"Contains data provided directly by {StateCode.get_state(state_code)} that is not run through direct ingest, e.g. validation data."
    for state_code in StateCode
}
SUPPLEMENTAL_DATASETS = set(SUPPLEMENTAL_DATASETS_TO_DESCRIPTIONS.keys())

VALIDATION_DATASETS_TO_DESCRIPTIONS = {
    validation_dataset_for_state(
        state_code
    ): f"Contains one-off validation data provided directly by {StateCode.get_state(state_code)}. "
    "Once this state has been migrated to have version controlled validation views, the one-off "
    f"data will be moved to {validation_oneoff_dataset_for_state(state_code)}."
    for state_code in StateCode
    if state_code
    # TODO(#13312): Move all one off validation data for these states into `us_xx_validation_oneoffs`
    # and have `us_xx_validation` only contain version controlled views pulling from oneoffs and raw data.
    in (
        StateCode.US_ID,
        StateCode.US_ME,
        StateCode.US_MO,
        StateCode.US_ND,
        StateCode.US_PA,
        # Note: Some pre-existing migration work exists for TN in #13641.
        StateCode.US_TN,
    )
}
VALIDATION_ONEOFF_DATASETS_TO_DESCRIPTIONS = {
    validation_oneoff_dataset_for_state(
        state_code
    ): f"Contains one-off validation data provided directed by {StateCode.get_state(state_code)}."
    for state_code in StateCode
}
VALIDATION_DATASETS = set(VALIDATION_DATASETS_TO_DESCRIPTIONS.keys()).union(
    set(VALIDATION_ONEOFF_DATASETS_TO_DESCRIPTIONS.keys())
)

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

VERA_DATASET: str = "vera_data"

OTHER_SOURCE_TABLE_DATASETS_TO_DESCRIPTIONS = {
    CASE_TRIAGE_FEDERATED_DATASET: "Case Triage data. This dataset is a copy of the case-triage postgres database.",
    CASE_TRIAGE_SEGMENT_DATASET: "Stores metrics about users on case triage",
    COVID_DASHBOARD_REFERENCE_DATASET: "Reference tables used by the COVID dashboard. Updated manually.",
    DATAFLOW_METRICS_DATASET: "Stores metric output of Dataflow pipeline jobs.",
    EXPORT_ARCHIVES_DATASET: "Contains tables that archive the contents of daily exports.",
    EXTERNAL_REFERENCE_DATASET: "Stores data gathered from external sources. CSV versions"
    " of tables are committed to our codebase, and updates to"
    " tables are fully managed by Terraform.",
    OPERATIONS_BASE_DATASET: "Internal Recidiviz operations data. This dataset is a"
    " copy of the operations postgres database.",
    POPULATION_PROJECTION_OUTPUT_DATASET: "Stores output of the population projection"
    " simulations.",
    PULSE_DASHBOARD_SEGMENT_DATASET: "Stores events logged from pulse-dashboard via Segment.",
    SENDGRID_EMAIL_DATA_DATASET: "Stores the output of email activity data from Sendgrid.",
    STATE_BASE_DATASET: "Ingested state data. This dataset is a copy of the state"
    " postgres database.",
    STATIC_REFERENCE_TABLES_DATASET: "Reference tables used by various views in BigQuery."
    " May need to be updated manually for new states.",
    SUPPLEMENTAL_DATA_DATASET: "Stores datasets generated not by traditional ingest or calc pipelines in BigQuery.",
    VERA_DATASET: "Stores data calculated outside of our codebase by Vera. Used only by Vera.",
    VIEW_UPDATE_METADATA_DATASET: "Stores metadata about our view update operations.",
}
OTHER_SOURCE_TABLE_DATASETS = set(OTHER_SOURCE_TABLE_DATASETS_TO_DESCRIPTIONS.keys())

# These datasets should only contain tables that provide the source data for our view graph.
VIEW_SOURCE_TABLE_DATASETS_TO_DESCRIPTIONS = {
    **RAW_DATA_TABLE_DATASETS_TO_DESCRIPTIONS,
    **SUPPLEMENTAL_DATASETS_TO_DESCRIPTIONS,
    **VALIDATION_DATASETS_TO_DESCRIPTIONS,
    **VALIDATION_ONEOFF_DATASETS_TO_DESCRIPTIONS,
    **OTHER_SOURCE_TABLE_DATASETS_TO_DESCRIPTIONS,
    **NORMALIZED_DATASETS_TO_DESCRIPTIONS,
}
VIEW_SOURCE_TABLE_DATASETS = set(VIEW_SOURCE_TABLE_DATASETS_TO_DESCRIPTIONS.keys())


def is_state_specific_address(address: BigQueryAddress) -> bool:
    """Returns true if either of the dataset_id or table_id starts with a state code
    prefix ('us_xx_') or ends with a state code suffix ('_us_xx').
    """
    is_state_specific = False
    for s in [address.dataset_id, address.table_id]:
        is_state_specific |= bool(re.match("^us_[a-z]{2}_.*$", s)) or bool(
            re.match("^.*_us_[a-z]{2}$", s)
        )

    return is_state_specific
