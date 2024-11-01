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
"""Constants related to externally managed source table datasets."""
from recidiviz.calculator.query.state.dataset_config import (
    AUTH0_EVENTS,
    AUTH0_PROD_ACTION_LOGS,
    EXPORT_ARCHIVES_DATASET,
    GOOGLE_SHEET_BACKED_TABLES_DATASET,
    MANUALLY_UPDATED_SOURCE_TABLES_DATASET,
    POPULATION_PROJECTION_OUTPUT_DATASET,
    PULSE_DASHBOARD_SEGMENT_DATASET,
    SENDGRID_EMAIL_DATA_DATASET,
    SPARK_OUTPUT_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.datasets.static_data.config import EXTERNAL_REFERENCE_DATASET
from recidiviz.validation.validation_result_storage import VALIDATION_RESULTS_DATASET_ID
from recidiviz.validation.views.dataset_config import (
    validation_oneoff_dataset_for_state,
)

VERA_DATASET: str = "vera_data"


VALIDATION_ONEOFF_DATASETS_TO_DESCRIPTIONS = {
    validation_oneoff_dataset_for_state(state_code): (
        f"Contains one-off validation data provided directed by "
        f"{StateCode.get_state(state_code)}."
    )
    for state_code in [
        StateCode.US_IX,
        StateCode.US_ME,
        StateCode.US_MO,
        StateCode.US_ND,
        StateCode.US_PA,
        StateCode.US_TN,
    ]
}

SUPPLEMENTAL_DATASETS_TO_DESCRIPTIONS = {
    f"{state_code.value.lower()}_supplemental": (
        f"Contains data provided directly by {StateCode.get_state(state_code)} that is "
        f"not run through direct ingest, e.g. validation data."
    )
    for state_code in [
        StateCode.US_PA,
    ]
}

EXTERNALLY_MANAGED_DATASETS_TO_DESCRIPTIONS = {
    **SUPPLEMENTAL_DATASETS_TO_DESCRIPTIONS,
    **VALIDATION_ONEOFF_DATASETS_TO_DESCRIPTIONS,
    AUTH0_EVENTS: "Stores legacy events logged from Auth0 actions via Segment",
    AUTH0_PROD_ACTION_LOGS: "Stores events logged from Auth0 actions via Segment",
    EXPORT_ARCHIVES_DATASET: (
        "Contains tables that archive the contents of daily exports."
    ),
    EXTERNAL_REFERENCE_DATASET: (
        "Stores data gathered from external sources. CSV versions of tables are "
        "committed to our codebase, and updates to tables are fully managed by "
        "Terraform."
    ),
    GOOGLE_SHEET_BACKED_TABLES_DATASET: (
        "Stores views that are backed by Google Sheets."
    ),
    MANUALLY_UPDATED_SOURCE_TABLES_DATASET: (
        "Stores source tables that are updated manually."
    ),
    POPULATION_PROJECTION_OUTPUT_DATASET: (
        "Stores output of the population projection simulations."
    ),
    PULSE_DASHBOARD_SEGMENT_DATASET: (
        "Stores events logged from pulse-dashboard via Segment."
    ),
    SENDGRID_EMAIL_DATA_DATASET: (
        "Stores the output of email activity data from Sendgrid."
    ),
    SPARK_OUTPUT_DATASET: "Stores output of Spark simulations",
    STATIC_REFERENCE_TABLES_DATASET: (
        "Reference tables used by various views in BigQuery. May need to be updated manually for new states."
    ),
    VALIDATION_RESULTS_DATASET_ID: (
        "Stores results from our data validations framework."
    ),
    VERA_DATASET: (
        "Stores data calculated outside of our codebase by Vera. Used only by Vera."
    ),
}
