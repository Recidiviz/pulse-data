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
"""Constants related to source table datasets that contain tables that are managed
outside of our standard table update process (e.g. via Terraform or via an external
process that writes to BQ).
"""
from recidiviz.calculator.query.state.dataset_config import (
    AUTH0_EVENTS,
    AUTH0_PROD_ACTION_LOGS,
    EXPORT_ARCHIVES_DATASET,
    JII_AUTH0_PROD_SEGMENT_DATASET,
    PULSE_DASHBOARD_SEGMENT_DATASET,
    SENDGRID_EMAIL_DATA_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.validation.views.dataset_config import (
    validation_oneoff_dataset_for_state,
)

# Views that are updated manually
MANUALLY_UPDATED_SOURCE_TABLES_DATASET: str = "manually_updated_source_tables"

VALIDATION_RESULTS_DATASET_ID: str = "validation_results"

ALL_BILLING_DATA_DATASET: str = "all_billing_data"

TWILIO_WEBHOOK_REQUESTS_DATASET_ID: str = "twilio_webhook_requests"

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
    MANUALLY_UPDATED_SOURCE_TABLES_DATASET: (
        "Stores source tables that are updated manually."
    ),
    PULSE_DASHBOARD_SEGMENT_DATASET: (
        "Stores events logged from pulse-dashboard via Segment."
    ),
    SENDGRID_EMAIL_DATA_DATASET: (
        "Stores the output of email activity data from Sendgrid."
    ),
    STATIC_REFERENCE_TABLES_DATASET: (
        "Reference tables used by various views in BigQuery. May need to be updated manually for new states."
    ),
    VALIDATION_RESULTS_DATASET_ID: (
        "Stores results from our data validations framework."
    ),
    ALL_BILLING_DATA_DATASET: "Stores information about our GCP cloud costs",
    TWILIO_WEBHOOK_REQUESTS_DATASET_ID: "Stores requests from Twilio made to internal webhooks",
    JII_AUTH0_PROD_SEGMENT_DATASET: "Stores auth0 events logged from JII Tablet App via Segment",
}
