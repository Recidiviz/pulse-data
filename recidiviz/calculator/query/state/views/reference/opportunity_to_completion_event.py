# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Mapping between opportunity type and completion event type"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import REFERENCE_VIEWS_DATASET
from recidiviz.calculator.query.state.views.workflows.firestore.client_record import (
    ELIGIBILITY_QUERY_CONFIGS as CLIENT_RECORD_ELIGIBILITY_QUERY_CONFIGS,
)
from recidiviz.calculator.query.state.views.workflows.firestore.client_record import (
    EligibilityQueryConfig,
)
from recidiviz.calculator.query.state.views.workflows.firestore.resident_record import (
    ELIGIBILITY_QUERY_CONFIGS as RESIDENT_RECORD_ELIGIBILITY_QUERY_CONFIGS,
)
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    TaskCompletionEventType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

OPPORTUNITY_TO_COMPLETION_EVENT_VIEW_NAME = "opportunity_to_completion_event"

OPPORTUNITY_TO_COMPLETION_EVENT_DESCRIPTION = (
    """Mapping between opportunity type and completion event type"""
)

# List the launched opportunities not included in the ELIGIBILITY_QUERY_CONFIGS list
# TODO(#19054): include the TN configs in the unified config file
ADDITIONAL_ELIGIBILITY_QUERY_CONFIGS = [
    EligibilityQueryConfig(
        "US_TN",
        "compliantReporting",
        "us_tn_compliant_reporting_logic_materialized",
        TaskCompletionEventType.TRANSFER_TO_LIMITED_SUPERVISION,
    ),
    EligibilityQueryConfig(
        "US_TN",
        "supervisionLevelDowngrade",
        "us_tn_supervision_level_downgrade_record_materialized",
        TaskCompletionEventType.SUPERVISION_LEVEL_DOWNGRADE,
    ),
    EligibilityQueryConfig(
        "US_TN",
        "usTnExpiration",
        "us_tn_full_term_supervision_discharge_record_materialized",
        TaskCompletionEventType.FULL_TERM_DISCHARGE,
    ),
]

OPPORTUNITY_TO_COMPLETION_EVENT_QUERY_TEMPLATE = "UNION ALL".join(
    [
        f"""
SELECT
    "{eligibility_config.state_code}" AS state_code,
    "{eligibility_config.opportunity_name}" AS opportunity_type,
    "{eligibility_config.opportunity_record_view}" AS opportunity_record_view,
    "{eligibility_config.task_completion_event.name}" AS completion_event_type,
"""
        for eligibility_config in CLIENT_RECORD_ELIGIBILITY_QUERY_CONFIGS
        + RESIDENT_RECORD_ELIGIBILITY_QUERY_CONFIGS
        + ADDITIONAL_ELIGIBILITY_QUERY_CONFIGS
    ]
)

OPPORTUNITY_TO_COMPLETION_EVENT_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=REFERENCE_VIEWS_DATASET,
    view_id=OPPORTUNITY_TO_COMPLETION_EVENT_VIEW_NAME,
    view_query_template=OPPORTUNITY_TO_COMPLETION_EVENT_QUERY_TEMPLATE,
    description=OPPORTUNITY_TO_COMPLETION_EVENT_DESCRIPTION,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        OPPORTUNITY_TO_COMPLETION_EVENT_VIEW_BUILDER.build_and_print()
