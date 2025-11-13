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
"""Lists views that are currently exempted from checks related to external id-type
columns.
"""
from functools import cache
from types import ModuleType

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.state.views.analyst_data.product_roster_archive_sessions import (
    PRODUCT_ROSTER_ARCHIVE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_segregation_lists import (
    US_TN_SEGREGATION_LISTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.jii_texting.jii_to_text import (
    JII_TO_TEXT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.jii_texting.us_ix_lsu import (
    US_IX_LSU_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.jii_texting.us_tx_scheduled_contacts import (
    US_TX_SCHEDULED_CONTACTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_district_managers import (
    SUPERVISION_DISTRICT_MANAGERS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_officer_supervisors import (
    SUPERVISION_OFFICER_SUPERVISORS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_officers import (
    SUPERVISION_OFFICERS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.prototypes.case_note_search.case_notes import (
    CASE_NOTES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reentry.case_manager import (
    REENTRY_CASE_MANAGER_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reentry.client import (
    REENTRY_CLIENT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reentry.supervision_officer import (
    REENTRY_SUPERVISION_OFFICER_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.ingested_product_users import (
    INGESTED_PRODUCT_USERS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.product_roster import (
    PRODUCT_ROSTER_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.product_roster_archive import (
    PRODUCT_ROSTER_ARCHIVE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.supervision_current_staff import (
    SUPERVISION_CURRENT_STAFF_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.workflows_opportunity_configs import (
    WORKFLOWS_OPPORTUNITY_CONFIGS,
)
from recidiviz.calculator.query.state.views.sentencing.case_record import (
    SENTENCING_CASE_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentencing.case_record_historical import (
    SENTENCING_CASE_RECORD_HISTORICAL_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentencing.client_record import (
    SENTENCING_CLIENT_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentencing.client_record_historical import (
    SENTENCING_CLIENT_RECORD_HISTORICAL_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentencing.staff_record import (
    SENTENCING_STAFF_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentencing.us_ix.us_ix_sentencing_case_disposition_preprocessed import (
    US_IX_SENTENCING_CASE_DISPOSITION_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentencing.us_nd.us_nd_sentencing_case_disposition_preprocessed import (
    US_ND_SENTENCING_CASE_DISPOSITION_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.state_staff_id_to_legacy_supervising_officer_external_id import (
    STATE_STAFF_ID_TO_LEGACY_SUPERVISING_OFFICER_EXTERNAL_ID_VIEW_BUILDER,
)
from recidiviz.ingest.views.dataset_config import (
    NORMALIZED_STATE_VIEWS_DATASET,
    STATE_BASE_VIEWS_DATASET,
)
from recidiviz.persistence.database.schema_utils import is_association_table
from recidiviz.persistence.entity.base_entity import (
    ExternalIdEntity,
    HasExternalIdEntity,
)
from recidiviz.persistence.entity.entity_utils import (
    get_entity_class_in_module_with_table_id,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.utils import metadata
from recidiviz.view_registry.deployed_views import deployed_view_builders


def get_workflows_opportunity_record_addresses() -> set[BigQueryAddress]:
    return {config.view_builder.address for config in WORKFLOWS_OPPORTUNITY_CONFIGS}


# Lists of views that have an "external_id" column. The name "external_id" is vague
# and ideally we'll eventually rename all these columns to something more specific.
# TODO(#45176): These exemptions should eventually all be removed once external_id
#  columns in these views are either deleted or renamed with more specific names.
_KNOWN_VIEWS_WITH_UNQUALIFIED_EXTERNAL_ID_COLUMN: dict[BigQueryAddress, str] = {
    # analyst_data views
    PRODUCT_ROSTER_ARCHIVE_SESSIONS_VIEW_BUILDER.address: (
        "TODO(#45176): Remove external_id column once a new, "
        "more specifically named column is introduced and referenced downstream. As of "
        "7/17/25, this view is referenced in Looker."
    ),
    # case_notes_prototype_views views
    CASE_NOTES_VIEW_BUILDER.address: (
        "TODO(#45176): Remove external_id column once a new, "
        "more specifically named column is introduced and referenced downstream. As of "
        "7/17/25, this view is exported via the CASE_NOTES_VERTEX_SEARCH metric export."
    ),
    # jii_texting views
    JII_TO_TEXT_VIEW_BUILDER.address: (
        "TODO(#45177): Remove external_id column once JII texting frontend has "
        "migrated to use person_id or stable_person_external_id cols"
    ),
    # outliers_views views
    SUPERVISION_DISTRICT_MANAGERS_VIEW_BUILDER.address: (
        "TODO(#45176): Remove external_id column once a new, "
        "more specifically named column is introduced and referenced downstream. As of "
        "7/17/25, this view is exported via the INSIGHTS metric export."
    ),
    SUPERVISION_OFFICER_SUPERVISORS_VIEW_BUILDER.address: (
        "TODO(#45176): Remove external_id column once a new, "
        "more specifically named column is introduced and referenced downstream. As of "
        "7/17/25, this view is exported via the INSIGHTS metric export."
    ),
    SUPERVISION_OFFICERS_VIEW_BUILDER.address: (
        "TODO(#45176): Remove external_id column once a new, "
        "more specifically named column is introduced and referenced downstream. As of "
        "7/17/25, this view is exported via the INSIGHTS metric export."
    ),
    # raw data views
    BigQueryAddress(
        dataset_id="us_tn_raw_data_views",
        table_id="RECIDIVIZ_REFERENCE_staff_supervisor_and_caseload_roster_all",
    ): "TODO(#45176):rename this column to staff_external_id",
    # reentry views
    REENTRY_CASE_MANAGER_VIEW_BUILDER.address: (
        "TODO(#45253): Remove external_id column once it is no longer referenced "
        "in the Reentry tools repo."
    ),
    REENTRY_CLIENT_VIEW_BUILDER.address: (
        "TODO(#45253): Remove external_id column once it is no longer referenced "
        "in the Reentry tools repo."
    ),
    REENTRY_SUPERVISION_OFFICER_VIEW_BUILDER.address: (
        "TODO(#45253): Remove external_id column once it is no longer referenced "
        "in the Reentry tools repo."
    ),
    SUPERVISION_CURRENT_STAFF_VIEW_BUILDER.address: (
        "TODO(#45176): Remove external_id column once it is no longer referenced in "
        "Looker (will be migrated to staff_external_id)"
    ),
    # reference_views views
    INGESTED_PRODUCT_USERS_VIEW_BUILDER.address: (
        "TODO(#45176): Remove external_id column once a new, "
        "more specifically named column is introduced and referenced downstream. As of "
        "7/17/25, this view is exported via the PRODUCT_USER_IMPORT metric export."
    ),
    PRODUCT_ROSTER_VIEW_BUILDER.address: (
        "TODO(#45176): Remove external_id column once a new, "
        "more specifically named column is introduced and referenced downstream. As of "
        "7/17/25, this view is referenced in Looker."
    ),
    PRODUCT_ROSTER_ARCHIVE_VIEW_BUILDER.address: (
        "TODO(#45176): Remove external_id column once a new, "
        "more specifically named column is introduced and referenced downstream. As of "
        "7/17/25, this view is not referenced in Looker or any metric export."
    ),
    # sentencing_views views
    SENTENCING_CASE_RECORD_VIEW_BUILDER.address: (
        "TODO(#45176): Remove external_id column once a new, "
        "more specifically named column is introduced and referenced downstream. As of "
        "7/17/25, this view is exported via the SENTENCING metric export."
    ),
    SENTENCING_CASE_RECORD_HISTORICAL_VIEW_BUILDER.address: (
        "TODO(#45176): Remove external_id column once a new, "
        "more specifically named column is introduced and referenced downstream. As of "
        "7/17/25, this view is exported via the SENTENCING metric export."
    ),
    SENTENCING_CLIENT_RECORD_VIEW_BUILDER.address: (
        "TODO(#45176): Remove external_id column once a new, "
        "more specifically named column is introduced and referenced downstream. As of "
        "7/17/25, this view is exported via the SENTENCING metric export."
    ),
    SENTENCING_CLIENT_RECORD_HISTORICAL_VIEW_BUILDER.address: (
        "TODO(#45176): Remove external_id column once a new, "
        "more specifically named column is introduced and referenced downstream. As of "
        "7/17/25, this view is exported via the SENTENCING metric export."
    ),
    SENTENCING_STAFF_RECORD_VIEW_BUILDER.address: (
        "TODO(#45176): Remove external_id column once a new, "
        "more specifically named column is introduced and referenced downstream. As of "
        "7/17/25, this view is exported via the SENTENCING metric export."
    ),
    # sessions views
    STATE_STAFF_ID_TO_LEGACY_SUPERVISING_OFFICER_EXTERNAL_ID_VIEW_BUILDER.address: (
        "TODO(#45176): Delete the external_id column once it is no longer referenced "
        "in Looker"
    ),
    # workflows_views views
    **{
        address: (
            "TODO(#45179): Remove this exemption once product frontends only reference "
            "the person_id column on opportunity records and the external_id column "
            "can be deleted."
        )
        for address in get_workflows_opportunity_record_addresses()
    },
}


def _get_state_entity_view_addresses_with_external_id_column(
    deployed_views_by_address: dict[BigQueryAddress, BigQueryViewBuilder],
) -> set[BigQueryAddress]:
    """Returns the addresses for all views in the `state_views` /
    `normalized_state_views` datasets which we know and expect to have an external_id
    column.
    """
    external_id_entity_view_addresses: set[BigQueryAddress] = set()
    for address, vb in deployed_views_by_address.items():
        entity_table_id = vb.table_for_query.table_id
        if is_association_table(entity_table_id):
            continue
        if address.dataset_id not in {
            STATE_BASE_VIEWS_DATASET,
            NORMALIZED_STATE_VIEWS_DATASET,
        }:
            continue
        entities_module: ModuleType
        if address.dataset_id == STATE_BASE_VIEWS_DATASET:
            entities_module = state_entities
        elif address.dataset_id == NORMALIZED_STATE_VIEWS_DATASET:
            entities_module = normalized_entities
        else:
            raise ValueError(f"Unexpected dataset [{address.dataset_id}]")
        entity_cls = get_entity_class_in_module_with_table_id(
            entities_module, entity_table_id
        )
        if issubclass(entity_cls, (HasExternalIdEntity, ExternalIdEntity)):
            external_id_entity_view_addresses.add(address)
    return external_id_entity_view_addresses


@cache
def get_known_views_with_unqualified_external_id(
    # We require project_id as an argument so that we don't return incorrect cached
    # results when metadata.project_id() changes (e.g. in tests).
    project_id: str,
) -> set[BigQueryAddress]:
    """Returns the addresses of every deployed BQ view that has a known column named
    "external_id". Generally we want views to use a more specific name because
    external_id is vague (external_id for what?), but there are some views where we
    expect this column name or which this column name has been exempted for legacy
    reasons.
    """
    if project_id != metadata.project_id():
        raise ValueError(
            f"Expected project_id [{project_id}] to match metadata.project_id() "
            f"[{metadata.project_id()}]"
        )

    deployed_views_by_address = {vb.address: vb for vb in deployed_view_builders()}
    external_id_entity_view_addresses = (
        _get_state_entity_view_addresses_with_external_id_column(
            deployed_views_by_address
        )
    )

    # These are views with an unqualified external_id column that we don't plan to
    # eventually rename / eliminate
    expected_views_with_unqualified_external_id = external_id_entity_view_addresses | {
        # This view is just a mirror of the state_person_external_id table, so we expect
        # the schema to remain the same.
        BigQueryAddress(
            dataset_id="externally_shared_views",
            table_id="csg_state_person_external_id",
        )
    }

    return (
        set(_KNOWN_VIEWS_WITH_UNQUALIFIED_EXTERNAL_ID_COLUMN)
        | expected_views_with_unqualified_external_id
    )


# Views that have *person_external_id columns but are NOT part of metric exports.
# These should eventually be refactored so that person external IDs are only joined
# at the very end (in the metric export views) rather than passed through internal
# foundational views.
_KNOWN_NON_EXPORT_VIEWS_WITH_PERSON_EXTERNAL_ID_COLUMN: dict[BigQueryAddress, str] = {
    # analyst_data views
    US_TN_SEGREGATION_LISTS_VIEW_BUILDER.address: "TODO(#44755): Remove this exemption once we remove the person_external_id column from this view",
    # jii_texting views
    US_IX_LSU_VIEW_BUILDER.address: "TODO(#44755): Remove this exemption once we remove the person_external_id column from this view",
    US_TX_SCHEDULED_CONTACTS_VIEW_BUILDER.address: "TODO(#44755): Remove this exemption once we remove the person_external_id column from this view",
    # Sentencing views
    US_IX_SENTENCING_CASE_DISPOSITION_PREPROCESSED_VIEW_BUILDER.address: "TODO(#44755): Remove this exemption once we remove the person_external_id column from this view",
    US_ND_SENTENCING_CASE_DISPOSITION_PREPROCESSED_VIEW_BUILDER.address: "TODO(#44755): Remove this exemption once we remove the person_external_id column from this view",
}


@cache
def get_known_non_export_views_with_person_external_id_column(
    # We require project_id as an argument so that we don't return incorrect cached
    # results when metadata.project_id() changes (e.g. in tests).
    project_id: str,
) -> set[BigQueryAddress]:
    """Returns the addresses of every deployed BQ view that has a column matching
    the pattern *person_external_id but is NOT part of a metric export. Generally,
    we want person external ID columns to only exist in metric export views, since
    we should not pass external id information through our internal, foundational
    views but rather should join at the very end to get a relevant person external id.
    """
    if project_id != metadata.project_id():
        raise ValueError(
            f"Expected project_id [{project_id}] to match metadata.project_id() "
            f"[{metadata.project_id()}]"
        )
    return set(_KNOWN_NON_EXPORT_VIEWS_WITH_PERSON_EXTERNAL_ID_COLUMN)
