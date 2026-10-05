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
"""Lists views that are currently exempted from checks preventing references to
deprecated views.
"""

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.calculator.query.state.views.analyst_data.psa_risk_scores import (
    PSA_RISK_SCORES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.charges_preprocessed import (
    CHARGES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.compartment_sessions_closest_sentence_imposed_group import (
    COMPARTMENT_SESSIONS_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.sentence_deadline_spans import (
    SENTENCE_DEADLINE_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.sentence_imposed_group_summary import (
    SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.sentence_relationship import (
    SENTENCE_RELATIONSHIP_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.sentence_spans import (
    SENTENCE_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.sentences_preprocessed import (
    SENTENCES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.ingest.views.unioned_normalized_state_views import (
    normalized_state_view_address_for_entity_table,
)
from recidiviz.ingest.views.view_config import (
    get_view_builders_for_views_to_update as get_ingest_infra_view_builders,
)
from recidiviz.monitoring.platform_kpis.dataset_config import PLATFORM_KPIS_DATASET
from recidiviz.monitoring.platform_kpis.velocity.normalized_state_hydration_live_snapshot import (
    NORMALIZED_STATE_HYDRATION_LIVE_SNAPSHOT_VIEW_ID,
)
from recidiviz.observations.views.events.person.sentences_imposed import (
    VIEW_BUILDER as SENTENCES_IMPOSED_OBSERVATIONS_VIEW_BUILDER,
)
from recidiviz.persistence.entity.activity.entities import (
    StateCharge,
    StateIncarcerationSentence,
    StateSupervisionSentence,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.pipelines.ingest.activity.dataset_config import (
    normalized_state_dataset_for_state_code,
)
from recidiviz.task_eligibility.criteria.general.serving_incarceration_sentence_of_less_than_6_years import (
    VIEW_BUILDER as SERVING_INCARCERATION_SENTENCE_OF_LESS_THAN_6_YEARS_VIEW_BUILDER,
)
from recidiviz.utils.types import assert_subclass
from recidiviz.validation.configured_validations import (
    NORMALIZED_STATE_CHARGE_MISSING_DESCRIPTIONS_VIEW_BUILDER,
    SENTENCE_COMPARISON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.primary_keys_unique_across_all_states import (
    PRIMARY_KEYS_UNIQUE_ACROSS_ALL_STATES_VIEW_BUILDER,
)
from recidiviz.validation.views.state.sentences.normalized_state_charge_missing_uniform_offense_labels import (
    NORMALIZED_STATE_CHARGE_MISSING_UNIFORM_OFFENSE_LABELS_VIEW_BUILDER,
)
from recidiviz.validation.views.state.sentences.sentences_missing_date_imposed import (
    SENTENCES_MISSING_DATE_IMPOSED_VIEW_BUILDER,
)
from recidiviz.validation.views.state.sentences.sentences_undefined_relationship import (
    SENTENCES_UNDEFINED_RELATIONSHIP_VIEW_BUILDER,
)
from recidiviz.validation.views.state.sentences.session_liberty_releases_with_no_sentence_completion_date import (
    SESSION_LIBERTY_RELEASES_WITH_NO_SENTENCE_COMPLETION_DATE_VIEW_BUILDER,
)
from recidiviz.validation.views.state.sentences.sessions_missing_closest_sentence_imposed_group import (
    SESSIONS_MISSING_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_BUILDER,
)
from recidiviz.validation.views.state.workflows.us_mi_flag_new_offense_codes import (
    US_MI_FLAG_NEW_OFFENSE_CODES_VIEW_BUILDER,
)


def _get_ingest_metadata_addresses_table(
    state_table_id: str,
) -> list[BigQueryAddress]:
    """Returns addresses for all ingest_metadata views that reference the given table."""
    addresses = []
    table_prefix = f"ingest_state_metadata__{state_table_id}__"
    for builder in get_ingest_infra_view_builders():
        if builder.address.table_id.startswith(table_prefix):
            addresses.append(builder.address)
    return addresses


_ALL_SCHEMA_TABLE_VIEWS = [
    BigQueryAddress(
        dataset_id=PLATFORM_KPIS_DATASET,
        table_id=NORMALIZED_STATE_HYDRATION_LIVE_SNAPSHOT_VIEW_ID,
    ),
    PRIMARY_KEYS_UNIQUE_ACROSS_ALL_STATES_VIEW_BUILDER.address,
]

# These are views that still reference state-specific sentences v1 tables and have been
# exempted. For example the views listed under (StateCode.US_MI, StateCharge) are views
# that reference the us_mi_normalized_state.state_charge table directly.
_SENTENCE_STATE_SPECIFIC_REFERENCE_EXEMPTIONS: dict[
    tuple[StateCode, type[Entity]], dict[BigQueryAddress, str]
] = {
    (StateCode.US_MI, StateCharge): {
        US_MI_FLAG_NEW_OFFENSE_CODES_VIEW_BUILDER.address: (
            "TODO(#46258): Update validation to read from state_charge_v2"
        ),
    },
}

# Maps deprecated view addresses to a dict of views that are allowed to reference
# them. The inner dict maps each exempted view address to a reason string explaining
# why the reference still exists.
SENTENCES_V1_DEPRECATED_VIEWS_AND_USAGE_EXEMPTIONS: dict[
    BigQueryAddress, dict[BigQueryAddress, str]
] = {
    # TODO(#33402): Delete `sentences_preprocessed` once all states are migrated
    # to v2 infra
    SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
        PSA_RISK_SCORES_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view (or delete this view)"
        ),
        SENTENCE_DEADLINE_SPANS_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted as part of the v2 sentences migration"
        ),
        SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted as part of the v2 sentences migration"
        ),
        SENTENCE_RELATIONSHIP_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted as part of the v2 sentences migration"
        ),
        SENTENCE_SPANS_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted as part of the v2 sentences migration"
        ),
        SERVING_INCARCERATION_SENTENCE_OF_LESS_THAN_6_YEARS_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        SENTENCES_MISSING_DATE_IMPOSED_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        SESSION_LIBERTY_RELEASES_WITH_NO_SENTENCE_COMPLETION_DATE_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
    },
    # TODO(#33402): Delete `sentence_spans` once all states are migrated
    # to v2 infra
    SENTENCE_SPANS_VIEW_BUILDER.address: {
        SERVING_INCARCERATION_SENTENCE_OF_LESS_THAN_6_YEARS_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
    },
    # TODO(#33402): Delete `charges_preprocessed` once all states are migrated
    # to v2 infra
    CHARGES_PREPROCESSED_VIEW_BUILDER.address: {
        SENTENCES_PREPROCESSED_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted as part of the v2 sentences "
            "migration"
        ),
    },
    # TODO(#33402): Delete `sentence_deadline_spans` once all states are migrated
    # to v2 infra
    SENTENCE_DEADLINE_SPANS_VIEW_BUILDER.address: {
        SENTENCE_SPANS_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted as part of the v2 sentences "
            "migration"
        ),
    },
    # TODO(#33402): Delete `sentence_imposed_group_summary` once all states are
    # migrated to v2 infra
    SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER.address: {
        SENTENCES_IMPOSED_OBSERVATIONS_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted as part of the v2 sentences "
            "migration"
        ),
        COMPARTMENT_SESSIONS_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted as part of the v2 sentences "
            "migration"
        ),
        SENTENCE_SPANS_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted as part of the v2 sentences "
            "migration"
        ),
    },
    # TODO(#33402): Delete `compartment_sessions_closest_sentence_imposed_group` once
    # all states are migrated to v2 infra
    COMPARTMENT_SESSIONS_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_BUILDER.address: {
        SESSIONS_MISSING_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
    },
    # TODO(#33402): Delete `sentence_relationship` once all states are migrated
    # to v2 infra
    SENTENCE_RELATIONSHIP_VIEW_BUILDER.address: {
        SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted as part of the v2 sentences migration"
        ),
        SENTENCES_UNDEFINED_RELATIONSHIP_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted as part of the v2 sentences "
            "migration - this validation is no longer necessary as all the same "
            "invariants are checked at ingest pipeline runtime."
        ),
    },
    # TODO(#33402): Delete StateIncarcerationSentence once all states are migrated
    # to v2 infra
    normalized_state_view_address_for_entity_table(
        StateIncarcerationSentence.get_table_id()
    ): {
        # Boilerplate views that will be deleted automatically
        **{
            view_address: (
                "TODO(#33402): This view will be deleted when "
                "state_incarceration_sentence is deleted"
            )
            for view_address in (
                _ALL_SCHEMA_TABLE_VIEWS
                + _get_ingest_metadata_addresses_table(
                    StateIncarcerationSentence.get_table_id()
                )
            )
        },
        SENTENCE_COMPARISON_VIEW_BUILDER.address: (
            "TODO(#33402): This view will no longer be needed once we have migrated "
            "all downstream views to read from sentences v2"
        ),
    },
    # TODO(#33402): Delete StateSupervisionSentence once all states are migrated
    # to v2 infra
    normalized_state_view_address_for_entity_table(
        StateSupervisionSentence.get_table_id()
    ): {
        # Boilerplate views that will be deleted automatically
        **{
            view_address: (
                "TODO(#33402): This view will be deleted when "
                "state_supervision_sentence is deleted"
            )
            for view_address in (
                _ALL_SCHEMA_TABLE_VIEWS
                + _get_ingest_metadata_addresses_table(
                    StateSupervisionSentence.get_table_id()
                )
            )
        },
        SENTENCE_COMPARISON_VIEW_BUILDER.address: (
            "TODO(#33402): This view will no longer be needed once we have migrated "
            "all downstream views to read from sentences v2"
        ),
    },
    # TODO(#33402): Delete StateCharge once all states are migrated
    # to v2 infra
    normalized_state_view_address_for_entity_table(StateCharge.get_table_id()): {
        # Boilerplate views that will be deleted automatically
        **{
            view_address: (
                "TODO(#33402): This view will be deleted when state_charge is deleted"
            )
            for view_address in (
                _ALL_SCHEMA_TABLE_VIEWS
                + _get_ingest_metadata_addresses_table(StateCharge.get_table_id())
            )
        },
        NORMALIZED_STATE_CHARGE_MISSING_DESCRIPTIONS_VIEW_BUILDER.address: (
            "TODO(#33402): We need to make a state_charge_v2 equivalent of this "
            "validation, then delete this once all states are migrated to v2."
        ),
        NORMALIZED_STATE_CHARGE_MISSING_UNIFORM_OFFENSE_LABELS_VIEW_BUILDER.address: (
            "TODO(#33402): We need to make a state_charge_v2 equivalent of this "
            "validation, then delete this once all states are migrated to v2."
        ),
        SENTENCE_COMPARISON_VIEW_BUILDER.address: (
            "TODO(#33402): This view will no longer be needed once we have migrated "
            "all downstream views to read from sentences v2"
        ),
    },
    # State-specific v1 sentences table reference exemptions
    **{
        BigQueryAddress(
            dataset_id=normalized_state_dataset_for_state_code(state_code),
            table_id=assert_subclass(entity_cls, Entity).get_table_id(),
        ): (
            {
                normalized_state_view_address_for_entity_table(
                    assert_subclass(entity_cls, Entity).get_table_id()
                ): (
                    f"TODO(#33402): This view will be deleted when "
                    f"{assert_subclass(entity_cls, Entity).get_table_id()} is deleted"
                ),
                **_SENTENCE_STATE_SPECIFIC_REFERENCE_EXEMPTIONS.get(
                    (state_code, entity_cls), {}
                ),
            }
        )
        for state_code in get_existing_direct_ingest_states()
        if state_code != StateCode.US_OZ
        for entity_cls in [
            StateIncarcerationSentence,
            StateSupervisionSentence,
            StateCharge,
        ]
    },
}


# Maps deprecated view addresses to a dict of views that are allowed to reference
# them. The inner dict maps each exempted view address to a reason string explaining
# why the reference still exists.
DEPRECATED_VIEWS_AND_USAGE_EXEMPTIONS: dict[
    BigQueryAddress, dict[BigQueryAddress, str]
] = {**SENTENCES_V1_DEPRECATED_VIEWS_AND_USAGE_EXEMPTIONS}
