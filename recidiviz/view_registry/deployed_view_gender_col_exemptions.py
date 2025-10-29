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
"""Lists views that are currently exempted from checks related to gender columns.
"""
from functools import cache

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.calculator.query.state.views.analyst_data.session_cohort_reincarceration import (
    SESSION_COHORT_REINCARCERATION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.event_level.supervision_to_prison_transitions_raw import (
    SUPERVISION_TO_PRISON_TRANSITIONS_RAW_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dataflow_metrics_materialized.most_recent_dataflow_metrics import (
    MOST_RECENT_METRICS_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.dataflow_metrics_materialized.most_recent_dataflow_population_span_to_single_day_metrics import (
    MOST_RECENT_POPULATION_SPAN_TO_SINGLE_DAY_METRICS_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.dataflow_metrics_materialized.most_recent_single_day_dataflow_metrics import (
    MOST_RECENT_SINGLE_DAY_METRICS_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.lantern_revocations_matrix.revocations_matrix_by_person import (
    REVOCATIONS_MATRIX_BY_PERSON_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.lantern_revocations_matrix.supervision_matrix_by_person import (
    SUPERVISION_MATRIX_BY_PERSON_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.lantern_revocations_matrix.supervision_termination_matrix_by_person import (
    SUPERVISION_TERMINATION_MATRIX_BY_PERSON_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.state_resident_population import (
    STATE_RESIDENT_POPULATION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.state_resident_population_combined_race_ethnicity import (
    STATE_RESIDENT_POPULATION_COMBINED_RACE_ETHNICITY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentencing.client_record_historical import (
    SENTENCING_CLIENT_RECORD_HISTORICAL_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentencing.sentence_cohort import (
    SENTENCE_COHORT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.compartment_sessions import (
    COMPARTMENT_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.compartment_sub_sessions import (
    COMPARTMENT_SUB_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.shared_metric.event_based_commitments_from_supervision import (
    EVENT_BASED_COMMITMENTS_FROM_SUPERVISION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.shared_metric.event_based_supervision_populations import (
    EVENT_BASED_SUPERVISION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.shared_metric.single_day_incarceration_population_for_spotlight import (
    SINGLE_DAY_INCARCERATION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.shared_metric.single_day_supervision_population_for_spotlight import (
    SINGLE_DAY_SUPERVISION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.shared_metric.supervision_officer_caseload import (
    SUPERVISION_OFFICER_CASELOAD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.shared_metric.supervision_terminations_for_spotlight import (
    SUPERVISION_TERMINATIONS_FOR_SPOTLIGHT_VIEW_BUILDER,
)
from recidiviz.observations.views.events.person.violation import (
    VIEW_BUILDER as VIOLATION_VIEW_BUILDER,
)
from recidiviz.observations.views.spans.person.compartment_sub_session import (
    VIEW_BUILDER as COMPARTMENT_SUB_SESSION_OBSERVATION_VIEW_BUILDER,
)
from recidiviz.utils import metadata
from recidiviz.validation.views.state.incarceration_population_by_demographic_internal_comparison import (
    INCARCERATION_POPULATION_BY_DEMOGRAPHIC_INTERNAL_COMPARISON_VIEW_BUILDER,
)

# Views that have a "gender" column but are NOT part of metric exports.
# These should eventually be refactored to use "sex" instead of "gender".
# TODO(#50972): These exemptions should eventually all be removed once views are
#  migrated to read from sex instead of gender.
_KNOWN_NON_EXPORT_VIEWS_WITH_GENDER_COLUMN: dict[BigQueryAddress, str] = {
    # analyst_data views
    SESSION_COHORT_REINCARCERATION_VIEW_BUILDER.address: "TODO(#50972): Migrate to use sex instead of gender",
    # dashboard_views views
    SUPERVISION_TO_PRISON_TRANSITIONS_RAW_VIEW_BUILDER.address: "TODO(#50972): Migrate to use sex instead of gender",
    # dataflow_metrics_materialized views - all most_recent_* views
    **{
        vb.address: "TODO(#50972): Migrate to use sex instead of gender"
        for vb in MOST_RECENT_METRICS_VIEW_BUILDERS
    },
    **{
        vb.address: "TODO(#50972): Migrate to use sex instead of gender"
        for vb in MOST_RECENT_POPULATION_SPAN_TO_SINGLE_DAY_METRICS_VIEW_BUILDERS
    },
    **{
        vb.address: "TODO(#50972): Migrate to use sex instead of gender"
        for vb in MOST_RECENT_SINGLE_DAY_METRICS_VIEW_BUILDERS
    },
    # externally_shared views
    BigQueryAddress(
        dataset_id="externally_shared_views",
        table_id="csg_compartment_sessions",
    ): "TODO(#50972): This is auto-generated - will be fixed when compartment_sessions is migrated",
    # lantern_revocations_matrix views
    REVOCATIONS_MATRIX_BY_PERSON_VIEW_BUILDER.address: "TODO(#50972): Migrate to use sex instead of gender",
    SUPERVISION_MATRIX_BY_PERSON_VIEW_BUILDER.address: "TODO(#50972): Migrate to use sex instead of gender",
    SUPERVISION_TERMINATION_MATRIX_BY_PERSON_VIEW_BUILDER.address: "TODO(#50972): Migrate to use sex instead of gender",
    # observations views
    VIOLATION_VIEW_BUILDER.address: "TODO(#50972): Migrate to use sex instead of gender",
    COMPARTMENT_SUB_SESSION_OBSERVATION_VIEW_BUILDER.address: "TODO(#50972): Migrate to use sex instead of gender",
    # reference_views views
    STATE_RESIDENT_POPULATION_VIEW_BUILDER.address: "TODO(#50972): Migrate to use sex instead of gender",
    STATE_RESIDENT_POPULATION_COMBINED_RACE_ETHNICITY_VIEW_BUILDER.address: "TODO(#50972): Migrate to use sex instead of gender",
    # sentencing_views views
    SENTENCE_COHORT_VIEW_BUILDER.address: "TODO(#50972): Migrate to use sex instead of gender",
    SENTENCING_CLIENT_RECORD_HISTORICAL_VIEW_BUILDER.address: "TODO(#50972): Migrate to use sex instead of gender",
    # sessions views
    COMPARTMENT_SESSIONS_VIEW_BUILDER.address: "TODO(#50972): Migrate to use sex instead of gender",
    COMPARTMENT_SUB_SESSIONS_VIEW_BUILDER.address: "TODO(#50972): Migrate to use sex instead of gender",
    # shared_metric_views views
    EVENT_BASED_COMMITMENTS_FROM_SUPERVISION_VIEW_BUILDER.address: "TODO(#50972): Migrate to use sex instead of gender",
    EVENT_BASED_SUPERVISION_VIEW_BUILDER.address: "TODO(#50972): Migrate to use sex instead of gender",
    SINGLE_DAY_INCARCERATION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER.address: "TODO(#50972): Migrate to use sex instead of gender",
    SINGLE_DAY_SUPERVISION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER.address: "TODO(#50972): Migrate to use sex instead of gender",
    SUPERVISION_OFFICER_CASELOAD_VIEW_BUILDER.address: "TODO(#50972): Migrate to use sex instead of gender",
    SUPERVISION_TERMINATIONS_FOR_SPOTLIGHT_VIEW_BUILDER.address: "TODO(#50972): Migrate to use sex instead of gender",
    # validation_views views
    INCARCERATION_POPULATION_BY_DEMOGRAPHIC_INTERNAL_COMPARISON_VIEW_BUILDER.address: "TODO(#50972): Migrate to use sex instead of gender",
    # The _errors view is auto-generated by the validation framework
    BigQueryAddress(
        dataset_id="validation_views",
        table_id="incarceration_population_by_demographic_internal_comparison_errors",
    ): "TODO(#50972): This is auto-generated - will be fixed when the base view is migrated",
}


@cache
def get_known_non_export_views_with_gender_column(
    # We require project_id as an argument so that we don't return incorrect cached
    # results when metadata.project_id() changes (e.g. in tests).
    project_id: str,
) -> set[BigQueryAddress]:
    """Returns the addresses of every deployed BQ view that has a column named
    "gender" but is NOT part of a metric export. Generally, we want views to use
    "sex" instead of "gender", but there are some views where this migration is
    still in progress.
    """
    if project_id != metadata.project_id():
        raise ValueError(
            f"Expected project_id [{project_id}] to match metadata.project_id() "
            f"[{metadata.project_id()}]"
        )
    return set(_KNOWN_NON_EXPORT_VIEWS_WITH_GENDER_COLUMN)
