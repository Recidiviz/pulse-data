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
"""Script that prints out views that are candidates for deletion because they or their
children are not referenced by any known downstream user.

Usage:
    python -m recidiviz.tools.find_unused_bq_views
"""

from typing import Dict, Optional, Set

from recidiviz.aggregated_metrics.dataset_config import AGGREGATED_METRICS_DATASET_ID
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.big_query.big_query_view_dag_walker import (
    BigQueryViewDagWalker,
    TraversalDirection,
)
from recidiviz.calculator.query.experiments_metadata.views.officer_assignments import (
    OFFICER_ASSIGNMENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.experiments_metadata.views.state_assignments import (
    STATE_ASSIGNMENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.externally_shared_views.dataset_config import (
    EXTERNALLY_SHARED_VIEWS_DATASET,
)
from recidiviz.calculator.query.state.dataset_config import (
    CLASSIFICATION_VIEWS_DATASET,
    IMPACT_REPORTS_DATASET_ID,
    SPARK_OUTPUT_DATASET_MOST_RECENT,
)
from recidiviz.calculator.query.state.views.analyst_data.notion_linear_roadmap_latest import (
    NOTION_LINEAR_ROADMAP_LATEST_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_co.us_co_certificate_awards_to_sentence_credits import (
    US_CO_CERTIFICATE_AWARDS_TO_SENTENCE_CREDITS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.workflows_person_marked_ineligible_status_session_details import (
    WORKFLOWS_PERSON_MARKED_INELIGIBLE_STATUS_SESSION_DETAILS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.classification.all_classification_score_components import (
    ALL_CLASSIFICATION_SCORE_COMPONENTS_VIEW_ID,
)
from recidiviz.calculator.query.state.views.earned_time.us_co_work_engagement import (
    US_CO_WORK_ENGAGEMENT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_officer_metrics_archive import (
    SUPERVISION_OFFICER_METRICS_ARCHIVE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_officers_archive import (
    SUPERVISION_OFFICERS_ARCHIVE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.us_ar_edovo_id_match import (
    US_AR_EDOVO_ID_MATCH_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.us_co_edovo_id_match import (
    US_CO_EDOVO_ID_MATCH_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.us_nd_edovo_id_match import (
    US_ND_EDOVO_ID_MATCH_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.us_ne_edovo_id_match import (
    US_NE_EDOVO_ID_MATCH_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.us_nyc_edovo_id_match import (
    US_NYC_EDOVO_ID_MATCH_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentencing.recidivism_event import (
    RECIDIVISM_EVENT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.cohort_month_index import (
    COHORT_MONTH_INDEX_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions_validation.session_incarceration_admissions_to_dataflow_disaggregated import (
    SESSION_INCARCERATION_ADMISSIONS_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions_validation.session_incarceration_population_to_dataflow_disaggregated import (
    SESSION_INCARCERATION_POPULATION_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions_validation.session_incarceration_releases_to_dataflow_disaggregated import (
    SESSION_INCARCERATION_RELEASES_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions_validation.session_supervision_out_of_state_population_to_dataflow_disaggregated import (
    SESSION_SUPERVISION_OUT_OF_STATE_POPULATION_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions_validation.session_supervision_population_to_dataflow_disaggregated import (
    SESSION_SUPERVISION_POPULATION_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions_validation.session_supervision_starts_to_dataflow_disaggregated import (
    SESSION_SUPERVISION_STARTS_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions_validation.session_supervision_terminations_to_dataflow_disaggregated import (
    SESSION_SUPERVISION_TERMINATIONS_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.tasks.snooze_status_archive import (
    SNOOZE_STATUS_ARCHIVE_VIEW_BUILDER as TASKS_SNOOZE_STATUS_ARCHIVE_VIEW_BUILDER,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.dataset_config import (
    document_extraction_results_unvalidated_dataset_for_region,
)
from recidiviz.ingest.direct.dataset_config import (
    raw_data_views_dataset_for_region,
    raw_latest_views_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.metrics.export.exported_view_utils import (
    get_all_metric_export_view_addresses,
)
from recidiviz.monitoring.platform_kpis.dataset_config import PLATFORM_KPIS_DATASET
from recidiviz.observations.dataset_config import dataset_for_observation_type_cls
from recidiviz.observations.event_observation_big_query_view_builder import (
    MetricUnitOfObservationType,
)
from recidiviz.observations.event_type import EventType
from recidiviz.observations.span_type import SpanType
from recidiviz.outcome_metrics.dataset_config import TRANSITIONS_DATASET
from recidiviz.task_eligibility.candidate_populations.general.non_temporary_custody_incarceration_population import (
    VIEW_BUILDER as NON_TEMPORARY_CUSTODY_INCARCERATION_POPULATION_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.general.supervision_not_past_full_term_completion_date_or_upcoming_120_days import (
    VIEW_BUILDER as SUPERVISION_NOT_PAST_FULL_TERM_COMPLETION_DATE_OR_UPCOMING_120_DAYS,
)
from recidiviz.task_eligibility.criteria.general.supervision_past_full_term_completion_date_or_upcoming_120_days import (
    VIEW_BUILDER as SUPERVISION_PAST_FULL_TERM_COMPLETION_DATE_OR_UPCOMING_120_DAYS,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az.not_serving_expanded_ineligible_offense_for_admin_supervision import (
    VIEW_BUILDER as US_AZ_NOT_SERVING_EXPANDED_INELIGIBLE_OFFENSE_FOR_ADMIN_SUPERVISION,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az.oras_risk_level_is_medium_or_lower import (
    VIEW_BUILDER as US_AZ_ORAS_RISK_LEVEL_IS_MEDIUM_OR_LOWER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az.risk_release_assessment_level_is_minimum import (
    VIEW_BUILDER as US_AZ_RISK_RELEASE_ASSESSMENT_LEVEL_IS_MINIMUM,
)
from recidiviz.tools.unreferenced_bq_view_exemptions import (
    PERMANENT_UNREFERENCED_ADDRESSES_TO_KEEP,
    TEMPORARY_GRANDFATHERED_UNREFERENCED_ADDRESSES_TO_KEEP,
)
from recidiviz.utils.environment import DATA_PLATFORM_GCP_PROJECTS
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.configured_validations import get_all_validations
from recidiviz.validation.views.dataset_config import EXTERNAL_ACCURACY_DATASET
from recidiviz.view_registry.deployed_view_graphs import (
    build_dag_walker_for_all_deployed_view_graphs,
)

# List of views that are definitely referenced in Looker (as of 11/29/23). This list is
# incomplete and you should add to this list / update the date in this comment as you
# work with this script.
LOOKER_REFERENCED_ADDRESSES: Set[BigQueryAddress] = {
    OFFICER_ASSIGNMENTS_VIEW_BUILDER.address,
    STATE_ASSIGNMENTS_VIEW_BUILDER.address,
    NOTION_LINEAR_ROADMAP_LATEST_VIEW_BUILDER.address,
    WORKFLOWS_PERSON_MARKED_INELIGIBLE_STATUS_SESSION_DETAILS_VIEW_BUILDER.address,
    COHORT_MONTH_INDEX_VIEW_BUILDER.address,
    # These views are referenced by a sessions validation dashboard in Looker
    SESSION_INCARCERATION_POPULATION_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER.address,
    SESSION_SUPERVISION_POPULATION_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER.address,
    SESSION_SUPERVISION_OUT_OF_STATE_POPULATION_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER.address,
    SESSION_INCARCERATION_ADMISSIONS_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER.address,
    SESSION_SUPERVISION_STARTS_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER.address,
    SESSION_INCARCERATION_RELEASES_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER.address,
    SESSION_SUPERVISION_TERMINATIONS_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER.address,
}


# Views that are temporarily unreferenced - they will gain a downstream consumer
# or be deleted - and cite a tracking issue for that follow-up work. Every entry
# MUST cite that tracking issue, e.g. TODO(#12345).
TEMPORARY_UNREFERENCED_ADDRESSES_TO_KEEP: Dict[BigQueryAddress, str] = {
    **{
        BigQueryAddress.from_str(f"us_co_document_extraction_results.{table_id}"): (
            "Public LLM document extraction results view. The cross-state union "
            "views that read these are added to the calculation graph in "
            "TODO(#98833). (Emily Turner, 2026-09-18)"
        )
        for table_id in [
            "case_note_employment_info",
            "case_note_employment_info_employer_entities",
            "case_note_employment_info_employers",
            "case_note_employment_info_employment_changes",
            "case_note_housing_info",
            "case_note_housing_info_housing_entities",
            "case_note_payment_info",
            "case_note_payment_info_payments",
        ]
    },
    TASKS_SNOOZE_STATUS_ARCHIVE_VIEW_BUILDER.address: (
        "New tasks snooze status archive view. "
        "Downstream work is tracked in TODO(OBT-39438). (Daniel McLaughlin, 2026-07-29)"
    ),
    US_CO_CERTIFICATE_AWARDS_TO_SENTENCE_CREDITS_VIEW_BUILDER.address: (
        "New CO certificate-to-sentence-credit reconciliation view, consumed directly "
        "by the AET retroactive-credit research notebook rather than a tracked "
        "downstream BQ view. Tracked in TODO(OBT-29669). (Jessica Liu, 2026-08-04)"
    ),
    US_CO_WORK_ENGAGEMENT_VIEW_BUILDER.address: (
        "CO Work Credit Writeback File #1 for new Earned Credit Policy. Work tracked in TODO(OBT-37777)."
        "(Shalin Brahmbhatt, 2026-08-26)"
    ),
    **{
        builder.address: (
            "Per-state lookup of Edovo-reported IDs to person_id. No downstream "
            "consumer yet; wiring one up is tracked in TODO(OBT-52080). "
            "(Kris Gerig, 2026-09-25)"
        )
        for builder in [
            US_AR_EDOVO_ID_MATCH_VIEW_BUILDER,
            US_CO_EDOVO_ID_MATCH_VIEW_BUILDER,
            US_ND_EDOVO_ID_MATCH_VIEW_BUILDER,
            US_NE_EDOVO_ID_MATCH_VIEW_BUILDER,
            US_NYC_EDOVO_ID_MATCH_VIEW_BUILDER,
        ]
    },
    SUPERVISION_NOT_PAST_FULL_TERM_COMPLETION_DATE_OR_UPCOMING_120_DAYS.address: (
        "Temporarily unused after being pulled out of needs_cst_reassessment.py "
        "pending an investigation into probation end-date ingest reliability for "
        "dual-supervision clients. Tracked in TODO(MO-2). (Regina Medina, 2026-08-31)"
    ),
    SUPERVISION_PAST_FULL_TERM_COMPLETION_DATE_OR_UPCOMING_120_DAYS.address: (
        "Only consumer is the NOT-version above, which is itself temporarily "
        "unused for the same reason. Tracked in TODO(MO-2). (Regina Medina, 2026-08-31)"
    ),
    **{
        BigQueryAddress(dataset_id="llm_eval__label_studio", table_id=table_id): (
            "Label Studio annotation eval views. Not yet consumed by a downstream "
            "product; tracked in TODO(#90005). (Mayuka Sarukkai, 2026-07-06)"
        )
        for table_id in [
            "cni_accuracy_per_field_annotations_parsed",
            "cni_accuracy_per_field_annotation_overrides",
            "cni_accuracy_per_field_annotation_pairs",
            "cni_accuracy_per_field_annotation_summary",
            "cni_accuracy_per_field_irr_summary",
            "cni_accuracy_per_field_irr_trend",
            "meetings_module_quality_annotations_parsed",
            "meetings_module_quality_annotation_overrides",
            "meetings_module_quality_annotation_pairs",
            "meetings_module_quality_annotation_summary",
            "meetings_module_quality_irr_summary",
            "meetings_module_quality_irr_trend",
        ]
    },
    **{
        BigQueryAddress(dataset_id="llm_eval__label_studio", table_id=table_id): (
            "Label Studio submitted-tasks views, the coverage/backlog denominator "
            "for the annotation views. Not yet consumed downstream; consumers are "
            "tracked in TODO(OBT-46570) and TODO(OBT-49399). (Ben Packer, 2026-09-16)"
        )
        for table_id in [
            "cni_accuracy_per_field_submitted_tasks",
            "meetings_module_quality_submitted_tasks",
        ]
    },
    SUPERVISION_OFFICER_METRICS_ARCHIVE_VIEW_BUILDER.address: (
        "Will be referenced for zero grants impact tracking (see TODO(#34607)) (Gina Valderrama 10/29/24)"
    ),
    SUPERVISION_OFFICERS_ARCHIVE_VIEW_BUILDER.address: (
        "Will be referenced to support Outliers analytics work (see TODO(#27576)) (Alexa Batino 2/14/24)"
    ),
    RECIDIVISM_EVENT_VIEW_BUILDER.address: (
        "Referenced by PSI Case Insights BigQuery writer (see TODO(#30876)) (Ben Packer 7/9/24) "
    ),
    NON_TEMPORARY_CUSTODY_INCARCERATION_POPULATION_VIEW_BUILDER.address: (
        "Will be used for MO RH eligibility spans (see TODO(#31337)) (Daniel Allen 7/24/24)"
    ),
    US_AZ_NOT_SERVING_EXPANDED_INELIGIBLE_OFFENSE_FOR_ADMIN_SUPERVISION.address: (
        "Temporarily unused until AZ administrative superivsion V1 is deprecated. TODO(AZ-392) (Elise Gonzalez 9/18/2026)"
    ),
    US_AZ_ORAS_RISK_LEVEL_IS_MEDIUM_OR_LOWER.address: (
        "Temporarily unused until AZ administrative superivsion V1 is deprecated. TODO(AZ-392) (Elise Gonzalez 9/16/2026)"
    ),
    US_AZ_RISK_RELEASE_ASSESSMENT_LEVEL_IS_MINIMUM.address: (
        "Temporarily unused until AZ administrative superivsion V1 is deprecated. TODO(AZ-392) (Elise Gonzalez 9/16/2026)"
    ),
    BigQueryAddress.from_str(
        "static_reference_data_views.us_tn_incarceration_facility_names"
    ): "TODO(#46196): This is unused, will need to check with TN team to confirm it can be deleted (Anna Geiduschek, 8/26/2025)",
    BigQueryAddress.from_str(
        "static_reference_data_views.us_tn_supervision_facility_names"
    ): "TODO(#46196): This is unused, will need to check with TN team to confirm it can be deleted (Anna Geiduschek, 8/26/2025)",
    BigQueryAddress.from_str(
        "static_reference_data_views.us_tn_supervision_locations"
    ): "TODO(#46196): This is unused, will need to check with TN team to confirm it can be deleted (Anna Geiduschek, 8/26/2025)",
    BigQueryAddress.from_str(
        "static_reference_data_views.us_tn_incident_infraction_descriptions"
    ): "TODO(#53520): This will be used to add incident and infraction descriptions to facility tools (Lydia Masr, 11/26/2025)",
    **{
        BigQueryAddress.from_str(f"normalized_state_views.{table_id}"): (
            "Generated over a v1 sentence association table; its only reader was the v1 "
            "branch of sentences_preprocessed. Goes away with the v1 entities in "
            "TODO(#33402). (Andrew Gaidus, 2026-09-29)"
        )
        for table_id in [
            "state_charge_incarceration_sentence_association_view",
            "state_charge_supervision_sentence_association_view",
        ]
    },
    BigQueryAddress.from_str(
        "sentence_sessions_v2_all.compartment_sessions_closest_imposed_sentence"
    ): (
        "The sentence_sessions_v2_all copy of a view whose readers use the "
        "sentence_sessions version. The v2_all copies go away in TODO(#33402). "
        "(Andrew Gaidus, 2026-09-30)"
    ),
}

UNREFERENCED_ADDRESSES_TO_KEEP_WITH_REASON: Dict[BigQueryAddress, str] = {
    # temporary exemptions, each citing a tracking issue for removal
    **TEMPORARY_UNREFERENCED_ADDRESSES_TO_KEEP,
    # do NOT add to this list
    **TEMPORARY_GRANDFATHERED_UNREFERENCED_ADDRESSES_TO_KEEP,
    # views with no plan to reference
    **PERMANENT_UNREFERENCED_ADDRESSES_TO_KEEP,
}

DATASETS_REFERENCED_BY_MISC_PROCESSES = {
    # All views in this dataset are referenced in Looker via autogenerated views
    AGGREGATED_METRICS_DATASET_ID,
    # These are validation views that compare external reference data to actual counts.
    EXTERNAL_ACCURACY_DATASET,
    # Views used in CSG export
    EXTERNALLY_SHARED_VIEWS_DATASET,
    # Contains views referencing the output of Spark population projection runs. These
    # views are used to build Spark reports.
    SPARK_OUTPUT_DATASET_MOST_RECENT,
    # Views in this dataset are aggregated metrics related to impact reports
    IMPACT_REPORTS_DATASET_ID,
    # Views in this dataset are used by platform kpi dashboards
    PLATFORM_KPIS_DATASET,
    # Views in the dataset are used for org-wide impact tracking
    TRANSITIONS_DATASET,
    # Views in observation span and event datasets are used for generating
    # custom metrics in looker or analyst notebooks
    *[
        dataset_for_observation_type_cls(unit_of_observation_type, EventType)
        for unit_of_observation_type in MetricUnitOfObservationType
    ],
    *[
        dataset_for_observation_type_cls(unit_of_observation_type, SpanType)
        for unit_of_observation_type in MetricUnitOfObservationType
    ],
}


def _should_ignore_unused_address(address: BigQueryAddress) -> bool:
    """Returns true for views that may not be in use but can be ignored if they happen
    to be. These views are not tracked in UNREFERENCED_ADDRESSES_TO_KEEP_WITH_REASON because
    they aren't views that are important enough to keep that parent views should be
    marked as "used".
    """

    all_validations = get_all_validations()
    views_with_configured_validations: Set[BigQueryAddress] = {
        v.view_builder.address for v in all_validations
    } | {v.error_view_builder.address for v in all_validations}
    if address in views_with_configured_validations:
        # We don't mark these views as "used" because the existence of a validation does
        # not in itself mean we need to keep a parent view, but we generally don't
        # expect these views to be referenced by other views.
        return True

    if address.dataset_id in {
        # We autogenerate these views out of convenience and don't expect all to be used
        *{
            raw_latest_views_dataset_for_region(state_code, instance)
            for state_code in StateCode
            for instance in DirectIngestInstance
        },
        *{
            raw_data_views_dataset_for_region(state_code, instance)
            for state_code in StateCode
            for instance in DirectIngestInstance
        },
        # Unvalidated LLM extraction results views exist for debugging extraction
        # output before validation; nothing downstream reads them.
        *{
            document_extraction_results_unvalidated_dataset_for_region(state_code)
            for state_code in StateCode
        },
    }:
        return True

    if address.dataset_id.startswith("task_eligibility") and address.table_id in {
        # TES autogenerated convenience views
        "all_general_completion_events",
        "all_state_specific_completion_events",
        "all_criteria",
        "all_general_criteria",
        "all_state_specific_criteria",
        "all_general_candidate_populations",
        "all_candidate_populations",
        "all_state_specific_candidate_populations",
    }:
        return True

    if address == BigQueryAddress(
        dataset_id=CLASSIFICATION_VIEWS_DATASET,
        table_id=ALL_CLASSIFICATION_SCORE_COMPONENTS_VIEW_ID,
    ):
        # Autogenerated union of every classification score component span view, and the
        # classification analog of the TES `all_criteria` view above. Its only consumers
        # are the score component span validations, and a validation alone does not mark
        # a parent view as used (see above), so it needs an explicit exemption.
        return True

    return False


def _get_single_project_unused_addresses(
    all_views_dag_walker: BigQueryViewDagWalker, ignore_exemptions: bool
) -> Set[BigQueryAddress]:
    """Returns views that are unused within a single project. The
    `metadata.project_id()` must be set before this function is called.
    """

    all_in_use_addresses = (
        get_all_metric_export_view_addresses() | LOOKER_REFERENCED_ADDRESSES
    )
    if not ignore_exemptions:
        all_in_use_addresses = all_in_use_addresses | set(
            UNREFERENCED_ADDRESSES_TO_KEEP_WITH_REASON
        )

    def is_view_used(v: BigQueryView, child_results: Dict[BigQueryView, bool]) -> bool:
        if v.address in all_in_use_addresses:
            return True

        if v.address.dataset_id in DATASETS_REFERENCED_BY_MISC_PROCESSES:
            return True

        return any(child_results.values())

    view_results = all_views_dag_walker.process_dag(
        view_process_fn=is_view_used,
        traversal_direction=TraversalDirection.LEAVES_TO_ROOTS,
        synchronous=False,
    ).view_results

    return {
        v.address
        for v, is_used in view_results.items()
        if not is_used and not _should_ignore_unused_address(v.address)
    }


def get_unused_across_all_projects_addresses_from_all_views_dag(
    ignore_exemptions: bool = False,
) -> Set[BigQueryAddress]:
    """Returns the addresses of all views that are not referenced by a product, Looker,
    or marked as used via the UNREFERENCED_ADDRESSES_TO_KEEP_WITH_REASON exemptions
    list. If |ignore_exemptions| is True, will return ALL views that are unused, even if
    that view or one of its children is exempted in
    UNREFERENCED_ADDRESSES_TO_KEEP_WITH_REASON.
    """
    unused_addresses: Optional[Set[BigQueryAddress]] = None
    for project in DATA_PLATFORM_GCP_PROJECTS:
        with local_project_id_override(project):
            project_dag_walker = build_dag_walker_for_all_deployed_view_graphs()

            if len(project_dag_walker.views) == 0:
                raise ValueError(f"Failed to collect views for project {project}.")

            unused_addresses_in_project = _get_single_project_unused_addresses(
                project_dag_walker, ignore_exemptions=ignore_exemptions
            )
            if unused_addresses is None:
                unused_addresses = unused_addresses_in_project
                continue
            # Only keep addresses that were unused in all previous projects
            unused_addresses = unused_addresses.intersection(
                unused_addresses_in_project
            )
    if unused_addresses is None:
        raise ValueError(f"Expected nonnull {unused_addresses} at this point.")
    return unused_addresses


def find_unused_views() -> None:
    print("\nLooking for views that are unused across all projects ... ")
    unused_addresses = get_unused_across_all_projects_addresses_from_all_views_dag()

    if not unused_addresses:
        print("✅ Found no unused BQ views that are eligible for deletion")
        return

    print(
        f"⚠️ Found {len(unused_addresses)} BQ views that are not used in any project "
        f"and may be eligible for deletion:\n"
    )
    for address in sorted(unused_addresses):
        print(address.to_str())

    print(
        "\n⚠️ PLEASE NOTE ⚠️: The information this script has is incomplete. Please "
        "check with the view owner and verify that the view is not referenced in "
        "Looker before deleting."
    )


if __name__ == "__main__":
    find_unused_views()
