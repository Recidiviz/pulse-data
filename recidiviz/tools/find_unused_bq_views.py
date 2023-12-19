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

from typing import Dict, Set

from recidiviz.aggregated_metrics.dataset_config import AGGREGATED_METRICS_DATASET_ID
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.big_query.big_query_view_dag_walker import BigQueryViewDagWalker
from recidiviz.calculator.query.externally_shared_views.dataset_config import (
    EXTERNALLY_SHARED_VIEWS_DATASET,
)
from recidiviz.calculator.query.state.dataset_config import (
    POPULATION_PROJECTION_DATASET,
    REFERENCE_VIEWS_DATASET,
    SPARK_OUTPUT_DATASET_MOST_RECENT,
)
from recidiviz.calculator.query.state.views.analyst_data.us_pa.us_pa_raw_required_treatment import (
    US_PA_RAW_REQUIRED_TREATMENT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_compliant_reporting_funnel import (
    US_TN_COMPLIANT_REPORTING_FUNNEL_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_compliant_reporting_guardrail import (
    US_TN_COMPLIANT_REPORTING_GUARDRAIL_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.housing_unit_type_collapsed_solitary_sessions import (
    HOUSING_UNIT_TYPE_COLLAPSED_SOLITARY_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.current_impact_funnel_status import (
    CURRENT_IMPACT_FUNNEL_STATUS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.workflows_usage import (
    WORKFLOWS_USAGE_VIEW_BUILDER,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.metrics.export.export_config import VIEW_COLLECTION_EXPORT_INDEX
from recidiviz.pipelines.utils.pipeline_run_utils import collect_all_pipeline_classes
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views.dataset_config import EXTERNAL_ACCURACY_DATASET
from recidiviz.validation.views.dataset_config import (
    VIEWS_DATASET as VALIDATION_VIEWS_DATASET,
)
from recidiviz.view_registry.deployed_views import build_all_deployed_views_dag_walker

# List of views that are definitely not referenced in Looker (as of 11/29/23). This list
# is # incomplete and you should add to this list / update the date in this comment as
# you work with this script.
CONFIRMED_NOT_IN_LOOKER_ADDRESSES: set[BigQueryAddress] = set()

# List of views that are definitely referenced in Looker (as of 11/29/23). This list is
# incomplete and you should add to this list / update the date in this comment as you
# work with this script.
LOOKER_REFERENCED_ADDRESSES = {
    WORKFLOWS_USAGE_VIEW_BUILDER.address,
    CURRENT_IMPACT_FUNNEL_STATUS_VIEW_BUILDER.address,
    # TODO(Recidiviz/looker#521): The dashboard referencing this view is no longer
    #  needed and can be deleted.
    US_PA_RAW_REQUIRED_TREATMENT_VIEW_BUILDER.address,
    # TODO(Recidiviz/looker#427): The legacy TN compliant reporting dashboard
    #  referencing this view is actively in use because it has more functionality than
    #  any existing dashboard for standard workflows infra, and it cannot be deleted
    #  until we have an adequate replacement.
    US_TN_COMPLIANT_REPORTING_FUNNEL_VIEW_BUILDER.address,
    # TODO(Recidiviz/looker#520): The dashboard referencing this view is no longer
    #  needed and can be deleted.
    US_TN_COMPLIANT_REPORTING_GUARDRAIL_VIEW_BUILDER.address,
}

# List of views that are not referenced in Looker but should still be kept around,
# listed along with the reason we still need to keep them. Please be as descriptive
# as possible when updating this list, including a point of contact and date we were
# still using this view where possible.
OTHER_ADDRESSES_TO_KEEP_WITH_REASON = {
    HOUSING_UNIT_TYPE_COLLAPSED_SOLITARY_SESSIONS_VIEW_BUILDER.address: (
        "This view was created relatively recently (5/31/23) and may still be in use "
        "for ad-hoc analysis."
    )
}

DATASETS_REFERENCED_BY_MISC_PROCESSES = {
    # All views in this dataset are referenced in Looker via autogenerated views
    AGGREGATED_METRICS_DATASET_ID,
    # These are validation views that compare external reference data to actual counts.
    EXTERNAL_ACCURACY_DATASET,
    # Views used in CSG export
    EXTERNALLY_SHARED_VIEWS_DATASET,
    # These views are inputs to Spark population projection modeling
    POPULATION_PROJECTION_DATASET,
    # Contains views referencing the output of Spark population projection runs. These
    # views are used to build Spark reports.
    SPARK_OUTPUT_DATASET_MOST_RECENT,
}


def _get_all_metric_export_addresses() -> Set[BigQueryAddress]:
    export_addresses = set()
    for export_config in VIEW_COLLECTION_EXPORT_INDEX.values():
        export_addresses |= {vb.address for vb in export_config.view_builders_to_export}
    return export_addresses


def _get_all_dataflow_pipeline_referenced_addresses() -> Set[BigQueryAddress]:
    pipeline_addresses = set()
    for pipeline_class in collect_all_pipeline_classes():
        pipeline_addresses |= {
            BigQueryAddress(dataset_id=REFERENCE_VIEWS_DATASET, table_id=table_id)
            for table_id in pipeline_class.all_required_reference_table_ids()
        }

    return pipeline_addresses


def _should_ignore_unused_address(address: BigQueryAddress) -> bool:
    """Returns true for views that may not be in use but can be ignored if they happen
    to be. These views are not tracked in OTHER_ADDRESSES_TO_KEEP_WITH_REASON because
    they aren't views that are important enough to keep that parent views should be
    marked as "used".
    """
    if address.dataset_id in {
        # We don't mark these views as "used" because the existence of a validation does
        # not in itself mean we need to keep a parent view, but we generally don't
        # expect these views to be referenced by other views.
        VALIDATION_VIEWS_DATASET,
        # We autogenerate these views out of convenience and don't expect all to be used
        *{
            raw_latest_views_dataset_for_region(state_code, instance)
            for state_code in StateCode
            for instance in DirectIngestInstance
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
    }:
        return True
    return False


def get_unused_addresses_from_all_views_dag(
    all_views_dag_walker: BigQueryViewDagWalker,
) -> Set[BigQueryAddress]:
    """Returns the addresses of all views that are either"""
    if intersection := LOOKER_REFERENCED_ADDRESSES.intersection(
        CONFIRMED_NOT_IN_LOOKER_ADDRESSES
    ):
        raise ValueError(
            f"Found addresses listed both in LOOKER_REFERENCED_ADDRESSES and "
            f"CONFIRMED_NOT_IN_LOOKER_ADDRESSES: {intersection}"
        )

    all_in_use_addresses = (
        _get_all_metric_export_addresses()
        | _get_all_dataflow_pipeline_referenced_addresses()
        | LOOKER_REFERENCED_ADDRESSES
        | set(OTHER_ADDRESSES_TO_KEEP_WITH_REASON)
    )

    def is_view_used(v: BigQueryView, child_results: Dict[BigQueryView, bool]) -> bool:
        if v.address in all_in_use_addresses:
            return True

        if v.address.dataset_id in DATASETS_REFERENCED_BY_MISC_PROCESSES:
            return True

        return any(child_results.values())

    view_results = all_views_dag_walker.process_dag(
        view_process_fn=is_view_used, reverse=True, synchronous=False
    ).view_results

    return {
        v.address
        for v, is_used in view_results.items()
        if not is_used and not _should_ignore_unused_address(v.address)
    }


def main() -> None:
    dag_walker = build_all_deployed_views_dag_walker()
    unused_addresses = get_unused_addresses_from_all_views_dag(dag_walker)

    if not unused_addresses:
        print("✅ Found no unused BQ views that are eligible for deletion")
        return

    print(
        f"⚠️ Found {len(unused_addresses)} BQ views that may be eligible for deletion:\n"
    )
    for address in sorted(unused_addresses):
        is_leaf = dag_walker.nodes_by_address[address].is_leaf
        is_leaf_str = " [*** LEAF NODE ***]" if is_leaf else ""
        not_in_looker_str = (
            " [*** CONFIRMED NOT IN LOOKER ***]"
            if address in CONFIRMED_NOT_IN_LOOKER_ADDRESSES
            else ""
        )
        print(f"{address.to_str()}{is_leaf_str}{not_in_looker_str}")

    print(
        "\n⚠️ PLEASE NOTE ⚠️: The information this script has is incomplete. Please "
        "check with the view owner and verify that the view is not referenced in "
        "Looker before deleting."
    )


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        main()
