"""Refresh pollen (policy scratch) BigQuery views for a given state/project.

Usage:
    # Refresh all views in a project
    python -m recidiviz.tools.pollen.refresh_scratch_views \
        --project_id recidiviz-staging \
        --state_code US_CO \
        --policy_project aet

    # Refresh a single view by short name (upstream deps must already be current)
    python -m recidiviz.tools.pollen.refresh_scratch_views \
        --project_id recidiviz-staging \
        --state_code US_CO \
        --policy_project aet \
        --view_id ratings
"""

import argparse

import recidiviz.research.notebooks.policy as policy_module
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.big_query.view_update_manager import (
    BigQueryViewDagWalkerProcessingFailureMode,
    create_managed_dataset_and_deploy_views_for_view_builders,
)
from recidiviz.common.module_collector_mixin import ModuleCollectorMixin
from recidiviz.utils.metadata import local_project_id_override


def refresh_scratch_views(
    project_id: str,
    state_code: str,
    policy_project: str,
    view_id: str | None = None,
) -> None:
    """Collect and deploy pollen views for the given state/project."""
    view_dir_module = ModuleCollectorMixin.get_relative_module(
        policy_module, [state_code.lower(), policy_project, "views"]
    )
    builders = BigQueryViewCollector.collect_view_builders_in_module(
        builder_type=SimpleBigQueryViewBuilder,
        view_dir_module=view_dir_module,
        recurse=True,
    )
    if view_id is not None:
        full_view_id = f"{state_code.lower()}_{policy_project}_{view_id}"
        builders = [b for b in builders if b.view_id == full_view_id]
        if not builders:
            raise ValueError(
                f"No view '{view_id}' found in {state_code}/{policy_project}"
            )
    with local_project_id_override(project_id):
        create_managed_dataset_and_deploy_views_for_view_builders(
            view_builders_to_update=builders,
            historically_managed_datasets_to_clean=None,
            rematerialize_changed_views_only=False,
            failure_mode=BigQueryViewDagWalkerProcessingFailureMode.FAIL_EXHAUSTIVELY,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project_id", required=True, help="GCP project ID")
    parser.add_argument(
        "--state_code",
        required=True,
        help="State code, e.g. US_CO",
    )
    parser.add_argument(
        "--policy_project",
        required=True,
        help="Policy project name, e.g. aet",
    )
    parser.add_argument(
        "--view_id",
        default=None,
        help="Short view name to refresh (e.g. 'ratings'). Omit to refresh all views.",
    )
    args = parser.parse_args()
    refresh_scratch_views(
        project_id=args.project_id,
        state_code=args.state_code,
        policy_project=args.policy_project,
        view_id=args.view_id,
    )


if __name__ == "__main__":
    main()
