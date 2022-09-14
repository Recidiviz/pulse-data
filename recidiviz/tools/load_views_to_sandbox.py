# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""A script for writing regularly updated views in BigQuery to a set of temporary
datasets. Used during development to test updates to views.

To load all views that have changed since the last view deploy, run:
    python -m recidiviz.tools.load_views_to_sandbox \
       --sandbox_dataset_prefix [SANDBOX_DATASET_PREFIX] auto


To load all views that have changed since the last view deploy, but exclude some
datasets when considering which views have changed, run:
    python -m recidiviz.tools.load_views_to_sandbox \
       --sandbox_dataset_prefix [SANDBOX_DATASET_PREFIX] auto \
       --changed_datasets_to_ignore [DATASET_ID_1,DATASET_ID_2,...]

To manually choose which views to load, run:
    python -m recidiviz.tools.load_views_to_sandbox \
        --sandbox_dataset_prefix [SANDBOX_DATASET_PREFIX] manual \
        --view_ids_to_load [DATASET_ID_1.VIEW_ID_1,DATASET_ID_2.VIEW_ID_2,...] \
        --dataset_ids_to_load [DATASET_ID_1,DATASET_ID_2,...] \
        --update_ancestors [True,False] \
        --update_descendants [True,False]

To load ALL views to a sandbox (this should be used only in rare circumstances), run:
    python -m recidiviz.tools.load_views_to_sandbox \
       --sandbox_dataset_prefix [SANDBOX_DATASET_PREFIX] all


To view more info on arguments that can be used with any of the sub-commands (i.e. auto,
manual, all), run:
   python -m recidiviz.tools.load_views_to_sandbox --help


To view more info on arguments that can be used with any particular sub-command, run:
   python -m recidiviz.tools.load_views_to_sandbox auto --help

   OR

   python -m recidiviz.tools.load_views_to_sandbox all --help

   OR

   python -m recidiviz.tools.load_views_to_sandbox manual --help
"""
import argparse
import logging
import sys
from enum import Enum
from typing import Dict, Iterable, List, Optional, Set

from google.api_core import exceptions

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.big_query_view import BigQueryView, BigQueryViewBuilder
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.big_query.big_query_view_dag_walker import BigQueryViewDagWalker
from recidiviz.big_query.big_query_view_sub_dag_collector import (
    BigQueryViewSubDagCollector,
)
from recidiviz.big_query.view_update_manager import (
    build_views_to_update,
    create_managed_dataset_and_deploy_views_for_view_builders,
)
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
)
from recidiviz.common.git import (
    get_hash_of_deployed_commit,
    is_commit_in_current_branch,
)
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override, project_id
from recidiviz.utils.params import str_to_bool
from recidiviz.view_registry.address_overrides_factory import (
    address_overrides_for_view_builders,
)
from recidiviz.view_registry.datasets import VIEW_SOURCE_TABLE_DATASETS
from recidiviz.view_registry.deployed_views import deployed_view_builders


class _AllButSomeBigQueryViewsCollector(BigQueryViewCollector[BigQueryViewBuilder]):
    def __init__(self, datasets_to_exclude: Set[str]):
        self.datasets_to_exclude = datasets_to_exclude

    def collect_view_builders(self) -> List[BigQueryViewBuilder]:
        all_deployed_builders = deployed_view_builders(project_id())
        return [
            builder
            for builder in all_deployed_builders
            if builder.dataset_id not in self.datasets_to_exclude
        ]


def load_all_views_to_sandbox(
    sandbox_dataset_prefix: str,
    dataflow_dataset_override: Optional[str] = None,
) -> None:
    """Loads ALL views to sandbox datasets with prefix |sandbox_dataset_prefix|. If
    |dataflow_dataset_override| is not set, excludes views in
    DATAFLOW_METRICS_MATERIALIZED_DATASET, which is very expensive to materialize.
    """
    prompt_for_confirmation("This will load all sandbox views, continue?")

    _load_collected_views_to_sandbox(
        sandbox_dataset_prefix=sandbox_dataset_prefix,
        sandbox_view_builder_collector=_AllButSomeBigQueryViewsCollector(
            datasets_to_exclude=_datasets_to_exclude_from_sandbox(
                dataflow_dataset_override
            )
        ),
        dataflow_dataset_override=dataflow_dataset_override,
    )


def _datasets_to_exclude_from_sandbox(
    dataflow_dataset_override: Optional[str] = None,
) -> Set[str]:
    datasets_to_exclude = set()
    # Only update views in the DATAFLOW_METRICS_MATERIALIZED_DATASET if the
    # dataflow_dataset_override is set
    if not dataflow_dataset_override:
        datasets_to_exclude.add(DATAFLOW_METRICS_MATERIALIZED_DATASET)
    return datasets_to_exclude


def _load_manually_filtered_views_to_sandbox(
    *,
    sandbox_dataset_prefix: str,
    dataflow_dataset_override: Optional[str],
    view_ids_to_load: Optional[List[str]],
    dataset_ids_to_load: Optional[List[str]],
    update_ancestors: bool,
    update_descendants: bool,
) -> None:
    """Loads all views into sandbox datasets prefixed with the sandbox_dataset_prefix.

    Params
    ------
    sandbox_dataset_prefix : str
        Loads datasets that all take the form of
        "<sandbox_dataset_prefix>_<original dataset_id>

    dataflow_dataset_override : Optional[str]
        If True, updates views in the DATAFLOW_METRICS_MATERIALIZED_DATASET

    view_ids_to_load : Optional[List[str]]
        If specified, only loads views whose view_id matches one of the listed view_ids.
        View_ids must take the form dataset_id.view_id

    dataset_ids_to_load : Optional[List[str]]
        If specified, only loads views whose dataset_id matches one of the listed
        dataset_ids.

    update_descendants : bool, default True
        Only applied if `view_ids_to_load` or `dataset_ids_to_load` is included.
        If True, loads descendant views of views found in the two aforementioned
        parameters.

    update_ancestors : bool, default False
        To be used with `view_ids_to_load` or `dataset_ids_to_load`. If True, loads
        ancestor/parent views of views found in the two aforementioned parameters.
    """

    if not view_ids_to_load and not dataset_ids_to_load:
        raise ValueError(
            "Must define at least one of view_ids_to_load or dataset_ids_to_load"
        )

    logging.info("Prefixing all view datasets with [%s_].", sandbox_dataset_prefix)

    addresses_from_view_ids = (
        {
            BigQueryAddress(
                dataset_id=view_id_str.split(".")[0],
                table_id=view_id_str.split(".")[1],
            )
            for view_id_str in view_ids_to_load
        }
        if view_ids_to_load
        else None
    )

    view_builders = deployed_view_builders(project_id())

    collector = BigQueryViewSubDagCollector(
        view_builders_in_full_dag=view_builders,
        view_addresses_in_sub_dag=addresses_from_view_ids,
        dataset_ids_in_sub_dag=(
            set(dataset_ids_to_load) if dataset_ids_to_load else None
        ),
        include_ancestors=update_ancestors,
        include_descendants=update_descendants,
        datasets_to_exclude=_datasets_to_exclude_from_sandbox(
            dataflow_dataset_override
        ),
    )

    _load_collected_views_to_sandbox(
        sandbox_dataset_prefix=sandbox_dataset_prefix,
        dataflow_dataset_override=dataflow_dataset_override,
        sandbox_view_builder_collector=collector,
    )


class ViewChangeType(Enum):
    ADDED = "ADDED"
    UPDATED = "UPDATED"


def _get_all_views_changed_on_branch(
    full_dag_walker: BigQueryViewDagWalker,
) -> Dict[BigQueryAddress, ViewChangeType]:
    bq_client = BigQueryClientImpl()

    def check_for_change(
        v: BigQueryView, _parent_results: Dict[BigQueryView, Optional[ViewChangeType]]
    ) -> Optional[ViewChangeType]:
        try:
            t = bq_client.get_table(
                bq_client.dataset_ref_for_id(v.address.dataset_id),
                v.address.table_id,
            )
        except exceptions.NotFound:
            if not v.should_deploy():
                return None

            return ViewChangeType.ADDED

        if t.view_query != v.view_query:
            return ViewChangeType.UPDATED

        return None

    results = full_dag_walker.process_dag(check_for_change)
    return {
        view.address: change_type
        for view, change_type in results.items()
        if change_type
    }


def _get_changed_views_to_load_to_sandbox(
    view_builders_in_full_dag: List[BigQueryViewBuilder],
    changed_datasets_to_ignore: Optional[List[str]],
) -> Set[BigQueryAddress]:
    """Returns addresses for all views that have been added / updated since the last
    deploy and whose dataset is not in |changed_datasets_to_ignore|.
    """
    all_views = build_views_to_update(
        view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
        candidate_view_builders=view_builders_in_full_dag,
        address_overrides=None,
    )
    logging.info("Constructing DAG with all known views...")
    full_dag_walker = BigQueryViewDagWalker(all_views)
    logging.info("Checking for changes against [%s] BigQuery...", project_id())
    changed_view_addresses = _get_all_views_changed_on_branch(full_dag_walker)

    added_view_addresses = set()
    updated_view_addresses = set()
    ignored_changed_addresses = set()
    for address, change_type in changed_view_addresses.items():
        if (
            changed_datasets_to_ignore
            and address.dataset_id in changed_datasets_to_ignore
        ):
            if change_type == ViewChangeType.ADDED:
                logging.warning(
                    "Cannot exclude newly added view [%s] from sandbox - including "
                    "even though dataset [%s] is ignored.",
                    address,
                    address.dataset_id,
                )
            else:
                ignored_changed_addresses.add(address)

        if change_type == ViewChangeType.ADDED:
            added_view_addresses.add(address)
        elif change_type == ViewChangeType.UPDATED:
            updated_view_addresses.add(address)
        else:
            raise ValueError(
                f"Unexpected change_type [{change_type}] for view [{address}]"
            )

    if added_view_addresses:
        logging.info(
            "Found the following view(s) which have been ADDED \n%s",
            _sorted_address_list_str(added_view_addresses),
        )
    if updated_view_addresses:
        logging.info(
            "Found the following view(s) which have been UPDATED \n%s",
            _sorted_address_list_str(updated_view_addresses),
        )
    if ignored_changed_addresses:
        logging.info(
            "IGNORING changes to the following view(s) \n%s",
            _sorted_address_list_str(ignored_changed_addresses),
        )

    return set(changed_view_addresses) - ignored_changed_addresses


def _confirm_rebased_on_latest_deploy() -> None:
    last_deployed_commit = get_hash_of_deployed_commit(project_id())

    if not is_commit_in_current_branch(last_deployed_commit):
        logging.error(
            "Cannot find commit [%s] in the current branch, which is the commit last "
            "deployed to [%s]. You must rebase this branch before using `auto` mode.",
            last_deployed_commit,
            project_id(),
        )
        sys.exit(1)


def _load_views_changed_on_branch_to_sandbox(
    *,
    sandbox_dataset_prefix: str,
    dataflow_dataset_override: Optional[str],
    changed_datasets_to_ignore: Optional[List[str]],
) -> None:
    _confirm_rebased_on_latest_deploy()

    view_builders_in_full_dag = deployed_view_builders(project_id())

    changed_views_to_load = _get_changed_views_to_load_to_sandbox(
        view_builders_in_full_dag, changed_datasets_to_ignore
    )

    sub_dag_collector = BigQueryViewSubDagCollector(
        view_builders_in_full_dag=view_builders_in_full_dag,
        view_addresses_in_sub_dag=changed_views_to_load,
        dataset_ids_in_sub_dag=None,
        include_ancestors=False,
        include_descendants=True,
        datasets_to_exclude=_datasets_to_exclude_from_sandbox(
            dataflow_dataset_override
        ),
    )

    _load_collected_views_to_sandbox(
        sandbox_dataset_prefix=sandbox_dataset_prefix,
        dataflow_dataset_override=dataflow_dataset_override,
        sandbox_view_builder_collector=sub_dag_collector,
    )


def _sorted_address_list_str(addresses: Iterable[BigQueryAddress]) -> str:
    return "\n".join([f"- {a.dataset_id}.{a.table_id}" for a in sorted(addresses)])


def _load_collected_views_to_sandbox(
    *,
    sandbox_dataset_prefix: str,
    sandbox_view_builder_collector: BigQueryViewCollector,
    dataflow_dataset_override: Optional[str] = None,
) -> None:

    logging.info("Gathering views to load to sandbox...")

    collected_builders = sandbox_view_builder_collector.collect_view_builders()
    logging.info(
        "Will load the following views to the sandbox: \n%s",
        _sorted_address_list_str({b.address for b in collected_builders}),
    )

    logging.info("Updating %s views...", len(collected_builders))

    sandbox_address_overrides = address_overrides_for_view_builders(
        view_dataset_override_prefix=sandbox_dataset_prefix,
        view_builders=collected_builders,
        dataflow_dataset_override=dataflow_dataset_override,
    )

    create_managed_dataset_and_deploy_views_for_view_builders(
        view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
        view_builders_to_update=collected_builders,
        address_overrides=sandbox_address_overrides,
        # Don't clean up datasets when running a sandbox script
        historically_managed_datasets_to_clean=None,
    )


def parse_arguments() -> argparse.Namespace:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project_id",
        dest="project_id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        default=GCP_PROJECT_STAGING,
        required=False,
    )

    parser.add_argument(
        "--sandbox_dataset_prefix",
        dest="sandbox_dataset_prefix",
        help="A prefix to append to all names of the datasets where these views will "
        "be loaded.",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--dataflow_dataset_override",
        dest="dataflow_dataset_override",
        help="An override of the dataset containing Dataflow metric output that the "
        "updated views should reference.",
        type=str,
        required=False,
    )

    subparsers = parser.add_subparsers(
        title="Load to sandbox modes",
        description="Valid load_views_to_sandbox subcommands",
        help="additional help",
        dest="chosen_mode",
    )
    parser_manual = subparsers.add_parser("manual")

    parser_manual.add_argument(
        "--view_ids_to_load",
        dest="view_ids_to_load",
        help="A list of view_ids to load separated by commas. If provided, only loads "
        "views in this list (plus dependencies, if applicable). View_ids must take the "
        "form dataset_id.view_id. Can be called together with the "
        "--dataset_ids_to_load argument as long as none of the datasets overlap.",
        type=str_to_list,
        required=False,
    )

    parser_manual.add_argument(
        "--dataset_ids_to_load",
        dest="dataset_ids_to_load",
        help="A list of dataset_ids to load separated by commas. If provided, only "
        "loads datasets in this list (plus dependencies, if applicable).",
        type=str_to_list,
        required=False,
    )

    parser_manual.add_argument(
        "--update_ancestors",
        dest="update_ancestors",
        help="If True, will load views that are ancestors of those provided in "
        "view_ids_to_load or dataset_ids_to_load.",
        type=str_to_bool,
        default=False,
        required=False,
    )

    parser_manual.add_argument(
        "--update_descendants",
        dest="update_descendants",
        help="If True, will load views that are descendants of those provided in "
        "view_ids_to_load or dataset_ids_to_load.",
        type=str_to_bool,
        default=True,
        required=False,
    )

    parser_auto = subparsers.add_parser("auto")
    parser_auto.add_argument(
        "--changed_datasets_to_ignore",
        dest="changed_datasets_to_ignore",
        help="A list of dataset ids (comma-separated) for datasets we should skip when "
        "detecting which views have changed. Views in these datasets will still "
        "be loaded to the sandbox if they are downstream of other views not in these "
        "datasets which have been changed.",
        type=str_to_list,
        required=False,
    )

    subparsers.add_parser("all")

    return parser.parse_args()


def str_to_list(list_str: str) -> List[str]:
    """
    Separates strings by commas and returns a list
    """
    return list_str.split(",")


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    args = parse_arguments()

    with local_project_id_override(args.project_id):
        if args.chosen_mode == "all":
            load_all_views_to_sandbox(
                sandbox_dataset_prefix=args.sandbox_dataset_prefix,
                dataflow_dataset_override=args.dataflow_dataset_override,
            )
        elif args.chosen_mode == "manual":
            _load_manually_filtered_views_to_sandbox(
                sandbox_dataset_prefix=args.sandbox_dataset_prefix,
                dataflow_dataset_override=args.dataflow_dataset_override,
                view_ids_to_load=args.view_ids_to_load,
                dataset_ids_to_load=args.dataset_ids_to_load,
                update_ancestors=args.update_ancestors,
                update_descendants=args.update_descendants,
            )
        elif args.chosen_mode == "auto":
            _load_views_changed_on_branch_to_sandbox(
                sandbox_dataset_prefix=args.sandbox_dataset_prefix,
                dataflow_dataset_override=args.dataflow_dataset_override,
                changed_datasets_to_ignore=args.changed_datasets_to_ignore,
            )
        else:
            raise ValueError(f"Unexpected load to sandbox mode: [{args.chosen_mode}]")
