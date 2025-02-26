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

For any of the above commands, you can add a `--prompt` flag BEFORE the auto/manual/all
keyword. This will make the script ask you if you want to proceed with loading the
views after we've discovered all the views to be loaded to the sandbox. For example:
    python -m recidiviz.tools.load_views_to_sandbox \
       --sandbox_dataset_prefix [SANDBOX_DATASET_PREFIX] --prompt auto

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
    prompt: bool,
    dataflow_dataset_override: Optional[str] = None,
) -> None:
    """Loads ALL views to sandbox datasets with prefix |sandbox_dataset_prefix|. If
    |dataflow_dataset_override| is not set, excludes views in
    DATAFLOW_METRICS_MATERIALIZED_DATASET, which is very expensive to materialize.
    """
    prompt_for_confirmation("This will load all sandbox views, continue?")

    logging.info("Gathering views to load to sandbox...")
    collected_builders = _AllButSomeBigQueryViewsCollector(
        datasets_to_exclude=_datasets_to_exclude_from_sandbox(dataflow_dataset_override)
    ).collect_view_builders()
    _load_collected_views_to_sandbox(
        sandbox_dataset_prefix=sandbox_dataset_prefix,
        prompt=prompt,
        collected_builders=collected_builders,
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
    prompt: bool,
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

    logging.info("Gathering views to load to sandbox...")
    collected_builders = collector.collect_view_builders()
    _load_collected_views_to_sandbox(
        sandbox_dataset_prefix=sandbox_dataset_prefix,
        prompt=prompt,
        dataflow_dataset_override=dataflow_dataset_override,
        collected_builders=collected_builders,
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
    address_to_change_type: Dict[BigQueryAddress, ViewChangeType],
    changed_datasets_to_ignore: Optional[List[str]],
) -> Set[BigQueryAddress]:
    """Returns addresses for all views that have been added / updated since the last
    deploy and whose dataset is not in |changed_datasets_to_ignore|.
    """
    added_view_addresses = set()
    updated_view_addresses = set()
    ignored_changed_addresses = set()
    for address, change_type in address_to_change_type.items():
        if (
            changed_datasets_to_ignore
            and address.dataset_id in changed_datasets_to_ignore
        ):
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

    return set(address_to_change_type) - ignored_changed_addresses


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


def _check_for_invalid_ignores(
    full_dag_walker: BigQueryViewDagWalker,
    address_to_change_type: Dict[BigQueryAddress, ViewChangeType],
    addresses_to_load: Set[BigQueryAddress],
) -> None:
    """Exits if any views that will be loaded (in |addresses_to_load|) are downstream
    of an ingored view that has not yet been deployed.
    """
    ignored_added_views = []
    invalid_views_to_load = []

    def find_invalid_ignores(
        v: BigQueryView, parent_results: Dict[BigQueryView, bool]
    ) -> bool:
        view_change_type = (
            address_to_change_type[v.address]
            if v.address in address_to_change_type
            else None
        )

        is_downstream_of_new_ignored_view = any(parent_results.values())

        if v.address not in addresses_to_load:
            if not view_change_type:
                # All references to this downstream view will be to the normal version
                # in BQ.
                return False

            if view_change_type == ViewChangeType.ADDED:
                ignored_added_views.append(v.address)
                return True
            return is_downstream_of_new_ignored_view

        if is_downstream_of_new_ignored_view:
            # This is a view that will be loaded to the sandbox that is downstream of
            # a new view that will NOT be loaded to the sandbox. This view (or any
            # changed views upstream of this view) must be ignored as well.
            invalid_views_to_load.append(v.address)
        return is_downstream_of_new_ignored_view

    full_dag_walker.process_dag(find_invalid_ignores)

    if invalid_views_to_load:
        logging.error(
            "Attempting to load these views which are children of views that have been "
            "added since the last deploy but were excluded from this sandbox run:\n%s",
            _sorted_address_list_str(invalid_views_to_load),
        )
        logging.error(
            "New views excluded from this sandbox run:\n%s",
            _sorted_address_list_str(ignored_added_views),
        )
        logging.error(
            "You must include these new views in the sandbox load or ignore all "
            "changed downstream parents as well."
        )
        sys.exit(1)


def _load_views_changed_on_branch_to_sandbox(
    *,
    sandbox_dataset_prefix: str,
    prompt: bool,
    dataflow_dataset_override: Optional[str],
    changed_datasets_to_ignore: Optional[List[str]],
) -> None:
    """Loads all views that have changed on this branch as compared to what is deployed
    to the current project (usually staging).
    """
    _confirm_rebased_on_latest_deploy()

    view_builders_in_full_dag = deployed_view_builders(project_id())

    all_views = build_views_to_update(
        view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
        candidate_view_builders=view_builders_in_full_dag,
        address_overrides=None,
    )
    logging.info("Constructing DAG with all known views...")
    full_dag_walker = BigQueryViewDagWalker(all_views)

    logging.info("Checking for changes against [%s] BigQuery...", project_id())
    address_to_change_type = _get_all_views_changed_on_branch(full_dag_walker)

    changed_views_to_load = _get_changed_views_to_load_to_sandbox(
        address_to_change_type, changed_datasets_to_ignore
    )

    if not changed_views_to_load:
        logging.warning(
            "Did not find any changed views to load to the sandbox. Exiting."
        )
        sys.exit(1)

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
    logging.info("Gathering views to load to sandbox...")
    collected_builders = sub_dag_collector.collect_view_builders()

    _check_for_invalid_ignores(
        full_dag_walker=full_dag_walker,
        address_to_change_type=address_to_change_type,
        addresses_to_load={b.address for b in collected_builders},
    )

    _load_collected_views_to_sandbox(
        sandbox_dataset_prefix=sandbox_dataset_prefix,
        prompt=prompt,
        dataflow_dataset_override=dataflow_dataset_override,
        collected_builders=collected_builders,
    )


def _sorted_address_list_str(addresses: Iterable[BigQueryAddress]) -> str:
    return "\n".join([f"- {a.dataset_id}.{a.table_id}" for a in sorted(addresses)])


def _load_collected_views_to_sandbox(
    *,
    sandbox_dataset_prefix: str,
    prompt: bool,
    collected_builders: List[BigQueryViewBuilder],
    dataflow_dataset_override: Optional[str] = None,
) -> None:
    logging.info(
        "Will load the following views to the sandbox: \n%s",
        _sorted_address_list_str({b.address for b in collected_builders}),
    )
    if prompt:
        prompt_for_confirmation(
            f"Continue with loading {len(collected_builders)} views?"
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
        "--prompt",
        dest="prompt",
        action="store_true",
        default=False,
        help="If true, the script will prompt and ask if you want to continue after "
        "the full list of views that will be loaded has been printed to the "
        "console.",
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
                prompt=args.prompt,
                dataflow_dataset_override=args.dataflow_dataset_override,
            )
        elif args.chosen_mode == "manual":
            _load_manually_filtered_views_to_sandbox(
                sandbox_dataset_prefix=args.sandbox_dataset_prefix,
                prompt=args.prompt,
                dataflow_dataset_override=args.dataflow_dataset_override,
                view_ids_to_load=args.view_ids_to_load,
                dataset_ids_to_load=args.dataset_ids_to_load,
                update_ancestors=args.update_ancestors,
                update_descendants=args.update_descendants,
            )
        elif args.chosen_mode == "auto":
            _load_views_changed_on_branch_to_sandbox(
                sandbox_dataset_prefix=args.sandbox_dataset_prefix,
                prompt=args.prompt,
                dataflow_dataset_override=args.dataflow_dataset_override,
                changed_datasets_to_ignore=args.changed_datasets_to_ignore,
            )
        else:
            raise ValueError(f"Unexpected load to sandbox mode: [{args.chosen_mode}]")
