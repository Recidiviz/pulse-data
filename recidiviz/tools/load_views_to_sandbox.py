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
       --sandbox_dataset_prefix [SANDBOX_DATASET_PREFIX] auto \
       --load_changed_views_only

To load all views that have changed since the last view deploy and views
downstream of those views, stopping once we've loaded all views specified by
--load_up_to_addresses and --load_up_to_datasets, run:
    python -m recidiviz.tools.load_views_to_sandbox \
       --sandbox_dataset_prefix [SANDBOX_DATASET_PREFIX] auto
       --load_up_to_addresses [DATASET_ID_1.VIEW_ID_1,DATASET_ID_2.VIEW_ID_2,...]
       --load_up_to_datasets [DATASET_ID_1,DATASET_ID_2,...]

To load all views that have changed since the last view deploy, but exclude some
datasets when considering which views have changed, run:
    python -m recidiviz.tools.load_views_to_sandbox \
       --sandbox_dataset_prefix [SANDBOX_DATASET_PREFIX] auto \
       --changed_datasets_to_ignore [DATASET_ID_1,DATASET_ID_2,...] \
       --load_changed_views_only

To load all views that have changed since the last view deploy, but only look in some
datasets when considering which views have changed, run:
    python -m recidiviz.tools.load_views_to_sandbox \
       --sandbox_dataset_prefix [SANDBOX_DATASET_PREFIX] auto \
       --changed_datasets_to_include [DATASET_ID_1,DATASET_ID_2,...]

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

To load all views downstream of a sandbox dataflow dataset, run:
    python -m recidiviz.tools.load_views_to_sandbox \
       --dataflow_dataset_override [SANDBOX_DATAFLOW_DATASET] \
       --sandbox_dataset_prefix [SANDBOX_DATASET_PREFIX] manual \
       --dataset_ids_to_load dataflow_metrics_materialized

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

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.big_query_view import BigQueryView, BigQueryViewBuilder
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.big_query.big_query_view_dag_walker import BigQueryViewDagWalker
from recidiviz.big_query.big_query_view_sub_dag_collector import (
    BigQueryViewSubDagCollector,
)
from recidiviz.big_query.build_views_to_update import build_views_to_update
from recidiviz.big_query.union_all_big_query_view_builder import (
    UnionAllBigQueryViewBuilder,
)
from recidiviz.big_query.view_update_manager import (
    BigQueryViewUpdateSandboxContext,
    create_managed_dataset_and_deploy_views_for_view_builders,
)
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
)
from recidiviz.common.git import (
    get_hash_of_deployed_commit,
    is_commit_in_current_branch,
)
from recidiviz.source_tables.collect_all_source_table_configs import (
    get_all_source_table_datasets,
)
from recidiviz.tools.utils.arg_parsers import str_to_address_list
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override, project_id
from recidiviz.utils.params import str_to_bool, str_to_list
from recidiviz.view_registry.address_overrides_factory import (
    address_overrides_for_input_source_tables,
)
from recidiviz.view_registry.deployed_views import deployed_view_builders


class _AllButSomeBigQueryViewsCollector(BigQueryViewCollector[BigQueryViewBuilder]):
    def __init__(self, datasets_to_exclude: Set[str]):
        self.datasets_to_exclude = datasets_to_exclude

    def collect_view_builders(self) -> List[BigQueryViewBuilder]:
        all_deployed_builders = deployed_view_builders()
        return [
            builder
            for builder in all_deployed_builders
            if builder.dataset_id not in self.datasets_to_exclude
        ]


def load_all_views_to_sandbox(
    *,
    sandbox_dataset_prefix: str,
    prompt: bool,
    dataflow_dataset_override: Optional[str],
    filter_union_all: bool,
    allow_slow_views: bool,
) -> None:
    """Loads ALL views to sandbox datasets with prefix |sandbox_dataset_prefix|. If
    |dataflow_dataset_override| is not set, excludes views in
    DATAFLOW_METRICS_MATERIALIZED_DATASET, which is very expensive to materialize.
    """
    prompt_for_confirmation(
        "This will load ALL sandbox views. This can be a very slow / expensive "
        "operation. If you just want to test that your view changes do not introduce "
        "any view compilation issues, you can check with the tests in "
        "recidiviz/tests/big_query/view_graph_validation_test.py. Are you sure you "
        "want to continue?"
    )

    logging.info("Gathering views to load to sandbox...")
    collected_builders = _AllButSomeBigQueryViewsCollector(
        datasets_to_exclude=_datasets_to_exclude_from_sandbox(dataflow_dataset_override)
    ).collect_view_builders()
    _load_collected_views_to_sandbox(
        sandbox_dataset_prefix=sandbox_dataset_prefix,
        prompt=prompt,
        collected_builders=collected_builders,
        dataflow_dataset_override=dataflow_dataset_override,
        filter_union_all=filter_union_all,
        allow_slow_views=allow_slow_views,
    )


def _datasets_to_exclude_from_sandbox(
    dataflow_dataset_override: Optional[str] = None,
) -> Set[str]:
    datasets_to_exclude = set()
    # Only update views in the DATAFLOW_METRICS_MATERIALIZED_DATASET if the
    # dataflow_dataset_override is set
    if not dataflow_dataset_override:
        # TODO(#28430): These are expensive, but we should still load them if these
        # views have actually changed.
        datasets_to_exclude.add(DATAFLOW_METRICS_MATERIALIZED_DATASET)
    return datasets_to_exclude


def _load_manually_filtered_views_to_sandbox(
    *,
    sandbox_dataset_prefix: str,
    prompt: bool,
    dataflow_dataset_override: Optional[str],
    filter_union_all: bool,
    allow_slow_views: bool,
    view_ids_to_load: Optional[List[BigQueryAddress]],
    dataset_ids_to_load: Optional[List[str]],
    update_ancestors: bool,
    update_descendants: bool,
) -> None:
    """Loads all views into sandbox datasets prefixed with the sandbox_dataset_prefix.

    Params
    ------
    sandbox_dataset_prefix : str
        Loads datasets that all take the form of
        "<sandbox_dataset_prefix>_<original dataset_id>"

    dataflow_dataset_override : Optional[str]
        If True, updates views in the DATAFLOW_METRICS_MATERIALIZED_DATASET

    view_ids_to_load : Optional[List[BigQueryAddress]]
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

    confirmation_prompt = """
There are relatively few known use cases for the `manual` mode. Consider these alternatives:
* If you want to test how your local view changes impact a set of downstream views, you
    can use `auto` mode with the `--load_up_to_addresses` and/or `--load_up_to_datasets`
    arguments.
* If you just want to load the views changed on your branch, you can use `auto` mode
    with the `--load_changed_views_only` flag. 
* If `auto` mode is picking up other views on `main` you can use the
    `--changed_datasets_to_ignore` or `--changed_datasets_to_include` flags to filter 
    out changes in datasets unrelated to your changes.

Are you sure you still want to continue with `manual` mode? 
"""

    prompt_for_confirmation(confirmation_prompt)

    logging.info("Prefixing all view datasets with [%s_].", sandbox_dataset_prefix)

    view_builders = deployed_view_builders()

    addresses_to_load = set(view_ids_to_load) if view_ids_to_load else set()
    addresses_to_load |= (
        {vb.address for vb in view_builders if vb.dataset_id in dataset_ids_to_load}
        if dataset_ids_to_load
        else set()
    )

    collector = BigQueryViewSubDagCollector(
        view_builders_in_full_dag=view_builders,
        view_addresses_in_sub_dag=addresses_to_load,
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
        filter_union_all=filter_union_all,
        collected_builders=collected_builders,
        allow_slow_views=allow_slow_views,
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
            t = bq_client.get_table(v.address)
        except exceptions.NotFound:
            if not v.should_deploy():
                return None

            return ViewChangeType.ADDED

        if t.view_query != v.view_query:
            return ViewChangeType.UPDATED

        return None

    results = full_dag_walker.process_dag(check_for_change, synchronous=False)
    return {
        view.address: change_type
        for view, change_type in results.view_results.items()
        if change_type
    }


def _get_changed_views_to_load_to_sandbox(
    *,
    address_to_change_type: Dict[BigQueryAddress, ViewChangeType],
    changed_datasets_to_include: Optional[List[str]],
    changed_datasets_to_ignore: Optional[List[str]],
) -> Set[BigQueryAddress]:
    """Returns addresses for all views that have been added / updated since the last
    deploy and whose dataset is not in |changed_datasets_to_ignore|.
    """
    if changed_datasets_to_include and changed_datasets_to_ignore:
        raise ValueError(
            "Can only set changed_datasets_to_include or "
            "changed_datasets_to_ignore, but not both."
        )

    added_view_addresses = set()
    updated_view_addresses = set()
    ignored_changed_addresses = set()
    for address, change_type in address_to_change_type.items():
        if (
            changed_datasets_to_ignore
            and address.dataset_id in changed_datasets_to_ignore
        ) or (
            changed_datasets_to_include
            and address.dataset_id not in changed_datasets_to_include
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
    logging.info("Last deployed commit: %s", last_deployed_commit)

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
    of an ignored view that has not yet been deployed.
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

    full_dag_walker.process_dag(find_invalid_ignores, synchronous=True)

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


def _get_all_addresses_between_start_and_end_collections(
    all_views_dag_walker: BigQueryViewDagWalker,
    start_addresses: Set[BigQueryAddress],
    end_addresses: Set[BigQueryAddress],
) -> Set[BigQueryAddress]:
    """Given a set of addresses to start from and a set of addresses to end at, returns
    the set of addresses including the start addresses, end addresses, and any views in
    a dependency chain between the between start and end addresses.
    """

    start_views = all_views_dag_walker.views_for_addresses(list(start_addresses))
    start_descendants = {
        v.address
        for v in all_views_dag_walker.get_descendants_sub_dag(start_views).views
    }

    end_views = all_views_dag_walker.views_for_addresses(list(end_addresses))
    end_ancestors = {
        v.address for v in all_views_dag_walker.get_ancestors_sub_dag(end_views).views
    }

    return end_ancestors.intersection(start_descendants)


def _load_views_changed_on_branch_to_sandbox(
    *,
    sandbox_dataset_prefix: str,
    prompt: bool,
    dataflow_dataset_override: Optional[str],
    filter_union_all: bool,
    allow_slow_views: bool,
    changed_datasets_to_include: Optional[List[str]],
    changed_datasets_to_ignore: Optional[List[str]],
    load_up_to_addresses: Optional[List[BigQueryAddress]],
    load_up_to_datasets: Optional[List[str]],
    load_changed_views_only: bool,
) -> None:
    """Loads all views that have changed on this branch as compared to what is deployed
    to the current project (usually staging).
    """
    if (
        not load_changed_views_only
        and not load_up_to_addresses
        and not load_up_to_datasets
    ):
        raise ValueError(
            "Must set one of --load_changed_views_only, --load_up_to_addresses, "
            "or --load_up_to_datasets"
        )

    if load_changed_views_only and (load_up_to_addresses or load_up_to_datasets):
        raise ValueError(
            "Cannot set --load_changed_views_only at the same time as "
            "--load_up_to_addresses or --load_up_to_datasets."
        )

    _confirm_rebased_on_latest_deploy()

    view_builders_in_full_dag = deployed_view_builders()

    all_views = build_views_to_update(
        view_source_table_datasets=get_all_source_table_datasets(),
        candidate_view_builders=view_builders_in_full_dag,
        sandbox_context=None,
    )
    logging.info("Constructing DAG with all known views...")
    full_dag_walker = BigQueryViewDagWalker(all_views)

    logging.info("Checking for changes against [%s] BigQuery...", project_id())
    address_to_change_type = _get_all_views_changed_on_branch(full_dag_walker)

    changed_views_to_load = _get_changed_views_to_load_to_sandbox(
        address_to_change_type=address_to_change_type,
        changed_datasets_to_ignore=changed_datasets_to_ignore,
        changed_datasets_to_include=changed_datasets_to_include,
    )

    # TODO(#33733): Change logic here to expand the set of views to load when source
    #  table overrides are defined (all parents of the source tables should be loaded by
    #  default).

    if not changed_views_to_load:
        logging.warning(
            "Did not find any changed views to load to the sandbox. Exiting."
        )
        sys.exit(1)

    logging.info("Gathering views to load to sandbox...")
    if load_up_to_datasets or load_up_to_addresses:
        end_addresses = set(load_up_to_addresses) if load_up_to_addresses else set()
        end_addresses |= (
            {
                vb.address
                for vb in view_builders_in_full_dag
                if vb.dataset_id in load_up_to_datasets
            }
            if load_up_to_datasets
            else set()
        )
    elif load_changed_views_only:
        end_addresses = changed_views_to_load
    else:
        raise ValueError(
            "Expected one of load_changed_views_only, load_up_to_datasets or "
            "load_up_to_addresses to be set."
        )

    addresses_to_load = _get_all_addresses_between_start_and_end_collections(
        full_dag_walker,
        start_addresses=changed_views_to_load,
        end_addresses=end_addresses,
    )
    collected_builders = [
        vb for vb in view_builders_in_full_dag if vb.address in addresses_to_load
    ]

    _check_for_invalid_ignores(
        full_dag_walker=full_dag_walker,
        address_to_change_type=address_to_change_type,
        addresses_to_load={b.address for b in collected_builders},
    )

    _load_collected_views_to_sandbox(
        sandbox_dataset_prefix=sandbox_dataset_prefix,
        prompt=prompt,
        dataflow_dataset_override=dataflow_dataset_override,
        filter_union_all=filter_union_all,
        allow_slow_views=allow_slow_views,
        collected_builders=collected_builders,
    )


def _sorted_address_list_str(addresses: Iterable[BigQueryAddress]) -> str:
    return "\n".join([f"- {a.dataset_id}.{a.table_id}" for a in sorted(addresses)])


def _load_collected_views_to_sandbox(
    *,
    sandbox_dataset_prefix: str,
    prompt: bool,
    collected_builders: List[BigQueryViewBuilder],
    dataflow_dataset_override: Optional[str],
    filter_union_all: bool,
    allow_slow_views: bool,
) -> None:
    """Loads the provided list of builders to a sandbox dataset."""
    logging.info(
        "Will load the following views to the sandbox: \n%s",
        _sorted_address_list_str({b.address for b in collected_builders}),
    )
    if prompt:
        prompt_for_confirmation(
            f"Continue with loading {len(collected_builders)} views?"
        )

    if filter_union_all:
        addresses_to_load = {b.address for b in collected_builders}
        for builder in collected_builders:
            if isinstance(builder, UnionAllBigQueryViewBuilder):
                builder.set_parent_address_filter(addresses_to_load)

    logging.info("Updating %s views...", len(collected_builders))

    view_update_sandbox_context = None
    if sandbox_dataset_prefix:
        input_source_table_overrides = BigQueryAddressOverrides.empty()
        # TODO(#33733): Generalize this script to allow for an arbitrary set of source
        #  table overrides.
        if dataflow_dataset_override:
            input_source_table_overrides = address_overrides_for_input_source_tables(
                {DATAFLOW_METRICS_MATERIALIZED_DATASET: dataflow_dataset_override}
            )

        view_update_sandbox_context = BigQueryViewUpdateSandboxContext(
            output_sandbox_dataset_prefix=sandbox_dataset_prefix,
            input_source_table_overrides=input_source_table_overrides,
        )

    create_managed_dataset_and_deploy_views_for_view_builders(
        view_source_table_datasets=get_all_source_table_datasets(),
        view_builders_to_update=collected_builders,
        view_update_sandbox_context=view_update_sandbox_context,
        # Don't clean up datasets when running a sandbox script
        historically_managed_datasets_to_clean=None,
        allow_slow_views=allow_slow_views,
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

    parser.add_argument(
        "--no-filter-union-all",
        dest="filter_union_all",
        action="store_false",
        help="If True, all UnionAllBigQueryViewBuilder views (e.g. "
        "task_eligibility.all_tasks) will only query from views loaded in the "
        "sandbox. Otherwise, the UnionAllBigQueryViewBuilder views will query "
        "ALL parent views like they do in production. This should generally be "
        "set to false to avoid unnecessarily expensive sandbox runs.",
    )

    parser.add_argument(
        "--allow_slow_views",
        dest="allow_slow_views",
        action="store_true",
        default=False,
        help="If true, the the script will not fail when views take longer "
        "than the allowed threshold to materialize.",
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
        type=str_to_address_list,
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
        "datasets which have been changed. This argument cannot be used if --changed_datasets_to_include "
        "is set.",
        type=str_to_list,
        required=False,
    )

    parser_auto.add_argument(
        "--changed_datasets_to_include",
        dest="changed_datasets_to_include",
        help="A list of dataset ids (comma-separated) for datasets we should consider when "
        "detecting which views have changed. Views outside of these datasets will still "
        "be loaded to the sandbox if they are downstream of other changed views in these datasets. This "
        "argument cannot be used if --changed_datasets_to_ignore is set.",
        type=str_to_list,
        required=False,
    )

    parser_auto.add_argument(
        "--load_up_to_addresses",
        dest="load_up_to_addresses",
        help=(
            "If provided, the sandbox load will stop after all of these views have "
            "been loaded. Views that are only descendants of these views will not be "
            "loaded."
        ),
        type=str_to_address_list,
        required=False,
    )

    parser_auto.add_argument(
        "--load_up_to_datasets",
        dest="load_up_to_datasets",
        help=(
            "If provided, the sandbox load will stop after all of these views in these "
            "datasets have been loaded. Views that are only descendants of the views "
            "in these datasets will not be loaded."
        ),
        type=str_to_list,
        required=False,
    )
    parser_auto.add_argument(
        "--load_changed_views_only",
        dest="load_changed_views_only",
        action="store_true",
        default=False,
        help=(
            "If true, the sandbox load will only load views that have changed, or "
            "views in the dependency chain between views that have changed."
        ),
    )

    subparsers.add_parser("all")

    return parser.parse_args()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    args = parse_arguments()

    with local_project_id_override(args.project_id):
        if args.chosen_mode == "all":
            load_all_views_to_sandbox(
                sandbox_dataset_prefix=args.sandbox_dataset_prefix,
                prompt=args.prompt,
                dataflow_dataset_override=args.dataflow_dataset_override,
                filter_union_all=args.filter_union_all,
                allow_slow_views=args.allow_slow_views,
            )
        elif args.chosen_mode == "manual":
            _load_manually_filtered_views_to_sandbox(
                sandbox_dataset_prefix=args.sandbox_dataset_prefix,
                prompt=args.prompt,
                dataflow_dataset_override=args.dataflow_dataset_override,
                filter_union_all=args.filter_union_all,
                allow_slow_views=args.allow_slow_views,
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
                filter_union_all=args.filter_union_all,
                allow_slow_views=args.allow_slow_views,
                changed_datasets_to_include=args.changed_datasets_to_include,
                changed_datasets_to_ignore=args.changed_datasets_to_ignore,
                load_up_to_addresses=args.load_up_to_addresses,
                load_up_to_datasets=args.load_up_to_datasets,
                load_changed_views_only=args.load_changed_views_only,
            )
        else:
            raise ValueError(f"Unexpected load to sandbox mode: [{args.chosen_mode}]")
