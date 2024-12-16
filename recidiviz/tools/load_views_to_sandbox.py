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
       --sandbox_dataset_prefix [SANDBOX_DATASET_PREFIX] --prompt auto \
       --load_changed_views_only

To load all views that have changed since the last view deploy but only load rows for a
given state_code (much cheaper / faster!), run:
    python -m recidiviz.tools.load_views_to_sandbox \
       --sandbox_dataset_prefix [SANDBOX_DATASET_PREFIX] \
       --state_code_filter [STATE_CODE] auto \
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
       --changed_datasets_to_include [DATASET_ID_1,DATASET_ID_2,...] \
       --load_changed_views_only

To manually choose which views to load, run:
    python -m recidiviz.tools.load_views_to_sandbox \
        --sandbox_dataset_prefix [SANDBOX_DATASET_PREFIX] manual \
        --view_ids_to_load [DATASET_ID_1.VIEW_ID_1,DATASET_ID_2.VIEW_ID_2,...] \
        --dataset_ids_to_load [DATASET_ID_1,DATASET_ID_2,...] \
        --update_ancestors [True,False] \
        --update_descendants [True,False]

To load ALL views to a sandbox (this should be used only in rare circumstances), run:
    python -m recidiviz.tools.load_views_to_sandbox \
       --sandbox_dataset_prefix [SANDBOX_DATASET_PREFIX] --prompt all

To load all views downstream of a sandbox dataset, run:
    python -m recidiviz.tools.load_views_to_sandbox \
       --input_source_table_dataset_overrides_json [SANDBOX_DATASET_OVERRIDES_JSON] \
       --sandbox_dataset_prefix [SANDBOX_DATASET_PREFIX] manual \
       --dataset_ids_to_load [DATASET_ID_1,DATASET_ID_2,...]

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
import textwrap
from enum import Enum
from typing import Dict, List, Optional, Set

import attr
from google.api_core import exceptions
from tabulate import tabulate

from recidiviz.aggregated_metrics.dataset_config import AGGREGATED_METRICS_DATASET_ID
from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_address_formatter import (
    StateFilteringBigQueryAddressFormatterProvider,
)
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.big_query_view import BigQueryView, BigQueryViewBuilder
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
    DATAFLOW_METRICS_DATASET,
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
)
from recidiviz.common.attr_converters import optional_json_str_to_dict
from recidiviz.common.constants.states import StateCode
from recidiviz.common.git import (
    get_hash_of_deployed_commit,
    is_commit_in_current_branch,
)
from recidiviz.source_tables.collect_all_source_table_configs import (
    get_source_table_addresses,
    get_source_table_datasets,
)
from recidiviz.tools.utils.arg_parsers import str_to_address_list
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override, project_id
from recidiviz.utils.params import str_to_bool, str_to_list
from recidiviz.utils.types import assert_type
from recidiviz.view_registry.address_overrides_factory import (
    address_overrides_for_input_source_tables,
)
from recidiviz.view_registry.deployed_address_schema_utils import (
    get_deployed_addresses_without_state_code_column,
    get_source_tables_to_pseudocolumns,
)
from recidiviz.view_registry.deployed_views import deployed_view_builders


def load_all_views_to_sandbox(
    *,
    sandbox_dataset_prefix: str,
    prompt: bool,
    state_code_filter: StateCode | None,
    input_source_table_dataset_overrides_dict: dict[str, str] | None,
    allow_slow_views: bool,
    materialize_changed_views_only: bool,
) -> None:
    """Loads ALL views to sandbox datasets with prefix |sandbox_dataset_prefix|."""
    prompt_for_confirmation(
        "This will load ALL sandbox views. This can be a very slow / expensive "
        "operation. If you just want to test that your view changes do not introduce "
        "any view compilation issues, you can check with the tests in "
        "recidiviz/tests/big_query/view_graph_validation_test.py. Are you sure you "
        "want to continue?"
    )

    logging.info("Gathering views to load to sandbox...")
    collected_builders = deployed_view_builders()
    if prompt:
        prompt_for_confirmation(
            f"Continue with loading ALL {len(collected_builders)} views?"
        )
    load_collected_views_to_sandbox(
        sandbox_dataset_prefix=sandbox_dataset_prefix,
        state_code_filter=state_code_filter,
        collected_builders=collected_builders,
        input_source_table_dataset_overrides_dict=(
            input_source_table_dataset_overrides_dict
        ),
        allow_slow_views=allow_slow_views,
        materialize_changed_views_only=materialize_changed_views_only,
    )


def _load_manually_filtered_views_to_sandbox(
    *,
    sandbox_dataset_prefix: str,
    prompt: bool,
    state_code_filter: StateCode | None,
    input_source_table_dataset_overrides_dict: dict[str, str] | None,
    allow_slow_views: bool,
    materialize_changed_views_only: bool,
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

    input_source_table_dataset_overrides_json : Optional[str]
        If specified, views will read from the override address for any parent source
            tables.

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
        datasets_to_exclude=set(),
    )

    logging.info("Gathering views to load to sandbox...")
    collected_builders = collector.collect_view_builders()

    logging.info(
        "Will load the following views to the sandbox: \n%s",
        BigQueryAddress.addresses_to_str(
            {b.address for b in collected_builders}, indent_level=2
        ),
    )
    if prompt:
        prompt_for_confirmation(
            f"Continue with loading {len(collected_builders)} views?"
        )

    load_collected_views_to_sandbox(
        sandbox_dataset_prefix=sandbox_dataset_prefix,
        state_code_filter=state_code_filter,
        input_source_table_dataset_overrides_dict=(
            input_source_table_dataset_overrides_dict
        ),
        collected_builders=collected_builders,
        allow_slow_views=allow_slow_views,
        materialize_changed_views_only=materialize_changed_views_only,
    )


class ViewChangeType(Enum):
    ADDED = "ADDED"
    UPDATED = "UPDATED"


def _get_all_views_changed_on_branch(
    full_dag_walker: BigQueryViewDagWalker,
) -> Dict[BigQueryAddress, ViewChangeType]:
    """Finds all views that have changed (or been added) as compared to their currently
    deployed versions.
    """
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

        if v.materialized_address:
            try:
                materialized_t = bq_client.get_table(v.materialized_address)
            except exceptions.NotFound:
                return ViewChangeType.UPDATED
            old_clustering_fields = materialized_t.clustering_fields or []
            new_clustering_fields = v.clustering_fields or []
            if (
                old_clustering_fields != new_clustering_fields
                or materialized_t.time_partitioning != v.time_partitioning
            ):
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
            BigQueryAddress.addresses_to_str(added_view_addresses, indent_level=2),
        )
    if updated_view_addresses:
        logging.info(
            "Found the following view(s) which have been UPDATED \n%s",
            BigQueryAddress.addresses_to_str(updated_view_addresses, indent_level=2),
        )
    if ignored_changed_addresses:
        logging.info(
            "IGNORING changes to the following view(s) \n%s",
            BigQueryAddress.addresses_to_str(ignored_changed_addresses, indent_level=2),
        )

    return set(address_to_change_type) - ignored_changed_addresses


def confirm_rebased_on_latest_deploy() -> None:
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
            BigQueryAddress.addresses_to_str(invalid_views_to_load, indent_level=2),
        )
        logging.error(
            "New views excluded from this sandbox run:\n%s",
            BigQueryAddress.addresses_to_str(ignored_added_views, indent_level=2),
        )
        logging.error(
            "You must include these new views in the sandbox load or ignore all "
            "changed downstream parents as well."
        )
        sys.exit(1)


@attr.define(kw_only=True)
class SandboxChangedAddresses:
    """Tracks information about what views or source tables count as 'changed' during
    a given `auto` mode sandbox views load.
    """

    # ALL view addresses that have changed as compared to deployed views, with the
    # change type.
    view_address_to_change_type: dict[BigQueryAddress, ViewChangeType]

    changed_datasets_to_include: set[str] | None
    changed_datasets_to_ignore: set[str] | None
    state_code_filter: StateCode | None

    changed_source_table_addresses: set[BigQueryAddress]

    def __attrs_post_init__(self) -> None:
        if self.changed_datasets_to_include and self.changed_datasets_to_ignore:
            raise ValueError(
                "Can only set changed_datasets_to_include or "
                "changed_datasets_to_ignore, but not both."
            )

    @property
    def changed_view_addresses_to_ignore(self) -> set[BigQueryAddress]:
        """Returns all view addresses that have changed but which should be ignored when
        collecting which views to load to the sandbox.
        """
        return {
            address
            for address in self.view_address_to_change_type
            if (
                self.changed_datasets_to_ignore
                and address.dataset_id in self.changed_datasets_to_ignore
            )
            or (
                self.changed_datasets_to_include
                and address.dataset_id not in self.changed_datasets_to_include
            )
            or (
                self.state_code_filter is not None
                and address.state_code_for_address() is not None
                and address.state_code_for_address() != self.state_code_filter
            )
        }

    @property
    def has_changes_to_load(self) -> bool:
        """Returns True if there are any views / source tables whose changes should be
        picked up and loaded into the sandbox.
        """
        return bool(self.changed_view_addresses_to_load) or bool(
            self.changed_source_table_addresses
        )

    @property
    def changed_view_addresses_to_load(self) -> set[BigQueryAddress]:
        return (
            set(self.view_address_to_change_type.keys())
            - self.changed_view_addresses_to_ignore
        )

    @property
    def added_views_to_load(self) -> set[BigQueryAddress]:
        return {
            a
            for a in self.changed_view_addresses_to_load
            if self.view_address_to_change_type[a] == ViewChangeType.ADDED
        }

    @property
    def updated_views_to_load(self) -> set[BigQueryAddress]:
        return {
            a
            for a in self.changed_view_addresses_to_load
            if self.view_address_to_change_type[a] == ViewChangeType.UPDATED
        }


def summary_for_auto_sandbox(
    changed_addresses: SandboxChangedAddresses,
    all_view_addresses_to_load: set[BigQueryAddress],
) -> str:
    """Builds a summary string that describes the views that will / won't be loaded into
    the sandbox.
    """
    downstream_views_to_load = (
        all_view_addresses_to_load - changed_addresses.changed_view_addresses_to_load
    )

    descriptions_char_width = 50
    no_addresses_str = "<none>"
    table_data = [
        (
            "IGNORED view changes",
            BigQueryAddress.addresses_to_str(
                changed_addresses.changed_view_addresses_to_ignore
            )
            or no_addresses_str,
            textwrap.fill(
                "This is the set of views on your branch that has changed as compared "
                "to deployed views, but you've exempted via the "
                "--changed_datasets_to_include and --changed_datasets_to_ignore flags. "
                "These views will not be loaded unless they are also included in the "
                "DOWNSTREAM section below (can happen if they are in the dependency "
                "chain between other changed views and views you want to load).",
                width=descriptions_char_width,
            ),
        ),
        (
            "UPDATED source tables\n to read from",
            BigQueryAddress.addresses_to_str(
                changed_addresses.changed_source_table_addresses
            ),
            textwrap.fill(
                "This is the set of overridden source tables to read from as specified "
                "by the --input_source_table_dataset_overrides_json flag. Views will "
                "read from the overridden version of the table in place of these "
                "tables.",
                width=descriptions_char_width,
            ),
        )
        if changed_addresses.changed_source_table_addresses
        # Hide this row if there are no source table overrides
        else None,
        (
            "ADDED views to load",
            (
                BigQueryAddress.addresses_to_str(changed_addresses.added_views_to_load)
                or no_addresses_str
            ),
            textwrap.fill(
                "These are the views on your branch that have been added as compared "
                "to deployed views and will be treated as potential 'roots' of the "
                "graph of views to load to your sandbox. If you don't care about some "
                "of these changes, you can move them to the IGNORED section via the "
                "--changed_datasets_to_include and --changed_datasets_to_ignore flags.",
                width=descriptions_char_width,
            ),
        ),
        (
            "UPDATED views to load",
            (
                BigQueryAddress.addresses_to_str(
                    changed_addresses.updated_views_to_load
                )
                or no_addresses_str
            ),
            textwrap.fill(
                "These are the views on your branch that have been updated (i.e. view "
                "query has changed) as compared to deployed views and will be treated "
                "as potential 'roots' of the graph of views to load to your sandbox. "
                "If you don't care about some of these changes, you can move them to "
                "the IGNORED section via the --changed_datasets_to_include and "
                "--changed_datasets_to_ignore flags.",
                width=descriptions_char_width,
            ),
        ),
        (
            "DOWNSTREAM views to load",
            (
                BigQueryAddress.addresses_to_str(downstream_views_to_load)
                or no_addresses_str
            ),
            textwrap.fill(
                "These are the views that did not change themselves, but will still "
                "be loaded as a downstream dependency of one of the ADDED/UPDATED "
                "views / tables. This set of views is impacted by the "
                "--load_changed_views_only, --load_up_to_addresses, and "
                "--load_up_to_datasets flags.",
                width=descriptions_char_width,
            ),
        ),
    ]

    summary_string = tabulate(
        [row for row in table_data if row is not None],
        headers=["Category", "BigQuery Addresses", "Hint"],
        tablefmt="fancy_grid",
    )
    return summary_string


def get_sandbox_changed_addresses(
    full_dag_walker: BigQueryViewDagWalker,
    input_source_table_dataset_overrides_dict: dict[str, str] | None,
    changed_datasets_to_include: list[str] | None,
    changed_datasets_to_ignore: list[str] | None,
    state_code_filter: StateCode | None,
) -> SandboxChangedAddresses:
    logging.info("Checking for changes against [%s] BigQuery...", project_id())
    address_to_change_type = _get_all_views_changed_on_branch(full_dag_walker)

    changed_source_table_addresses: set[BigQueryAddress] = (
        {
            a
            for a in get_source_table_addresses(metadata.project_id())
            if a.dataset_id in input_source_table_dataset_overrides_dict
        }
        if input_source_table_dataset_overrides_dict
        else set()
    )

    return SandboxChangedAddresses(
        view_address_to_change_type=address_to_change_type,
        changed_datasets_to_include=(
            set(changed_datasets_to_include) if changed_datasets_to_include else None
        ),
        changed_datasets_to_ignore=(
            set(changed_datasets_to_ignore) if changed_datasets_to_ignore else None
        ),
        state_code_filter=state_code_filter,
        changed_source_table_addresses=changed_source_table_addresses,
    )


def collect_changed_views_and_descendants_to_load(
    prompt: bool,
    input_source_table_dataset_overrides_dict: dict[str, str] | None,
    changed_datasets_to_include: list[str] | None,
    changed_datasets_to_ignore: list[str] | None,
    state_code_filter: StateCode | None,
    load_up_to_addresses: list[BigQueryAddress] | None,
    load_up_to_datasets: list[str] | None,
    load_changed_views_only: bool,
) -> list[BigQueryViewBuilder]:
    """Returns the list of view builders for changed views (and their descendants,
    where relevant) that should be loaded to the sandbox.
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

    confirm_rebased_on_latest_deploy()

    logging.info("Constructing DAG with all known views...")
    view_builders_in_full_dag = deployed_view_builders()
    all_views = build_views_to_update(
        view_source_table_datasets=get_source_table_datasets(metadata.project_id()),
        candidate_view_builders=view_builders_in_full_dag,
        sandbox_context=None,
    )

    full_dag_walker = BigQueryViewDagWalker(all_views)
    changed_addresses_info = get_sandbox_changed_addresses(
        full_dag_walker=full_dag_walker,
        input_source_table_dataset_overrides_dict=input_source_table_dataset_overrides_dict,
        changed_datasets_to_ignore=changed_datasets_to_ignore,
        changed_datasets_to_include=changed_datasets_to_include,
        state_code_filter=state_code_filter,
    )

    if changed_addresses_info.has_changes_to_load:
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
            end_addresses = changed_addresses_info.changed_view_addresses_to_load
        else:
            raise ValueError(
                "Expected one of load_changed_views_only, load_up_to_datasets or "
                "load_up_to_addresses to be set."
            )

        addresses_to_load = full_dag_walker.get_all_node_addresses_between_start_and_end_collections(
            start_source_addresses=changed_addresses_info.changed_source_table_addresses,
            start_node_addresses=changed_addresses_info.changed_view_addresses_to_load,
            end_node_addresses=end_addresses,
        )
        collected_builders = [
            vb for vb in view_builders_in_full_dag if vb.address in addresses_to_load
        ]
    else:
        collected_builders = []

    _check_for_invalid_ignores(
        full_dag_walker=full_dag_walker,
        address_to_change_type=changed_addresses_info.view_address_to_change_type,
        addresses_to_load={b.address for b in collected_builders},
    )

    print(
        summary_for_auto_sandbox(
            changed_addresses=changed_addresses_info,
            all_view_addresses_to_load={vb.address for vb in collected_builders},
        )
    )

    if prompt:
        prompt_for_confirmation(
            f"Continue with loading {len(collected_builders)} views?"
        )

    return collected_builders


def load_views_changed_on_branch_to_sandbox(
    *,
    sandbox_dataset_prefix: str,
    prompt: bool,
    state_code_filter: StateCode | None,
    input_source_table_dataset_overrides_dict: dict[str, str] | None,
    allow_slow_views: bool,
    materialize_changed_views_only: bool,
    changed_datasets_to_include: Optional[List[str]],
    changed_datasets_to_ignore: Optional[List[str]],
    load_up_to_addresses: Optional[List[BigQueryAddress]],
    load_up_to_datasets: Optional[List[str]],
    load_changed_views_only: bool,
) -> None:
    """Loads all views that have changed on this branch as compared to what is deployed
    to the current project (usually staging).
    """
    collected_builders = collect_changed_views_and_descendants_to_load(
        prompt=prompt,
        input_source_table_dataset_overrides_dict=(
            input_source_table_dataset_overrides_dict
        ),
        changed_datasets_to_ignore=changed_datasets_to_ignore,
        changed_datasets_to_include=changed_datasets_to_include,
        state_code_filter=state_code_filter,
        load_changed_views_only=load_changed_views_only,
        load_up_to_addresses=load_up_to_addresses,
        load_up_to_datasets=load_up_to_datasets,
    )

    load_collected_views_to_sandbox(
        sandbox_dataset_prefix=sandbox_dataset_prefix,
        state_code_filter=state_code_filter,
        input_source_table_dataset_overrides_dict=(
            input_source_table_dataset_overrides_dict
        ),
        allow_slow_views=allow_slow_views,
        materialize_changed_views_only=materialize_changed_views_only,
        collected_builders=collected_builders,
    )


def _warn_on_expensive_views(
    collected_builders: List[BigQueryViewBuilder],
    state_code_filter: StateCode | None,
    input_source_table_overrides: BigQueryAddressOverrides,
) -> None:
    """Prompts the user with a warning if any of the views we plan to load are
    especially expensive / pull in a lot of data.
    """
    if state_code_filter:
        # If we're running with a state_code filter, we're not worried about the
        # materialization cost these more expensive views.
        return

    expensive_views = {
        vb.address
        for vb in collected_builders
        if (
            # TODO(#29291): Don't count UnionAllBigQueryViewBuilder with
            #  aggregated metrics parents as expensive
            isinstance(vb, UnionAllBigQueryViewBuilder)
            # TODO(#35897), TODO(#35898): Remove warning about views
            #  in this dataset once we've optimized them and they aren't so expensive.
            or vb.dataset_id == AGGREGATED_METRICS_DATASET_ID
        )
    }

    has_dataflow_metrics_override = (
        DATAFLOW_METRICS_DATASET
        != input_source_table_overrides.get_dataset(DATAFLOW_METRICS_DATASET)
    )
    if not has_dataflow_metrics_override:
        for vb in collected_builders:
            if vb.dataset_id == DATAFLOW_METRICS_MATERIALIZED_DATASET:
                expensive_views.add(vb.address)

    if expensive_views:
        prompt_for_confirmation(
            f"⚠️ This selection of views includes the following views that are "
            f"relatively expensive to materialize:"
            f"{BigQueryAddress.addresses_to_str(expensive_views, indent_level=2)}"
            f"⚠️ Are you sure you want to continue loading views without a "
            f"`--state_code_filter` set?"
        )


def load_collected_views_to_sandbox(
    *,
    sandbox_dataset_prefix: str,
    state_code_filter: StateCode | None,
    collected_builders: List[BigQueryViewBuilder],
    input_source_table_dataset_overrides_dict: dict[str, str] | None,
    allow_slow_views: bool,
    materialize_changed_views_only: bool,
) -> None:
    """Loads the provided list of builders to a sandbox dataset."""
    if not collected_builders:
        logging.warning("Did not find any views to load to the sandbox. Exiting.")
        return

    input_source_table_overrides = (
        address_overrides_for_input_source_tables(
            input_source_table_dataset_overrides_dict
        )
        if input_source_table_dataset_overrides_dict
        else BigQueryAddressOverrides.empty()
    )

    _warn_on_expensive_views(
        collected_builders=collected_builders,
        state_code_filter=state_code_filter,
        input_source_table_overrides=input_source_table_overrides,
    )

    logging.info("Updating %s views...", len(collected_builders))

    view_update_sandbox_context = None
    if sandbox_dataset_prefix:
        missing_state_code_addresses = get_deployed_addresses_without_state_code_column(
            metadata.project_id()
        )
        view_update_sandbox_context = BigQueryViewUpdateSandboxContext(
            output_sandbox_dataset_prefix=sandbox_dataset_prefix,
            input_source_table_overrides=input_source_table_overrides,
            parent_address_formatter_provider=(
                StateFilteringBigQueryAddressFormatterProvider(
                    state_code_filter=state_code_filter,
                    missing_state_code_addresses=missing_state_code_addresses,
                    pseudocolumns_by_address=get_source_tables_to_pseudocolumns(),
                )
                if state_code_filter
                else None
            ),
        )
    try:
        create_managed_dataset_and_deploy_views_for_view_builders(
            view_source_table_datasets=get_source_table_datasets(metadata.project_id()),
            view_builders_to_update=collected_builders,
            view_update_sandbox_context=view_update_sandbox_context,
            # Don't clean up datasets when running a sandbox script
            historically_managed_datasets_to_clean=None,
            allow_slow_views=allow_slow_views,
            materialize_changed_views_only=materialize_changed_views_only,
        )
    except exceptions.Forbidden as e:
        if "Permission denied while getting Drive credentials" in str(e):
            raise ValueError(
                "Permission denied while accessing Drive credentials during view update."
                " If the failing view is backed by a google sheet, and you have view permissions for the sheet,"
                " try running:\n  gcloud auth login --enable-gdrive-access --update-adc"
            ) from e
        raise e


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
        "--state_code_filter",
        dest="state_code_filter",
        help="If provided, all table references in any loaded view queries will be "
        "modified to only return rows for the given state. If running in `auto` mode, "
        "changes in state-specific views that do not match this filter will be ignored.",
        type=StateCode,
        choices=list(StateCode),
        required=False,
    )

    parser.add_argument(
        "--materialize_changed_views_only",
        dest="materialize_changed_views_only",
        action="store_true",
        default=False,
        help="If true, any given view will only be rematerialized as part of this "
        "sandbox run if it has changed (e.g. query change, clustering fields "
        "change) or any of its parent views have changed. Can be used to optimize "
        "subsequent sandbox runs if you're iterating on views later in the graph.",
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
        "--input_source_table_dataset_overrides_json",
        dest="input_source_table_dataset_overrides_json",
        help="A JSON string containing mapping of source table datasets to the "
        "replacement sandbox datasets that updated views should reference.",
        type=str,
        required=False,
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


def _parse_input_source_table_dataset_overrides(
    input_source_table_dataset_overrides_json: str | None,
) -> dict[str, str] | None:
    """Given a JSON string defining a mapping between source table dataset and override
    source table dataset, parses

    Raises a ValueError if any dataset to override is not a valid source table dataset.
    """
    parsed_dict = optional_json_str_to_dict(input_source_table_dataset_overrides_json)

    if parsed_dict is None:
        return None

    input_source_table_dataset_overrides_dict = {
        dataset_id: assert_type(override_dataset_id, str)
        for dataset_id, override_dataset_id in parsed_dict.items()
    }

    valid_source_table_datasets = get_source_table_datasets(metadata.project_id())
    for dataset in input_source_table_dataset_overrides_dict.keys():
        if dataset not in valid_source_table_datasets:
            raise ValueError(
                f"Found dataset [{dataset}] which is not a valid source table dataset "
                f"in input_source_table_dataset_overrides_json: "
                f"{input_source_table_dataset_overrides_json}"
            )

    return input_source_table_dataset_overrides_dict


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    args = parse_arguments()

    with local_project_id_override(args.project_id):
        if args.chosen_mode == "all":
            load_all_views_to_sandbox(
                sandbox_dataset_prefix=args.sandbox_dataset_prefix,
                prompt=args.prompt,
                state_code_filter=args.state_code_filter,
                input_source_table_dataset_overrides_dict=(
                    _parse_input_source_table_dataset_overrides(
                        args.input_source_table_dataset_overrides_json
                    )
                ),
                allow_slow_views=args.allow_slow_views,
                materialize_changed_views_only=args.materialize_changed_views_only,
            )
        elif args.chosen_mode == "manual":
            _load_manually_filtered_views_to_sandbox(
                sandbox_dataset_prefix=args.sandbox_dataset_prefix,
                prompt=args.prompt,
                state_code_filter=args.state_code_filter,
                input_source_table_dataset_overrides_dict=(
                    _parse_input_source_table_dataset_overrides(
                        args.input_source_table_dataset_overrides_json
                    )
                ),
                allow_slow_views=args.allow_slow_views,
                materialize_changed_views_only=args.materialize_changed_views_only,
                view_ids_to_load=args.view_ids_to_load,
                dataset_ids_to_load=args.dataset_ids_to_load,
                update_ancestors=args.update_ancestors,
                update_descendants=args.update_descendants,
            )
        elif args.chosen_mode == "auto":
            load_views_changed_on_branch_to_sandbox(
                sandbox_dataset_prefix=args.sandbox_dataset_prefix,
                prompt=args.prompt,
                state_code_filter=args.state_code_filter,
                input_source_table_dataset_overrides_dict=(
                    _parse_input_source_table_dataset_overrides(
                        args.input_source_table_dataset_overrides_json
                    )
                ),
                allow_slow_views=args.allow_slow_views,
                materialize_changed_views_only=args.materialize_changed_views_only,
                changed_datasets_to_include=args.changed_datasets_to_include,
                changed_datasets_to_ignore=args.changed_datasets_to_ignore,
                load_up_to_addresses=args.load_up_to_addresses,
                load_up_to_datasets=args.load_up_to_datasets,
                load_changed_views_only=args.load_changed_views_only,
            )
        else:
            raise ValueError(f"Unexpected load to sandbox mode: [{args.chosen_mode}]")
