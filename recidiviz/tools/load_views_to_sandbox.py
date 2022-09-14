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
"""A script for writing all regularly updated views in BigQuery to a set of temporary
datasets. Used during development to test updates to views.

This can be run on-demand whenever locally with the following command:
    python -m recidiviz.tools.load_views_to_sandbox \
        --project_id [PROJECT_ID] \
        --sandbox_dataset_prefix [SANDBOX_DATASET_PREFIX] \
        --dataflow_dataset_override [DATAFLOW_DATASET_OVERRIDE] \
        --view_ids_to_load [DATASET_ID_1.VIEW_ID_1,DATASET_ID_2.VIEW_ID_2,...] \
        --dataset_ids_to_load [DATASET_ID_1,DATASET_ID_2,...] \
        --update_ancestors [True,False] \
        --update_descendants [True,False]
"""
import argparse
import logging
from typing import List, Optional, Set

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.big_query.big_query_view_sub_dag_collector import (
    BigQueryViewSubDagCollector,
)
from recidiviz.big_query.view_update_manager import (
    create_managed_dataset_and_deploy_views_for_view_builders,
)
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
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

    logging.info("Gathering all deployed views...")
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


def _load_collected_views_to_sandbox(
    *,
    sandbox_dataset_prefix: str,
    sandbox_view_builder_collector: BigQueryViewCollector,
    dataflow_dataset_override: Optional[str] = None,
) -> None:

    logging.info("Gathering views to load to sandbox...")

    collected_builders = sandbox_view_builder_collector.collect_view_builders()

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
        required=True,
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

    parser.add_argument(
        "--view_ids_to_load",
        dest="view_ids_to_load",
        help="A list of view_ids to load separated by commas. If provided, only loads "
        "views in this list (plus dependencies, if applicable). View_ids must take the "
        "form dataset_id.view_id. Can be called together with the "
        "--dataset_ids_to_load argument as long as none of the datasets overlap.",
        type=str_to_list,
        required=False,
    )

    parser.add_argument(
        "--dataset_ids_to_load",
        dest="dataset_ids_to_load",
        help="A list of dataset_ids to load separated by commas. If provided, only "
        "loads datasets in this list (plus dependencies, if applicable).",
        type=str_to_list,
        required=False,
    )

    parser.add_argument(
        "--update_ancestors",
        dest="update_ancestors",
        help="If True, will load views that are ancestors of those provided in "
        "view_ids_to_load or dataset_ids_to_load.",
        type=str_to_bool,
        default=False,
        required=False,
    )

    parser.add_argument(
        "--update_descendants",
        dest="update_descendants",
        help="If True, will load views that are descendants of those provided in "
        "view_ids_to_load or dataset_ids_to_load.",
        type=str_to_bool,
        default=True,
        required=False,
    )

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
        if not args.view_ids_to_load and not args.dataset_ids_to_load:
            load_all_views_to_sandbox(
                sandbox_dataset_prefix=args.sandbox_dataset_prefix,
                dataflow_dataset_override=args.dataflow_dataset_override,
            )
        else:
            _load_manually_filtered_views_to_sandbox(
                sandbox_dataset_prefix=args.sandbox_dataset_prefix,
                dataflow_dataset_override=args.dataflow_dataset_override,
                view_ids_to_load=args.view_ids_to_load,
                dataset_ids_to_load=args.dataset_ids_to_load,
                update_ancestors=args.update_ancestors,
                update_descendants=args.update_descendants,
            )
