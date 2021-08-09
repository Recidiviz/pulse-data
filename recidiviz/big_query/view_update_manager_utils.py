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
"""Provides utilities for updating views within a live BigQuery instance."""

import logging
from typing import Dict, List, Set

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.big_query_view import BigQueryAddress
from recidiviz.big_query.big_query_view_dag_walker import BigQueryViewDagWalker

# A list of all datasets that have ever held managed views.
# This list is used to identify places where we should look for legacy views that we need to clean up.
# DO NOT DELETE ITEMS FROM THIS LIST UNLESS YOU KNOW THIS DATASET HAS BEEN FULLY DELETED FROM BOTH PROD AND STAGING.

DATASETS_THAT_HAVE_EVER_BEEN_MANAGED = {
    "analyst_data",
    "case_triage",
    "case_triage_cloudsql_connection",
    "case_triage_federated_regional",
    "census_managed_views",
    "census_regional",
    "covid_public_data",
    "dashboard_views",
    "dataflow_metrics_materialized",
    "experiments",
    "externally_shared_views",
    "ingest_metadata",
    "jails_cloudsql_connection",
    "justice_counts",
    "justice_counts_corrections",
    "justice_counts_dashboard",
    "justice_counts_jails",
    "operations_cloudsql_connection",
    "partner_data_csg",
    "po_report_views",
    "population_projection_data",
    "public_dashboard_views",
    "reference_views",
    "state_us_id_primary_cloudsql_connection",
    "state_us_mi_primary_cloudsql_connection",
    "state_us_mo_primary_cloudsql_connection",
    "state_us_nd_primary_cloudsql_connection",
    "state_us_pa_primary_cloudsql_connection",
    "state_us_tn_primary_cloudsql_connection",
    "us_id_operations_regional",
    "us_id_state_regional",
    "us_id_raw_data_up_to_date_views",
    "us_mi_operations_regional",
    "us_mi_state_regional",
    "us_mi_raw_data_up_to_date_views",
    "us_mo_operations_regional",
    "us_mo_state_regional",
    "us_mo_raw_data_up_to_date_views",
    "us_nd_operations_regional",
    "us_nd_state_regional",
    "us_nd_raw_data_up_to_date_views",
    "us_pa_operations_regional",
    "us_pa_state_regional",
    "us_pa_raw_data_up_to_date_views",
    "us_tn_operations_regional",
    "us_tn_state_regional",
    "us_tn_raw_data_up_to_date_views",
    "validation_metadata",
    "validation_views",
    "vitals_report_views",
}


def get_managed_view_and_materialized_table_addresses_by_dataset(
    managed_views_dag_walker: BigQueryViewDagWalker,
) -> Dict[str, Set[BigQueryAddress]]:
    """Creates a dictionary mapping every managed dataset in BigQuery to the set
    of managed views in the dataset. Returned dictionary's key is a dataset_id and
    each key's value is a set of all the BigQueryAddress's that are from the referenced dataset.
    """
    managed_views_for_dataset_map: Dict[str, Set[BigQueryAddress]] = {}
    for view in managed_views_dag_walker.views:
        managed_views_for_dataset_map.setdefault(view.address.dataset_id, set()).add(
            view.address
        )
        if view.materialized_address:
            managed_views_for_dataset_map.setdefault(
                view.materialized_address.dataset_id, set()
            ).add(view.materialized_address)
    return managed_views_for_dataset_map


def delete_unmanaged_views_and_tables_from_dataset(
    bq_client: BigQueryClient,
    dataset_id: str,
    managed_tables: Set[BigQueryAddress],
    dry_run: bool,
) -> Set[BigQueryAddress]:
    """This function takes in a set of managed views/tables and compares it to the list of
    tables BigQuery has. The function then deletes any views/tables that are in BigQuery but not
    in the set of managed views/tables. It then returns a set of the BigQueryAddress's
    from these unmanaged views/tables that are to be deleted."""
    unmanaged_views_and_tables: Set[BigQueryAddress] = set()
    dataset_ref = bq_client.dataset_ref_for_id(dataset_id)
    if not bq_client.dataset_exists(dataset_ref):
        raise ValueError("Dataset %s does not exist in BigQuery" % dataset_id)
    for table in list(bq_client.list_tables(dataset_id)):
        table_bq_address = BigQueryAddress.from_list_item(table)
        if table_bq_address not in managed_tables:
            unmanaged_views_and_tables.add(table_bq_address)
    for view in unmanaged_views_and_tables:
        if dry_run:
            logging.info(
                "[DRY RUN] Regular run would delete unmanaged table/view %s from dataset %s.",
                view.table_id,
                view.dataset_id,
            )

        else:
            logging.info(
                "Deleting unmanaged table/view %s from dataset %s.",
                view.table_id,
                view.dataset_id,
            )

            bq_client.delete_table(view.dataset_id, view.table_id)
    return unmanaged_views_and_tables


def cleanup_datasets_and_delete_unmanaged_views(
    bq_client: BigQueryClient,
    managed_views_map: Dict[str, Set[BigQueryAddress]],
    dry_run: bool = True,
) -> None:
    """This function filters through a list of managed dataset ids and a map of managed views to their corresponding
    datasets (which is obtained through get_managed_views_for_dataset_map()) and checks that the dataset is in the
    master list DATASETS_THAT_HAVE_EVER_BEEN_MANAGED. It then cleans up the datasets by deleting unmanaged datasets
    and deleting any unmanaged views within managed datasets."""
    datasets_that_have_ever_been_managed = get_datasets_that_have_ever_been_managed()
    managed_dataset_ids: List[str] = list(managed_views_map.keys())

    for dataset_id in managed_dataset_ids:
        if dataset_id not in datasets_that_have_ever_been_managed:
            raise ValueError(
                "Managed dataset %s not found in the list DATASETS_THAT_HAVE_EVER_BEEN_MANAGED."
                % dataset_id,
            )

    for dataset_id in datasets_that_have_ever_been_managed:
        if dataset_id not in managed_views_map:
            if bq_client.dataset_exists(bq_client.dataset_ref_for_id(dataset_id)):
                if dry_run:
                    logging.info(
                        "[DRY RUN] Regular run would delete unmanaged dataset %s.",
                        dataset_id,
                    )
                else:
                    logging.info(
                        "Deleting dataset %s, which is no longer managed.",
                        dataset_id,
                    )
                    bq_client.delete_dataset(
                        bq_client.dataset_ref_for_id(dataset_id), delete_contents=True
                    )
            else:
                logging.info(
                    "Dataset %s isn't being managed and no longer exists in BigQuery. It can be safely removed from "
                    "the list DATASETS_THAT_HAVE_EVER_BEEN_MANAGED.",
                    dataset_id,
                )

        else:
            delete_unmanaged_views_and_tables_from_dataset(
                bq_client, dataset_id, managed_views_map[dataset_id], dry_run
            )


def get_datasets_that_have_ever_been_managed() -> Set[str]:
    return DATASETS_THAT_HAVE_EVER_BEEN_MANAGED
